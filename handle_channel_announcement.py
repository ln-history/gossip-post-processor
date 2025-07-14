from typing import List, Optional
from kafka import KafkaProducer
from datetime import datetime, timezone

from lnhistoryclient.model.types import PlatformEvent, PlatformEventMetadata
from lnhistoryclient.model.ChannelAnnouncement import ChannelAnnouncement
from lnhistoryclient.constants import MSG_TYPE_CHANNEL_ANNOUNCEMENT, MSG_TYPE_CHANNEL_UPDATE
from lnhistoryclient.parser.parser import parse_channel_announcement
from lnhistoryclient.parser.common import strip_known_message_type

from CustomLogger import CustomLogger
from ValkeyClient import ValkeyCache
from common import split_scid, handle_platform_problem
from model.GossipIdMsgType import GossipIdMsgType
from BlockchainRequester import get_block_by_block_height, get_amount_sat_by_tx_idx_and_output_idx
from PostgreSQLDataStore import PostgreSQLDataStore


def add_channel_announcement_to_db(platformEvent: PlatformEvent, datastore: PostgreSQLDataStore, logger: CustomLogger, producer: KafkaProducer) -> None:
    """
    Workflow node_announcement insertion:
    1. Parse channel_announcement to get values: scid, node_id_1, node_id_2
    1b. Check if node_id_1 is lexicographically lower than node_id_2 -> If not: abort and to nothing
    2. Get the timestamp by the first part of the scid and get the amount_sat by using all three parts of the scid-> If this fails: abort and do nothing
    3. Start a transaction:
        1. Add raw_gossip table
        2. Add channels_raw_gossip table
        3. Check for both node_id_1 and node_id_2 if the node_id is present in nodes table
            If not: 
                1. Add (gossip_id, node_id) to nodes_raw_gossip
                2. Add (node_id, (timestamp, timestamp)) to nodes table 
        4. Add (scid, node_id_1, node_id_2, (timestamp, NULL), amount_sat) to channels table
    In case the transaction breaks the database -> everything gets aborted (Atomic: No modification on the database per message):
    The PlatformEvent containing the channel_announcement gets sent to Kafka topic 'problem.channel.*' -> Will be inspected at a later point
    """
    
    try:
        platform_event_metadata: PlatformEventMetadata = platformEvent.metadata
        gossip_id: str = platform_event_metadata.id
        raw_gossip_hex: str = platformEvent.raw_gossip_hex

        # Step 1: Parse and validate announcement
        try:
            parsed: ChannelAnnouncement = parse_channel_announcement(
                strip_known_message_type(bytes.fromhex(raw_gossip_hex))
            )
        except Exception as e:
            logger.error(f"Parsing failed for ChannelAnnouncement gossip_id={gossip_id}: {e}")
            handle_platform_problem(platformEvent, "problem.channel.parse", logger, producer)
            return

        scid = parsed.scid_str
        original_order = (parsed.node_id_1.hex(), parsed.node_id_2.hex())
        node_id_1, node_id_2 = sorted(original_order)

        # Step 1b: Enforce lexicographical order
        if original_order != (node_id_1, node_id_2):
            logger.error(f"Invalid node_id order for gossip_id={gossip_id}, original: {original_order}, sorted: {(node_id_1, node_id_2)}")
            handle_platform_problem(platformEvent, "problem.channel", logger, producer)
            return

        # Step 2: Split scid and get block data
        try:
            height, tx_idx, output_idx = split_scid(scid, platformEvent, logger, producer)
            block = get_block_by_block_height(height, logger)
            if not block:
                raise ValueError("Block not found")

            tx_list = block.get("tx")
            if not tx_list or len(tx_list) <= tx_idx:
                raise IndexError(f"Transaction index {tx_idx} out of bounds")

            tx_id = tx_list[tx_idx]
            amount_sat = get_amount_sat_by_tx_idx_and_output_idx(tx_id, output_idx, logger)

            if not (1 <= amount_sat <= 21_000_000 * 10**8):
                raise ValueError(f"Amount {amount_sat} out of valid Bitcoin range")

            timestamp = block.get("time")
            if not timestamp:
                raise ValueError("Block timestamp missing")

        except Exception as e:
            logger.error(f"Failed SCID analysis for gossip_id={gossip_id}: {e}")
            handle_platform_problem(platformEvent, "problem.channel.data", logger, producer)
            return

        # Step 3: Begin atomic DB transaction
        try:
            with datastore.transaction() as cur:
                timestamp_dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)

                # 3.1: Add to raw_gossip
                datastore.add_raw_gossip(
                    cur,
                    bytes.fromhex(gossip_id),
                    MSG_TYPE_CHANNEL_ANNOUNCEMENT,
                    timestamp,
                    bytes.fromhex(raw_gossip_hex),
                )


                # 3.2: Add to channels_raw_gossip
                datastore.add_channels_raw_gossip(cur, bytes.fromhex(gossip_id), scid)

                # 3.3: Check for both node_id_1 and node_id_2 if the node_id is present in nodes table
                for node_id in (node_id_1, node_id_2):
                    node_data = datastore.get_node_by_node_id(cur, bytes.fromhex(node_id))
                    # 3.3.1: Add node if it does not exist
                    if not node_data:
                        datastore.add_node(cur, bytes.fromhex(node_id), timestamp, timestamp)
                    # 3.3.2: Update node information in case its newer
                    else:
                         # Known: Checking timestamps and if necessary updating from_timestamp, last_seen
                        current_from_ts = node_data.from_timestamp
                        current_last_seen = node_data.last_seen

                        if current_from_ts > timestamp_dt:
                            logger.warning(f"Earlier timestamp for node_id {node_id} found; Trying to update from_timestamp to {timestamp}")
                            datastore.update_node_from_timestamp(cur, bytes.fromhex(node_id), timestamp)
                            datastore.add_nodes_raw_gossip(cur, bytes.fromhex(gossip_id), bytes.fromhex(node_id))

                        elif current_last_seen < timestamp_dt:
                            logger.info(f"Newer timestamp for node_id {node_id}; Trying to update last_seen to {timestamp}")
                            datastore.update_node_last_seen(cur, bytes.fromhex(node_id), timestamp)
                            datastore.add_nodes_raw_gossip(cur, bytes.fromhex(gossip_id), bytes.fromhex(node_id))

                # 3.4: Add to channels table
                datastore.add_channel(
                    cur,
                    scid,
                    bytes.fromhex(node_id_1),
                    bytes.fromhex(node_id_2),
                    timestamp,
                    amount_sat
                )

                logger.info(f"Successfully handled ChannelAnnouncement with scid={scid}, gossip_id={gossip_id}")

        except Exception as e:
            logger.error(f"Transaction failed for gossip_id={gossip_id}: {e}")
            handle_platform_problem(platformEvent, "problem.channel.transaction", logger, producer)
            return

    except Exception as e:
        logger.critical(f"Unhandled exception in add_channel_announcement_to_db: {e}")
        handle_platform_problem(platformEvent, "problem.channel", logger, producer)