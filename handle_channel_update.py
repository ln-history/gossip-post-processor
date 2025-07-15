from datetime import datetime, timezone
from logging import Logger
from typing import List, Optional

from kafka import KafkaProducer
from lnhistoryclient.constants import MSG_TYPE_CHANNEL_UPDATE
from lnhistoryclient.model.ChannelUpdate import ChannelUpdate
from lnhistoryclient.model.platform_internal.PlatformEvent import PlatformEvent
from lnhistoryclient.model.platform_internal.PlatformEventMetadata import PlatformEventMetadata
from lnhistoryclient.parser.common import strip_known_message_type
from lnhistoryclient.parser.parser import parse_channel_update

from BlockchainRequester import get_amount_sat_by_tx_idx_and_output_idx, get_block_by_block_height
from common import handle_platform_problem, split_scid
from model.ChannelInDb import ChannelInDb
from model.ChannelUpdateInDb import ChannelUpdateInDb
from model.Node import Node
from PostgreSQLDataStore import PostgreSQLDataStore


def add_channel_update_to_db(
    platformEvent: PlatformEvent, datastore: PostgreSQLDataStore, logger: Logger, producer: KafkaProducer
) -> None:
    """
    Workflow channel_update insertion:
    1. Parse channel_update to get values: scid, direction, from_update_timestamp
    2. Check if scid with channel already exists and store the information in flag is_channel_existing
        1. If it exists: try to get most recent update and store it in most_recent_update
        2. If it does not exist:
            1. Get the block_timestamp by the first part of the scid (block_height) and get the amount_sat by using all three parts of the scid-> If this fails: abort
    3. Start a transaction:
        1. Add to raw_gossip table
        2. Add to channels_raw_gossip table
        3. Channel exists:
            1. Update last_seen of node_ids
            2. Add to channel_update table
            3. Previous channel_updates exist:
                1. If from_update_timestamp is the most recent (fast path):
                    1. Update uppe(validity) of previously most recent channel_update to from_update_timestamp
                2. If the from_update_timestamp is in between (slow path):
                    1. Order all channel_updates by from_timestamp
                    2. Find the channel_update which is one before and one after the from_update_timestamp
                    3. Upate their from_timestamp and to_timestamp and set the to_timestamp of the handled channel_update
            4. If no previous channel_update exists:
                1. Continue, nothing to do

        4. Channel does not exist:
            1. Previous channel_updates exist:
                1. Should not happend -> Publish PlatformError
            2. No previous channel_updates exist
                1. Get timestamp and amount_sat of channel via scid and BlockchainRequester class
                2. Add "incomplete channel" (all information except source_node_id and target_node_id) to channels table
                3. Add to channel_update table

    In case the transaction breaks the database -> everything gets aborted (Atomic: No modification on the database per message):
    The PlatformEvent containing the channel_update gets sent to Kafka topic 'problem.channel.*' -> Will be inspected at a later point
    """

    try:
        platform_event_metadata: PlatformEventMetadata = platformEvent.metadata
        gossip_id: str = platform_event_metadata.id
        raw_gossip_hex: str = platformEvent.raw_gossip_hex

        # 1 Parse and validate ChannelUpdate
        try:
            parsed: ChannelUpdate = parse_channel_update(strip_known_message_type(bytes.fromhex(raw_gossip_hex)))

        except Exception as e:
            logger.error(f"Parsing failed for ChannelUpdate gossip_id={gossip_id}: {e}")
            handle_platform_problem(platformEvent, "problem.channel.parse", logger, producer)
            return

        scid: str = parsed.scid_str
        from_update_timestamp: int = parsed.timestamp
        from_update_timestamp_dt = datetime.fromtimestamp(from_update_timestamp, tz=timezone.utc)
        direction: bool = bool(parsed.direction)

        # 2 Get already existing data
        is_channel_existing: bool = False
        most_recent_channel_update: Optional[ChannelUpdateInDb] = None

        channel_data: Optional[ChannelInDb]
        channel_update_data: List[ChannelUpdateInDb]

        try:
            with datastore.transaction() as cur:
                channel_data = datastore.get_channel_by_scid(cur, scid)
                channel_update_data = datastore.get_channel_updates_by_scid_and_direction(cur, scid, direction)

        except Exception as e:
            logger.error(
                f"Failed to get channel_data or channel_update_data for channel with scid {scid} and gossip_id={gossip_id}: {e}"
            )
            handle_platform_problem(platformEvent, "problem.channel.lookup", logger, producer)

        if channel_data:
            is_channel_existing = True
        else:
            try:
                height, tx_idx, output_idx = split_scid(scid, platformEvent, logger, producer)
                block = get_block_by_block_height(height, logger)
                if not block:
                    raise ValueError("Block not found")

                tx_list = block.get("tx")
                if not tx_list or len(tx_list) <= tx_idx:
                    raise IndexError(f"Transaction index {tx_idx} out of bounds")

                tx_id = tx_list[tx_idx]
                amount_sat: Optional[int] = get_amount_sat_by_tx_idx_and_output_idx(tx_id, output_idx, logger)

                if amount_sat:
                    if not (1 <= amount_sat <= 21_000_000 * 10**8):
                        raise ValueError(f"Amount {amount_sat} out of valid Bitcoin range")

                block_timestamp: int = block.get("time")
                if not block_timestamp:
                    raise ValueError("Block timestamp missing")

            except Exception as e:
                logger.error(f"Failed SCID analysis for gossip_id={gossip_id}: {e}")
                handle_platform_problem(platformEvent, "problem.channel.data", logger, producer)
                return

        if channel_update_data:
            most_recent_channel_update = max(channel_update_data, key=lambda cu: cu.from_timestamp, default=None)

        if (
            ChannelUpdateInDb(
                scid=scid, direction=direction, from_timestamp=from_update_timestamp_dt, to_timestamp=None
            )
            in channel_update_data
        ):
            logger.warning(f"Found duplicate channel_update with scid {scid} and gossip_id {gossip_id}. Ignoring.")
            handle_platform_problem(platformEvent, "problem.duplicate", logger, producer)
            return

        # 3 transaction
        try:
            with datastore.transaction() as cur:
                # Always insert raw gossip
                datastore.add_raw_gossip(
                    cur,
                    bytes.fromhex(gossip_id),
                    MSG_TYPE_CHANNEL_UPDATE,
                    from_update_timestamp,
                    bytes.fromhex(raw_gossip_hex),
                )
                datastore.add_channels_raw_gossip(cur, bytes.fromhex(gossip_id), scid)

                # 1 & 2: channel exists
                if is_channel_existing:
                    # Update node `last_seen` value
                    if channel_data.source_node_id:
                        source_node: Optional[Node] = datastore.get_node_by_node_id(cur, channel_data.source_node_id)
                        if source_node:
                            if source_node.last_seen < from_update_timestamp_dt:
                                datastore.update_node_last_seen(cur, channel_data.source_node_id, from_update_timestamp)

                    if channel_data.target_node_id:
                        target_node: Optional[Node] = datastore.get_node_by_node_id(cur, channel_data.target_node_id)
                        if target_node.last_seen < from_update_timestamp_dt:
                            datastore.update_node_last_seen(cur, channel_data.target_node_id, from_update_timestamp)

                    # Insert the new channel_update
                    datastore.add_channel_update(cur, scid, direction, from_update_timestamp)

                    if not channel_update_data:
                        # No previous updates — nothing else to do
                        logger.debug(
                            f"Successfully added first channel_update for scid {scid} with from_timestamp {from_update_timestamp} and gossip_id {gossip_id}"
                        )
                        return

                    # === Fast path ===
                    if (
                        most_recent_channel_update
                        and from_update_timestamp_dt > most_recent_channel_update.from_timestamp
                    ):
                        datastore.update_channel_update_to_timestamp_by_cu_data(
                            cur,
                            most_recent_channel_update.scid,
                            most_recent_channel_update.direction,
                            int(most_recent_channel_update.from_timestamp.timestamp()),
                            from_update_timestamp,
                        )
                        logger.info(
                            f"Successfully added newer channel_update with gossip_id {gossip_id} for scid {scid} with from_timestamp {from_update_timestamp}"
                        )
                        return

                    # === Slow path ===
                    logger.info(f"Found channel_update of scid {scid} that is not the most recent!")
                    sorted_updates = sorted(channel_update_data, key=lambda cu: cu.from_timestamp)

                    for i, cu in enumerate(sorted_updates):
                        if from_update_timestamp_dt < cu.from_timestamp:
                            new_from_ts = from_update_timestamp
                            new_to_ts = int(cu.from_timestamp.timestamp())

                            # Insert/update the new one with gap-safe range ===
                            datastore.add_channel_update(
                                cur,
                                scid=scid,
                                direction=direction,
                                from_timestamp=new_from_ts,
                            )
                            datastore.update_channel_update_to_timestamp_by_cu_data(
                                cur,
                                scid=scid,
                                direction=direction,
                                from_update_timestamp=new_from_ts,
                                to_update_timestamp=new_to_ts,
                            )

                            # Update the next one (cu) to have its validity start after the new one ===
                            datastore.update_channel_update_from_timestamp_by_cu_data(
                                cur,
                                scid=cu.scid,
                                direction=cu.direction,
                                from_update_timestamp=int(cu.from_timestamp.timestamp()),
                                to_update_timestamp=new_from_ts,
                            )

                            # Optionally update the previous one (if any) to close its range at the new one ===
                            if i > 0:
                                prev_cu = sorted_updates[i - 1]
                                datastore.update_channel_update_to_timestamp_by_cu_data(
                                    cur,
                                    scid=prev_cu.scid,
                                    direction=prev_cu.direction,
                                    from_update_timestamp=int(prev_cu.from_timestamp.timestamp()),
                                    to_update_timestamp=new_from_ts,
                                )

                            break

                # 3 & 4: channel does not exist
                else:
                    if most_recent_channel_update:
                        logger.error(
                            f"UNUSUAL: channel_update {raw_gossip_hex} with gossip_id {gossip_id} for non-existent channel with scid {scid}!"
                        )
                        handle_platform_problem(platformEvent, "problem.channel.orphan", logger, producer)

                    datastore.add_incomplete_channel(cur, scid, block_timestamp, amount_sat)
                    datastore.add_channel_update(cur, scid, direction, from_update_timestamp)

            logger.debug(
                f"Successfully handled channel_update for scid {scid} with from_timestamp {from_update_timestamp} and gossip_id {gossip_id}"
            )

        except Exception as e:
            logger.error(f"Transaction failed for channel_update with gossip_id={gossip_id}: {e}")
            handle_platform_problem(platformEvent, "problem.channel_update.db_transaction", logger, producer)
            return

    except Exception as e:
        logger.critical(f"Unhandled exception in add_channel_update_to_db: {e}")
        handle_platform_problem(platformEvent, "problem.channel_update", logger, producer)
        return
