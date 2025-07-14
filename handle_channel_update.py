from typing import List, Optional

from lnhistoryclient.model.types import PlatformEvent, PlatformEventMetadata
from lnhistoryclient.model.ChannelUpdate import ChannelUpdate
from lnhistoryclient.constants import MSG_TYPE_CHANNEL_ANNOUNCEMENT, MSG_TYPE_CHANNEL_UPDATE
from lnhistoryclient.parser.parser import parse_channel_update
from lnhistoryclient.parser.common import strip_known_message_type

from CustomLogger import CustomLogger
from common import split_scid, handle_platform_problem
from model.ChannelInDb import ChannelInDb
from model.ChannelUpdateInDb import ChannelUpdateInDb
from model.GossipIdMsgTypeTimestamp import GossipIdMsgTypeTimestamp
from BlockchainRequester import get_amount_sat_by_tx_idx_and_output_idx, get_block_by_block_height
from PostgreSQLDataStore import PostgreSQLDataStore
from kafka import KafkaProducer

def add_channel_update_to_db(platformEvent: PlatformEvent, datastore: PostgreSQLDataStore, logger: CustomLogger, producer: KafkaProducer) -> None:
    """
    Workflow channel_update insertion:
    1. Parse channel_update to get values: scid, direction, timestamp
    2. Check if scid with channel already exists and store the information in flag is_channel_existing
        1. If it exists: try to get most recent update and store it in most_recent_update
        2. If it does not exist:
            1. Get the timestamp by the first part of the scid (block_height) and get the amount_sat by using all three parts of the scid-> If this fails: abort
    3. Start a transaction:
        1. Add to raw_gossip table
        2. Add to channels_raw_gossip table
        3. We have four cases depending on the variables from earlier on
            1. Channel exists and previous channel_updates exist
                1. Find most recent channel_update and set upper(validity) to timestamp
                2. Add to channel_update table
            2. Channel exists and no previous channel_updates exist
                1. Add to channel_update table
            3. Channel does not exist and previous channel_updates exist
                1. Should not happend -> Publish PlatformError 
            4. Channel does not exist and no previous channel_updates exist
                1. Get timestamp and amount_sat of channel via scid and BlockchainRequester class
                2. Add "incomplete channel" (all information except source_node_id and target_node_id) to channels table
        4. update node last_seen
            
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
        direction: bool = bool(parsed.direction)


        # 2 Get already existing data
        is_channel_existing: bool = False
        most_recent_channel_update: ChannelUpdateInDb = None

        channel_data: Optional[ChannelInDb]
        channel_update_data: List[ChannelUpdateInDb] 

        try:
            with datastore.transaction() as cur:
                channel_data: Optional[ChannelInDb] = datastore.get_channel_by_scid(cur, scid)
                channel_update_data: List[ChannelUpdateInDb] = datastore.get_channel_updates_by_scid_and_direction(cur, scid, direction)
        
        except Exception as e:
            logger.error(f"Failed to get channel_data or channel_update_data for channel with scid {scid} and gossip_id={gossip_id}: {e}")
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
                amount_sat = get_amount_sat_by_tx_idx_and_output_idx(tx_id, output_idx, logger)

                if not (1 <= amount_sat <= 21_000_000 * 10**8):
                    raise ValueError(f"Amount {amount_sat} out of valid Bitcoin range")

                block_timestamp = block.get("time")
                if not block_timestamp:
                    raise ValueError("Block timestamp missing")

            except Exception as e:
                logger.error(f"Failed SCID analysis for gossip_id={gossip_id}: {e}")
                handle_platform_problem(platformEvent, "problem.channel.data", logger, producer)
                return
        
        if channel_update_data:
            most_recent_channel_update = max(channel_update_data, key=lambda cu: cu.from_timestamp, default=None)

        
        # 3 transaction 
        try:
            with datastore.transaction() as cur:
                datastore.add_raw_gossip(cur, bytes.fromhex(gossip_id), MSG_TYPE_CHANNEL_UPDATE, from_update_timestamp, bytes.fromhex(raw_gossip_hex))
                datastore.add_channels_raw_gossip(cur, bytes.fromhex(gossip_id), scid)
                
                # 1 & 2 channel exists
                if is_channel_existing:
                    datastore.add_channel_update(cur, scid, direction, from_update_timestamp)
                    if most_recent_channel_update:
                        # Check if most_recent_channel_update.from_timestamp is older than the new channel_update.from_update_timestamp
                        if most_recent_channel_update.from_timestamp < from_update_timestamp:
                            # Update the last recent channel_update with the new channel_update
                            datastore.update_channel_update_to_timestamp_by_cu_data(cur, scid, direction, from_update_timestamp, from_update_timestamp)
                        else:
                            logger.info(f"Found channel_update of scid {scid} that is not the most recent!")
                            # Check between which two channel_updates the handled channel_update belongs to via from_timestamp
                            for i, cu in enumerate(channel_update_data): # TODO: sort this array by from_timestamp 
                                if cu.from_timestamp > from_update_timestamp:
                                    datastore.update_channel_update_to_timestamp_by_cu_data(cur, scid, direction, from_update_timestamp, cu.from_timestamp) # Update to_timestamp of handled channel_update
                                    datastore.update_channel_update_from_timestamp_by_cu_data(cur, cu.scid, cu.direction, cu.from_timestamp, from_update_timestamp) # Update from_timestamp of next channel_update
                                    datastore.update_channel_update_to_timestamp_by_cu_data(cur, channel_update_data[i-1].scid, channel_update_data[i-1].direction, channel_update_data[i-1].from_timestamp, from_update_timestamp) # Update to_timestamp of the previous channel_update
                            
                
                # 3 & 4 channel does not exist
                else:
                    if most_recent_channel_update:
                        logger.error(f"UNUSUAL: channel_update {raw_gossip_hex} for not existing channel with scid {scid} found!")
                    datastore.add_incomplete_channel(cur, scid, block_timestamp, amount_sat)
                    datastore.add_channel_update(cur, scid, direction, from_update_timestamp)

            logger.debug(f"Sucessfully handled channel_update for channel with scid {scid} and gossip_id {gossip_id}")

        except Exception as e:
            logger.error(f"Failed to construct database transaction for channel_update of channel with scid {scid}")#
            handle_platform_problem(platformEvent, "problem.channel_update.db_transaction", logger, producer)
            return
        
    except Exception as e:
        logger.critical(f"Unhandled exception in add_channel_update_to_db: {e}")
        handle_platform_problem(platformEvent, "problem.channel_update", logger, producer)