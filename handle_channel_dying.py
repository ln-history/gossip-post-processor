from typing import List, Optional

from lnhistoryclient.model.types import PlatformEvent, PlatformEventMetadata
from lnhistoryclient.model.core_lightning_internal.ChannelDying import ChannelDying
from lnhistoryclient.constants import MSG_TYPE_CHANNEL_ANNOUNCEMENT, MSG_TYPE_CHANNEL_UPDATE
from lnhistoryclient.parser.core_lightning_internal.parser import parse_channel_dying
from lnhistoryclient.parser.common import strip_known_message_type

from CustomLogger import CustomLogger
from common import split_scid, handle_platform_problem
from BlockchainRequester import get_block_by_block_height
from PostgreSQLDataStore import PostgreSQLDataStore


# Think about more sophisticated testing utilizing the scid
def handle_channel_dying(platformEvent: PlatformEvent, datastore, logger, producer) -> None:
    # global datastore, logger
    # datastore: PostgreSQLDataStore
    # logger: CustomLogger
    
    try:
        gossip_metadata: PlatformEventMetadata = platformEvent.get("metadata")
        gossip_id: bytes = bytes.fromhex(gossip_metadata.get("id"))
        gossip_id_str: str = gossip_id.hex()

        gossip_timestamp: int = gossip_metadata.get("timestamp")
        raw_gossip_bytes: bytes = bytes.fromhex(platformEvent.get("raw_hex"))  # "raw_gossip_bytes"

        # 1 - 1: Parse ChannelDying
        parsed: ChannelDying = parse_channel_dying(strip_known_message_type(raw_gossip_bytes))

        scid = parsed.scid_str
        height = int(parsed.blockheight)

        # 1 - 2: Check if scid is valid
        height, tx_idx, output_idx = split_scid(scid, platformEvent)

        # 2 - 1: Check if block is retrievable
        block: dict = get_block_by_block_height(height, logger)
        if not block:
            logger.error(f"Could not retrieve block at height {height}. - Skipping further handling")
            handle_platform_problem(platformEvent, "problem.channel", logger, producer)
            return

        # 2 - 2: Check if timestamp in block exists
        block_timestamp: int = block.get("time", None)
        if not block_timestamp:
            logger.error(f"Retrieved block {block} without `time` at height {height}. - Skipping further handling")
            handle_platform_problem(platformEvent, "problem.channel", logger, producer)
            return

        # 3 - 1: Check if channel with scid exists in DB
        history = datastore.get_gossip_id_msg_type_timestamp_by_scid(scid)

        if len(history) > 0:

            # 3 - 2: Check if block_timestamp fits into ChannelUpdate history of channel
            channel_updates = sorted(
                [entry for entry in history if entry.msg_type == MSG_TYPE_CHANNEL_UPDATE], key=lambda x: x.timestamp
            )

            same_ts = [cu for cu in channel_updates if cu.timestamp == block_timestamp and cu.gossip_id != gossip_id]
            if same_ts:
                logger.warning(
                    f"ChannelDying with gossip_id {gossip_id_str} for scid {scid} has the same timestamp {block_timestamp} as a ChannelUpdate."
                )

            # 3 - 3: Locate block_timestamp in timestamps of ChannelUpdates
            later = [cu for cu in channel_updates if cu.timestamp > block_timestamp]

            if later:
                logger.warning(
                    f"Found ChannelUpdates that happened *after* the ChannelDying timestamp ({block_timestamp}). This is inconsistent."
                )
                handle_platform_problem(platformEvent, "problem.channel", logger, producer)
                return

            # 3 - 4: Update to_timestamp of that channel to block_timestamp
            logger.info(f"Channel with scid {scid} closed at timestamp {block_timestamp}.")
            datastore.update_channel_update_to_timestamp_by_gossip_id(scid, block_timestamp)

            # 3 - 5: Update to_update_timestamp of the latest channel_update to block_timestamp
            logger.info(
                f"Updating latest ChannelUpdate of channel with scid {scid} setting `to_timestamp` to {block_timestamp}"
            )
            datastore.update_latest_channel_update_by_scid(scid, block_timestamp)
        else:
            logger.error(
                f"Found ChannelDying event with scid {scid} but scid does not exist in DB. - Ignoring ChannelDying information"
            )

    except Exception as e:
        logger.critical(f"An Error occured when handing platformEvent {platformEvent}: {e}.")
