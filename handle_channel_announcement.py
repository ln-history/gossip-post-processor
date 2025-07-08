from typing import List, Optional

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


def add_channel_announcement_to_db(platformEvent: PlatformEvent) -> None:
    global datastore, cache, logger   
    datastore: PostgreSQLDataStore
    logger: CustomLogger
    cache: ValkeyCache
    
    try:
        gossip_metadata: PlatformEventMetadata = platformEvent.get("metadata")
        gossip_id: bytes = bytes.fromhex(gossip_metadata.get("id"))
        gossip_id_str: str = gossip_id.hex()

        raw_gossip_bytes: bytes = bytes.fromhex(platformEvent.get("raw_hex"))  # "raw_gossip_bytes"

        # 1 - 1: Check parsing
        parsed: ChannelAnnouncement = parse_channel_announcement(strip_known_message_type(raw_gossip_bytes))
        if not parsed:
            logger.error(f"Could not parse ChannelAnnouncement with gossip_id = {gossip_id_str}")
            handle_platform_problem(platformEvent, "problem.channel")
            return

        scid: str = parsed.scid_str
        original_order = (parsed.node_id_1.hex(), parsed.node_id_2.hex())
        node_id_1, node_id_2 = sorted(original_order)

        # 1 - 2: Check if the ordering of node_ids was correct (node_id_1 needs to be lexicographically lower than node_id_2)
        if original_order != (node_id_1, node_id_2):
            logger.error(
                f"ChannelAnnouncement with gossip_id = {gossip_id_str} had wrongly ordered node_ids: {original_order}, reordered to: {(node_id_1, node_id_2)}"
            )
            handle_platform_problem(platformEvent, "problem.channel")

        # 1 - 3: Check if scid is valid
        height, tx_idx, output_idx = split_scid(scid, platformEvent)

        # 2 - 1: Analyze the msg_type of previously collected messages with identical scid
        gossip_id_msg_type_objects: List[GossipIdMsgType] = datastore.get_gossip_id_msg_type_by_scid(scid)

        if len(gossip_id_msg_type_objects) > 0:
            # 2 - 2: Check for an exact duplicate of this ChannelAnnouncement
            if (
                GossipIdMsgType(gossip_id=gossip_id, msg_type=MSG_TYPE_CHANNEL_ANNOUNCEMENT)
                in gossip_id_msg_type_objects
            ):
                logger.error(
                    f"Got duplicate PlatformEvent: ChannelAnnouncement with scid {scid} and gossip_id {gossip_id_str} already exists in Channels table! - Skipping further handling"
                )
                handle_platform_problem(platformEvent, "duplicate")
                return

            # 2 - 3: Check if there is any other ChannelAnnouncement (msg_type == 256) for this scid
            has_other_announcement: bool = any(
                msg.msg_type == MSG_TYPE_CHANNEL_ANNOUNCEMENT and msg.gossip_id != gossip_id
                for msg in gossip_id_msg_type_objects
            )
            if has_other_announcement:
                logger.error(
                    f"Found another ChannelAnnouncement with identical scid {scid}. - Skipping further handling"
                )
                handle_platform_problem(platformEvent, "problem.channel")
                return

            # 2 - 4: Check if we only have ChannelUpdates (msg_type == 258)
            only_channel_updates: List[GossipIdMsgType] = all(
                msg.msg_type == MSG_TYPE_CHANNEL_UPDATE for msg in gossip_id_msg_type_objects
            )
            if only_channel_updates:
                count = len(gossip_id_msg_type_objects)
                logger.info(
                    f"Found ChannelAnnouncement with scid {scid} for previously {count} unconnected ChannelUpdate(s)."
                )
            else:
                logger.error(
                    f"Unusual msg_type found in ChannelsRawGossip while handling platformEvent {platformEvent}"
                )
                handle_platform_problem(platformEvent, "problem.channel")
                return

        # 3 - 1: Check if block is retrievable
        block: dict = get_block_by_block_height(height, logger)
        if not block:
            logger.error(f"Could not retrieve block at height {height}. - Skipping further handling")
            handle_platform_problem(platformEvent, "problem.channel")
            return

        # 3 - 2: Check if tx list exists in block
        block_tx: List[str] = block.get("tx", None)
        tx_id = block_tx[tx_idx]
        if not tx_id:
            logger.error(
                f"Retrieved block {block} at height {height} but `tx` does not have `txid` at index {tx_idx}. - Skipping further handling"
            )
            handle_platform_problem(platformEvent, "problem.channel")
            return

        # 3 - 3: Check if amount_sat is valid
        amount_sat: int = get_amount_sat_by_tx_idx_and_output_idx(tx_id, output_idx, logger)
        if amount_sat < 1 or amount_sat > 21_000_000 * 10**8:
            logger.error(
                f"Amount_sat {amount_sat} for scid {scid} is out of possible range for ChannelAnnouncement with gossip_id = {gossip_id_str}. - Skipping further handling"
            )
            handle_platform_problem(platformEvent, "problem.channel")
            return

        # 3 - 4: Check if timestamp in block exists
        block_timestamp: int = block.get("time", None)
        if not block_timestamp:
            logger.error(f"Retrieved block {block} without `time` at height {height}. - Skipping further handling")
            handle_platform_problem(platformEvent, "problem.channel")
            return

        # 5: Add to RawGossip table
        logger.info(f"Adding ChannelAnnouncement with gossip_id {gossip_id_str} to RawGossip table.")
        datastore.add_raw_gossip(gossip_id, MSG_TYPE_CHANNEL_ANNOUNCEMENT, block_timestamp, raw_gossip_bytes)

        # 6: Add to ChannelsRawGossip table
        logger.info(
            f"Adding ChannelAnnouncement: scid {scid} and gossip_id {gossip_id_str} to ChannelsRawGossip table."
        )
        datastore.add_channels_raw_gossip(gossip_id, scid)

        # 7: Add to Channels table
        logger.info(f"Adding channel with scid {scid} to Channels table")
        datastore.add_channel(gossip_id, scid, block_timestamp, None, amount_sat)



        # 10: Add to NodesRawGossip
        logger.info(f"Adding ChannelAnnouncement: node_id_1 {node_id_1} to NodesRawGossip table.")
        datastore.add_nodes_raw_gossip(gossip_id, node_id_1)
        logger.info(f"Adding ChannelAnnouncement: node_id_2 {node_id_2} to NodesRawGossip table.")
        datastore.add_nodes_raw_gossip(gossip_id, node_id_2)

    except Exception as e:
        logger.critical(f"An Error occured when handing platformEvent {platformEvent}: {e}")