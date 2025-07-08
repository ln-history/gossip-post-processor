from typing import List, Optional

from lnhistoryclient.model.types import PlatformEvent, PlatformEventMetadata
from lnhistoryclient.model.NodeAnnouncement import NodeAnnouncement
from lnhistoryclient.constants import MSG_TYPE_NODE_ANNOUNCEMENT
from lnhistoryclient.parser.parser import parse_node_announcement
from lnhistoryclient.parser.common import strip_known_message_type

from CustomLogger import CustomLogger
from common import handle_platform_problem
from model.GossipIdMsgTypeTimestamp import GossipIdMsgTypeTimestamp
from PostgreSQLDataStore import PostgreSQLDataStore

def add_node_announcement_to_db(platformEvent: PlatformEvent) -> None:
    global datastore, logger

    datastore: PostgreSQLDataStore
    logger: CustomLogger

    try:
        gossip_metadata: PlatformEventMetadata = platformEvent.get("metadata")
        gossip_id: bytes = bytes.fromhex(gossip_metadata.get("id"))
        gossip_id_str: str = gossip_id.hex()

        gossip_timestamp: int = gossip_metadata.get("timestamp")
        raw_gossip_bytes: bytes = bytes.fromhex(platformEvent.get("raw_hex"))  # "raw_gossip_bytes"

        # 1 Parse NodeAnnouncement
        parsed: NodeAnnouncement = parse_node_announcement(strip_known_message_type(raw_gossip_bytes))

        node_id: str = parsed.node_id
        node_id_str: str = parsed.node_id.hex()
        timestamp: int = parsed.timestamp

        timestamps = []

        # 2 - 1: Analyze the msg_type of previously collected messages with identical node_id
        gossip_id_msg_type_timestamp_objects: List[GossipIdMsgTypeTimestamp] = (
            datastore.get_gossip_id_msg_type_timestamp_by_node_id(node_id_str)
        )

        if len(gossip_id_msg_type_timestamp_objects) > 0:
            # 2 - 2: Check for an exact duplicate of this NodeAnnouncement
            if (
                GossipIdMsgTypeTimestamp(gossip_id=gossip_id, msg_type=MSG_TYPE_NODE_ANNOUNCEMENT, timestamp=timestamp)
                in gossip_id_msg_type_timestamp_objects
            ):
                logger.error(
                    f"Got duplicate PlatformEvent: NodeAnnouncement with node_id {node_id_str} and gossip_id {gossip_id_str} already exists in Nodes table! - Skipping further handling"
                )
                handle_platform_problem(platformEvent, "duplicate")
                return

            # 2 - 3: Get earliest and latest timestamps among all NodesRawGossips (could be NodeAnnouncements as well as ChannelAnnouncements)
            timestamps = [
                entry.timestamp
                for entry in gossip_id_msg_type_timestamp_objects
                if entry.msg_type == MSG_TYPE_NODE_ANNOUNCEMENT
            ]

        # 3: Add NodeAnnouncement to RawGossip
        logger.info(f"Adding NodeAnnouncement with gossip_id {gossip_id_str} to RawGossip table.")
        datastore.add_raw_gossip(gossip_id, MSG_TYPE_NODE_ANNOUNCEMENT, timestamp, raw_gossip_bytes)

        # 5 - 1: If the node_id already exists in the Nodes table, update correctly (consider timestamps etc.)
        if timestamps:

            earliest_timestamp = min(timestamps)
            latest_timestamp = max(timestamps)

            # 5 - 1 - 1: Update from_timestamp if this handled NodeAnnouncements timestamp is older than previously known
            if earliest_timestamp > timestamp:
                logger.warning(
                    f"Found an earlier NodeAnnouncement with gossip_id {gossip_id_str} for node_id {node_id_str}."
                )
                logger.warning(f"Updating node_id {node_id_str} from_timestamp to {timestamp}.")
                datastore.update_node_from_timestamp(node_id, timestamp)

            # 5 - 1 - 2: Update last_seen if this handled NodeAnnouncement has a newer timestamp than the latest
            if latest_timestamp < timestamp:
                logger.info(
                    f"Found a more recent NodeAnnouncement with gossip_id {gossip_id_str} for node_id {node_id_str}. Updating last_seen to {timestamp}."
                )
                datastore.update_node_last_seen(node_id, timestamp)

        # 5 - 2: Add new node in Nodes table, set `from_timestamp` and `last_seen` to `timestamp`
        else:
            logger.info(f"Adding new node with node_id {node_id_str} and timestamp {timestamp} to nodes table.")
            datastore.add_node(node_id, timestamp, timestamp)

        # 4: Add NodeAnnouncement to NodesRawGossip
        logger.info(
            f"Adding NodeAnnouncement: node_id {node_id_str} and gossip_id {gossip_id_str} to NodesRawGossip table."
        )
        datastore.add_nodes_raw_gossip(gossip_id, node_id)

    except Exception as e:
        logger.critical(f"An Error occured when handing platformEvent {platformEvent}: {e}")