from datetime import datetime, timezone
from logging import Logger
from typing import Optional

from kafka import KafkaProducer
from lnhistoryclient.constants import MSG_TYPE_NODE_ANNOUNCEMENT
from lnhistoryclient.model.NodeAnnouncement import NodeAnnouncement
from lnhistoryclient.model.platform_internal.PlatformEvent import PlatformEvent
from lnhistoryclient.model.platform_internal.PlatformEventMetadata import PlatformEventMetadata
from lnhistoryclient.parser.common import strip_known_message_type
from lnhistoryclient.parser.parser import parse_node_announcement

from common import handle_platform_problem
from model.Node import Node
from PostgreSQLDataStore import PostgreSQLDataStore


def add_node_announcement_to_db(
    platformEvent: PlatformEvent, datastore: PostgreSQLDataStore, logger: Logger, producer: KafkaProducer
) -> None:
    """
    Workflow node_announcement insertion:
    1. Parse node_announcement to get values: timestamp, node_id
    2. Do a transaction:
        1. Insert into raw_gossip (gossip_message_type: 257)
        2. Insert into nodes_raw_gossip
        3. Is node_id known ?  (Check in PostgreSQL nodes table)
            -> If unknown: Add new row in nodes table with values (node_id, (timestamp, timestamp))
            -> If known:
                -> Get row from nodes table of that node_id
                -> Compare: from_timestamp with timestamp. If timestamp is lower than from_timestamp: Update from_timestamp
                -> Compare: last_seen: If timestamp is bigger than last_seen: Update last_seen
                (Note: Of the previous two comparisions ONLY ONE can be true)
    In case the transaction breaks the database -> everything gets aborted (Atomic: No modification on the database per message):
    The PlatformEvent containing the node_announcement gets sent to Kafka topic 'problem.node' -> Will be inspected at a later point
    """

    try:
        metadata: PlatformEventMetadata = platformEvent.metadata
        gossip_id: str = metadata.id

        raw_gossip_hex: str = platformEvent.raw_gossip_hex

        # 1 Parse NodeAnnouncement
        parsed: NodeAnnouncement = parse_node_announcement(strip_known_message_type(bytes.fromhex(raw_gossip_hex)))

        node_id: bytes = parsed.node_id
        node_id_str: str = parsed.node_id.hex()
        timestamp: int = parsed.timestamp

        logger.debug(
            f"Starting to add node_announcement with gossip_id {gossip_id} and node_id {node_id_str} and timestamp {timestamp} to db"
        )

        # 2 Start database transaction
        try:
            with datastore.transaction() as cur:
                timestamp_dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)

                # 1 Insert NodeAnnouncement to RawGossip
                logger.debug(f"Trying to insert raw_gossip with gossip_id {gossip_id}")
                datastore.add_raw_gossip(
                    cur, bytes.fromhex(gossip_id), MSG_TYPE_NODE_ANNOUNCEMENT, timestamp, bytes.fromhex(raw_gossip_hex)
                )

                # 2 Insert NodeAnnouncement to NodesRawGossip
                logger.debug(f"Trying to link gossip_id {gossip_id} to node_id {node_id_str} in NodesRawGossip")
                datastore.add_nodes_raw_gossip(cur, bytes.fromhex(gossip_id), node_id)

                # 3 Handle node
                node_data: Optional[Node] = datastore.get_node_by_node_id(cur, node_id)

                if not node_data:
                    # Unknown: Adding new node
                    logger.debug(
                        f"Trying to add new node with node_id {node_id_str} and from_timestamp {timestamp} and last_seen {timestamp}"
                    )
                    datastore.add_node(cur, node_id, timestamp, timestamp)
                else:
                    # Known: Checking timestamps and if necessary updating from_timestamp, last_seen
                    current_from_ts = node_data.from_timestamp
                    current_last_seen = node_data.last_seen

                    if current_from_ts > timestamp_dt:
                        logger.warning(
                            f"Earlier timestamp for node_id {node_id_str} found; Trying to update from_timestamp to {timestamp}"
                        )
                        datastore.update_node_from_timestamp(cur, node_id, timestamp)

                    elif current_last_seen < timestamp_dt:
                        logger.info(
                            f"Newer timestamp for node_id {node_id_str}; Trying to update last_seen to {timestamp}"
                        )
                        datastore.update_node_last_seen(cur, node_id, timestamp)
            logger.debug(
                f"Successfully handled database transaction for node_announcement of node with node_id {node_id_str} with gossip_id {gossip_id}."
            )
        except Exception as e:
            logger.error(f"Transaction failed for node_announcement with gossip_id={gossip_id}: {e}")
            handle_platform_problem(platformEvent, "problem.node_announcement.db_transaction", logger, producer)
            return

    except Exception as e:
        logger.critical(f"An Error occured when handing node_announcement platformEvent {platformEvent}: {e}")
        handle_platform_problem(platformEvent, "problem.node", logger, producer)
        return
