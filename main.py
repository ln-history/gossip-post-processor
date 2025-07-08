import json
import logging
import os
import signal
import sys
import time
from types import FrameType
from typing import Optional, cast

from kafka import KafkaConsumer, KafkaProducer
from lnhistoryclient.constants import (
    MSG_TYPE_CHANNEL_ANNOUNCEMENT,
    MSG_TYPE_CHANNEL_DYING,
    MSG_TYPE_CHANNEL_UPDATE,
    MSG_TYPE_NODE_ANNOUNCEMENT,
)
from lnhistoryclient.model.ChannelAnnouncement import ChannelAnnouncement
from lnhistoryclient.model.ChannelUpdate import ChannelUpdate
from lnhistoryclient.model.core_lightning_internal.ChannelDying import ChannelDying
from lnhistoryclient.model.NodeAnnouncement import NodeAnnouncement
from lnhistoryclient.parser.common import strip_known_message_type
from lnhistoryclient.parser.core_lightning_internal.parser import parse_channel_dying
from lnhistoryclient.parser.parser import (
    parse_channel_announcement,
    parse_channel_update,
    parse_node_announcement,
)

from handle_channel_announcement import add_channel_announcement_to_db
from handle_node_announcement import add_node_announcement_to_db
from handle_channel_update import add_channel_update_to_db
from handle_channel_dying import handle_channel_dying
from config import KAFKA_SERVER_IP_ADDRESS, KAFKA_SERVER_PORT, KAFKA_TOPIC_TO_SUBSCRIBE
from CustomLogger import CustomLogger
from model.ChannelIdTranslationResult import ChannelIdTranslationResult
from model.GossipIdMsgType import GossipIdMsgType
from model.GossipIdMsgTypeTimestamp import GossipIdMsgTypeTimestamp
from PostgreSQLDataStore import PostgreSQLDataStore
from ValkeyClient import ValkeyCache


def create_kafka_producer(logger: CustomLogger) -> KafkaProducer:
    bootstrap_servers = f"{KAFKA_SERVER_IP_ADDRESS}:{KAFKA_SERVER_PORT}"
    print(f"Connecting to KafkaProducer at: {bootstrap_servers}")

    try:
        producer = KafkaProducer(
            bootstrap_servers=[bootstrap_servers],
            client_id="gossip-post-processor",
            api_version="3",
            security_protocol="SASL_SSL",
            ssl_cafile="./certs/kafka.truststore.pem",
            ssl_certfile="./certs/kafka.keystore.pem",
            ssl_keyfile="./certs/kafka.keystore.pem",
            ssl_password=os.getenv("SSL_PASSWORD"),
            sasl_mechanism="SCRAM-SHA-512",
            sasl_plain_username=os.getenv("SASL_PLAIN_USERNAME"),
            sasl_plain_password=os.getenv("SASL_PLAIN_PASSWORD"),
            ssl_check_hostname=False,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )

        logger.info("Initialized KafkaProducer")
        return producer
    except Exception as e:
        print(f"Failed to create Kafka producer: {e}")
        raise


def create_kafka_consumer(topic: str, logger: CustomLogger) -> KafkaConsumer:
    """Create and return a configured Kafka consumer."""
    bootstrap_servers = f"{KAFKA_SERVER_IP_ADDRESS}:{KAFKA_SERVER_PORT}"
    logger.info(f"Connecting KafkaConsumer at: {bootstrap_servers}")

    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=[bootstrap_servers],
            client_id="test-gossip-post-processor",
            security_protocol="SASL_SSL",
            ssl_cafile="./certs/kafka.truststore.pem",
            ssl_certfile="./certs/kafka.keystore.pem",
            ssl_keyfile="./certs/kafka.keystore.pem",
            ssl_password=os.getenv("SSL_PASSWORD"),
            sasl_mechanism="SCRAM-SHA-512",
            sasl_plain_username=os.getenv("SASL_PLAIN_USERNAME"),
            sasl_plain_password=os.getenv("SASL_PLAIN_PASSWORD"),
            ssl_check_hostname=False,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id="gossip-post-processor-test",
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        )

        # Wait for partition assignment
        for _ in range(10):
            consumer.poll(1000)  # Triggers background assignment
            partitions = consumer.assignment()
            if partitions:
                break
            time.sleep(1)
        else:
            raise RuntimeError("Failed to get partition assignment")

        # Seek to the start offset on partition 0
        for tp in partitions:
            if tp.partition == 0:
                # consumer.seek(tp, 190542)
                # consumer.seek(tp, 290542)
                # consumer.seek(tp, 515542)
                consumer.seek(tp, 615542)

        logger.info("Initialized KafkaConsumer")
        return consumer
    except Exception as e:
        print(f"Failed to create Kafka producer: {e}")
        raise


def shutdown(logger: logging.Logger, signum: Optional[int] = None, frame: Optional[FrameType] = None) -> None:
    """Clean up resources"""
    global running, consumer, datastore
    logger.info("Shutting down gossip-post-processor...")
    running = False
    if consumer:
        consumer.close()
        logger.info("Kafka consumer closed.")
    if datastore:
        datastore.close()
        logger.info("PostgreSQL connection closed.")
    logger.info("Shutdown complete.")
    sys.exit(0)


def main() -> None:
    global consumer, producer, datastore, cache

    logger: CustomLogger = CustomLogger.create()
    producer = create_kafka_producer(logger)
    consumer = create_kafka_consumer(KAFKA_TOPIC_TO_SUBSCRIBE, logger)
    cache = ValkeyCache(logger)
    datastore = PostgreSQLDataStore(logger)

    # Register shutdown handlers
    signal.signal(signal.SIGINT, lambda s, f: shutdown(cast(logging.Logger, logger), s, f))
    signal.signal(signal.SIGTERM, lambda s, f: shutdown(cast(logging.Logger, logger), s, f))

    logger.info("Starting gossip-post-processor... Press Ctrl+C to exit.")

    for gossip_event in consumer:
        metadata = gossip_event.value.get("metadata")
        msg_type = metadata.get("type") if metadata else None
        gossip_id = metadata.get("id") if metadata else None

        if not gossip_id:
            logger.warning("Skipping consumed gossip_event without `metadata.id`!")
            continue

        if msg_type == MSG_TYPE_CHANNEL_ANNOUNCEMENT:
            add_channel_announcement_to_db(gossip_event.value)
        elif msg_type == MSG_TYPE_NODE_ANNOUNCEMENT:
            add_node_announcement_to_db(gossip_event.value)
        elif msg_type == MSG_TYPE_CHANNEL_UPDATE:
            add_channel_update_to_db(gossip_event.value)
        elif msg_type == MSG_TYPE_CHANNEL_DYING:
            handle_channel_dying(gossip_event.value)


# Global variables
running: bool = True
consumer: Optional[KafkaConsumer] = None
producer: Optional[KafkaProducer] = None
datastore: Optional[PostgreSQLDataStore] = None
cache: Optional[ValkeyCache] = None
logger: Optional[CustomLogger] = None

if __name__ == "__main__":
    main()
