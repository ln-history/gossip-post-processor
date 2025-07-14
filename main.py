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
from lnhistoryclient.model.platform_internal.PlatformEvent import PlatformEvent
from lnhistoryclient.model.platform_internal.PlatformEventMetadata import PlatformEventMetadata
from lnhistoryclient.parser.common import strip_known_message_type
from lnhistoryclient.parser.parser import parse_platform_event

from handle_channel_announcement import add_channel_announcement_to_db
from handle_node_announcement import add_node_announcement_to_db
from handle_channel_update import add_channel_update_to_db
from handle_channel_dying import handle_channel_dying
from common import handle_platform_problem
from config import KAFKA_SERVER_IP_ADDRESS, KAFKA_SERVER_PORT, KAFKA_TOPIC_TO_SUBSCRIBE
from CustomLogger import CustomLogger
from PostgreSQLDataStore import PostgreSQLDataStore
from ValkeyClient import ValkeyCache



def create_kafka_producer(logger: CustomLogger) -> KafkaProducer:
    bootstrap_servers = f"{KAFKA_SERVER_IP_ADDRESS}:{KAFKA_SERVER_PORT}"

    try:
        producer = KafkaProducer(
            bootstrap_servers=[bootstrap_servers],
            client_id="test-gossip-post-processor",
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
                consumer.seek(tp, 0)

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

    counter = 0

    for event in consumer:
        platformEvent: PlatformEvent = parse_platform_event(event.value)
        metadata: PlatformEventMetadata = platformEvent.metadata
        expected_gossip_id: bytes = metadata.id
        msg_type: int = metadata.type

        if not expected_gossip_id:
            logger.warning("Skipping consumed gossip_event without `metadata.id`!")
            continue

        # Check that the gossip_id is correct
        actual_gossip_id = cache.hash_raw_bytes(bytes.fromhex(platformEvent.raw_gossip_hex)).hex()
        if expected_gossip_id != actual_gossip_id:
            logger.error(f"Incorret gossip_id found: PlatformEvent gossip_id {expected_gossip_id.hex()}, actual gossip_id of raw_gossip_bytes {actual_gossip_id.hex()}.")
            handle_platform_problem(platformEvent, "problem", logger, producer)


        # if msg_type == MSG_TYPE_CHANNEL_ANNOUNCEMENT:
        #     add_channel_announcement_to_db(platformEvent, datastore, logger, producer)
        # if msg_type == MSG_TYPE_NODE_ANNOUNCEMENT:
        #    add_node_announcement_to_db(platformEvent, datastore, logger, producer)
        if msg_type == MSG_TYPE_CHANNEL_UPDATE:
            add_channel_update_to_db(platformEvent, datastore, logger, producer)
        # if msg_type == MSG_TYPE_CHANNEL_DYING:
        #     handle_channel_dying(platformEvent, datastore, logger, producer)
        # else:
        #     logger.error(f"Unkown msg_type {msg_type} recieved for gossip_id {gossip_id.hex()} with raw_message {gossip_event.value.get("raw_hex")}!")
        #     handle_platform_problem(gossip_event, "problem", logger, producer)

        counter += 1

        if counter % 1_000 == 0:
            logger.info(f"Handeled {counter} messages.")


# Global variables
running: bool = True
consumer: Optional[KafkaConsumer] = None
producer: Optional[KafkaProducer] = None
datastore: Optional[PostgreSQLDataStore] = None
cache: Optional[ValkeyCache] = None
logger: Optional[CustomLogger] = None

if __name__ == "__main__":
    main()
