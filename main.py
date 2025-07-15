import json
import logging
import os
import signal
import sys
import time
from datetime import datetime
from logging.handlers import RotatingFileHandler
from types import FrameType
from typing import Optional

from kafka import KafkaConsumer, KafkaProducer
from lnhistoryclient.constants import (
    MSG_TYPE_CHANNEL_ANNOUNCEMENT,
    MSG_TYPE_CHANNEL_DYING,
    MSG_TYPE_CHANNEL_UPDATE,
    MSG_TYPE_NODE_ANNOUNCEMENT,
)
from lnhistoryclient.model.platform_internal.PlatformEvent import PlatformEvent
from lnhistoryclient.model.platform_internal.PlatformEventMetadata import PlatformEventMetadata
from lnhistoryclient.parser.parser import parse_platform_event

from common import handle_platform_problem
from config import KAFKA_SERVER_IP_ADDRESS, KAFKA_SERVER_PORT, KAFKA_TOPIC_TO_SUBSCRIBE
from handle_channel_announcement import add_channel_announcement_to_db
from handle_channel_dying import handle_channel_dying
from handle_channel_update import add_channel_update_to_db
from handle_node_announcement import add_node_announcement_to_db
from PostgreSQLDataStore import PostgreSQLDataStore
from StatisticsTracker import StatisticsTracker
from ValkeyClient import ValkeyCache


def setup_logging(log_dir: str = "logs", log_file_base: str = "gossip_post_processor") -> logging.Logger:
    os.makedirs(log_dir, exist_ok=True)
    logger = logging.getLogger("gossip_post_processor")
    logger.setLevel(logging.INFO)

    # Add timestamp to filename
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_filename = f"{log_file_base}_{timestamp}.log"
    log_path = os.path.join(log_dir, log_filename)

    file_handler = RotatingFileHandler(log_path, maxBytes=5_000_000, backupCount=100)
    file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    return logger


def create_kafka_producer(logger: logging.Logger) -> KafkaProducer:
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


def create_kafka_consumer(topic: str, logger: logging.Logger) -> KafkaConsumer:
    """Create and return a configured Kafka consumer."""
    bootstrap_servers = f"{KAFKA_SERVER_IP_ADDRESS}:{KAFKA_SERVER_PORT}"

    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=[bootstrap_servers],
            client_id="gossip-post-processor-consumer",
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
            group_id="gossip-post-processor",
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        )

        logger.info("Initialized KafkaConsumer")
        return consumer
    except Exception as e:
        print(f"Failed to create Kafka producer: {e}")
        raise


def shutdown_signal_handler(logger: logging.Logger, signum: Optional[int] = None, frame: Optional[FrameType] = None) -> None:
    """Just signal intent to shutdown."""
    global running
    logger.info("Shutdown signal received...")
    running = False  # Let the loop exit gracefully

def cleanup(logger: logging.Logger) -> None:
    """Close all resources AFTER loop has stopped."""
    global consumer, datastore, producer, cleanup_done
    if cleanup_done:
        return
    logger.info("Shutting down gossip-post-processor...")

    if consumer:
        try:
            consumer.commit()
            logger.info("Kafka consumer offset committed.")
        except Exception as e:
            logger.warning(f"Failed to commit Kafka offset: {e}")
        try:
            consumer.close()
            logger.info("Kafka consumer closed.")
        except Exception as e:
            logger.warning(f"Failed to close Kafka consumer: {e}")

    if datastore:
        try:
            datastore.close()
            logger.info("PostgreSQL connection pool closed.")
        except Exception as e:
            logger.warning(f"Failed to close PostgreSQL connection: {e}")

    if producer:
        try:
            producer.flush(timeout=5)
            producer.close(timeout=5)
            logger.info("Kafka producer closed.")
        except Exception as e:
            logger.warning(f"Failed to close Kafka producer: {e}")

    cleanup_done = True
    logger.info("Shutdown complete.")


def main() -> None:
    global consumer, producer, datastore, cache

    logger: logging.Logger = setup_logging()
    producer = create_kafka_producer(logger)
    consumer = create_kafka_consumer(KAFKA_TOPIC_TO_SUBSCRIBE, logger)
    cache = ValkeyCache(logger)
    datastore = PostgreSQLDataStore(logger)
    stats_tracker = StatisticsTracker(logger)

    # Register signal handlers
    signal.signal(signal.SIGINT, lambda s, f: shutdown_signal_handler(logger, s, f))
    signal.signal(signal.SIGTERM, lambda s, f: shutdown_signal_handler(logger, s, f))

    logger.info("Starting gossip-post-processor... Press Ctrl+C to exit.")

    try:
        while running:
            try:
                event = next(consumer)

                event_start_time = time.time()

                platform_event: PlatformEvent = parse_platform_event(event.value)
                metadata: PlatformEventMetadata = platform_event.metadata
                expected_gossip_id: str = metadata.id
                msg_type: int = metadata.type

                if not expected_gossip_id:
                    logger.warning("Skipping event without `metadata.id`")
                    continue

                actual_gossip_id = cache.hash_raw_bytes(bytes.fromhex(platform_event.raw_gossip_hex)).hex()
                if expected_gossip_id != actual_gossip_id:
                    logger.error(f"Incorrect gossip_id: expected {expected_gossip_id}, got {actual_gossip_id}")
                    handle_platform_problem(platform_event, "problem.gossip_id_mismatch", logger, producer)
                    continue

                # Dispatch to handlers
                if msg_type == MSG_TYPE_CHANNEL_ANNOUNCEMENT:
                    add_channel_announcement_to_db(platform_event, datastore, logger, producer)
                elif msg_type == MSG_TYPE_NODE_ANNOUNCEMENT:
                    add_node_announcement_to_db(platform_event, datastore, logger, producer)
                elif msg_type == MSG_TYPE_CHANNEL_UPDATE:
                    add_channel_update_to_db(platform_event, datastore, logger, producer)
                elif msg_type == MSG_TYPE_CHANNEL_DYING:
                    handle_channel_dying(platform_event, datastore, logger, producer)
                else:
                    logger.error(f"Unknown msg_type {msg_type} for gossip_id {actual_gossip_id}")
                    handle_platform_problem(platform_event, "problem.unknown_type", logger, producer)

                # Record statistics
                event_duration = time.time() - event_start_time
                stats_tracker.record_message(msg_type, event_duration)

            except StopIteration:
                continue
            except Exception as e:
                logger.exception(f"Unhandled exception during message processing: {e}")
                handle_platform_problem(platform_event, "problem.unhandled", logger, producer)

            if not running:
                break  # in case shutdown happened during wait

    except Exception as e:
        logger.error(f"Error occured in main loop of gossip-post-processor: {e}")
    finally:
        cleanup(logger)
        


# Global variables
running: bool = True
cleanup_done: bool = False
consumer: Optional[KafkaConsumer] = None
producer: Optional[KafkaProducer] = None
datastore: Optional[PostgreSQLDataStore] = None
cache: Optional[ValkeyCache] = None

if __name__ == "__main__":
    main()
