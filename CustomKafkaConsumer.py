import json
import os
import time
from typing import Any, Callable, Iterable

from kafka import KafkaConsumer
from lnhistoryclient.model.types import PlatformEvent

from config import KAFKA_SERVER_IP_ADDRESS, KAFKA_SERVER_PORT
from CustomLogger import CustomLogger


class CustomKafkaConsumer:
    START_OFFSET = 190542  # Offset to start consuming from (partition 0 only)

    def __init__(self, consumer: KafkaConsumer, topic: str):
        self.consumer = consumer
        self.topic = topic

    def __iter__(self) -> Any:
        return iter(self.consumer)

    def close(self) -> None:
        """Close the underlying Kafka consumer."""
        self.consumer.close()

    # TODO: Think about typing it as Iterable[PluginEvent]
    @staticmethod
    def create(topic: str, logger: CustomLogger) -> Iterable[PlatformEvent]:
        """Create and configure a Kafka consumer with SASL_SSL settings."""

        bootstrap_servers = f"{KAFKA_SERVER_IP_ADDRESS}:{KAFKA_SERVER_PORT}"

        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=[bootstrap_servers],
            client_id="gossip-post-processor",
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
            group_id="gossip-deduplicator-consumer",
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
                consumer.seek(tp, CustomKafkaConsumer.START_OFFSET)

        logger.info(
            f"CustomKafkaConsumer initialized. Consuming on topic {topic} with offset {CustomKafkaConsumer.START_OFFSET}"
        )

        return CustomKafkaConsumer(consumer, topic)

    def consume_messages(self: "CustomKafkaConsumer", handler: Callable[[dict], None], logger: CustomLogger) -> None:  # type: ignore[type-arg]
        """Consume Kafka messages and pass them to a handler function."""
        try:
            for message in self.consumer:
                if logger:
                    logger.trace(
                        f"Received message from topic={message.topic}, "
                        f"partition={message.partition}, offset={message.offset}"
                    )
                handler(message.value)
        except Exception as e:
            if logger:
                logger.error(f"Kafka consumption error: {e}", exc_info=True)
            else:
                raise
        finally:
            self.consumer.close()
            if logger:
                logger.info("Kafka consumer closed.")
