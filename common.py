from logging import Logger

from kafka import KafkaProducer
from lnhistoryclient.model.platform_internal.PlatformEvent import PlatformEvent


def handle_platform_problem(platformEvent: PlatformEvent, topic: str, logger: Logger, producer: KafkaProducer) -> None:

    metadata = platformEvent.metadata
    id = metadata.id
    logger.error(f"Handling error: Publishing PlatformEvent with gossip_id {id} to {topic}")
    producer.send(topic, value=platformEvent.to_dict())


def split_scid(scid, platformEvent: PlatformEvent, logger: Logger, producer: KafkaProducer) -> tuple[int, int, int]:

    try:
        block, tx, out = map(int, scid.split("x"))
        return block, tx, out
    except ValueError:
        logger.error(f"Could not split `scid` {scid} of platformEvent {platformEvent}. - Skipping further handling")
        handle_platform_problem(platformEvent, "channel.problem", logger, producer)
        return
