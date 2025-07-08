import CustomLogger
from lnhistoryclient.model.types import PlatformEvent, PlatformEventMetadata
from kafka import KafkaProducer

def handle_platform_problem(platformEvent: PlatformEvent, topic: str) -> None:
    global logger, producer
    logger: CustomLogger
    producer: KafkaProducer
    
    logger.error(f"Handling error: Publishing PlatformEvent {platformEvent} to {topic}")
    producer.send(platformEvent, topic=topic)


def split_scid(scid, platformEvent) -> tuple[int, int, int]:
    try:
        block, tx, out = map(int, scid.split("x"))
        return block, tx, out
    except ValueError:
        logger.error(f"Could not split `scid` {scid} of platformEvent {platformEvent}. - Skipping further handling")
        handle_platform_problem(platformEvent, "channel.problem")
        return