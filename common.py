import CustomLogger
from lnhistoryclient.model.platform_internal.PlatformEvent import PlatformEvent
from lnhistoryclient.model.platform_internal.PlatformEventMetadata import PlatformEventMetadata
from kafka import KafkaProducer

def handle_platform_problem(platformEvent: PlatformEvent, topic: str, logger: CustomLogger, producer: KafkaProducer) -> None:
    # global logger, producer
    
    metadata = platformEvent.metadata
    id = metadata.id
    logger.error(f"Handling error: Publishing PlatformEvent with gossip_id {id} to {topic}")
    producer.send(topic, value=platformEvent.to_dict())


def split_scid(scid, platformEvent, logger, producer) -> tuple[int, int, int]:
    # global logger
    try:
        block, tx, out = map(int, scid.split("x"))
        return block, tx, out
    except ValueError:
        logger.error(f"Could not split `scid` {scid} of platformEvent {platformEvent}. - Skipping further handling")
        handle_platform_problem(platformEvent, "channel.problem", logger, producer)
        return