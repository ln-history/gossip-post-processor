from logging import Logger
from typing import List, Optional

from kafka import KafkaProducer
from lnhistoryclient.model.core_lightning_internal.ChannelDying import ChannelDying
from lnhistoryclient.model.platform_internal.PlatformEvent import PlatformEvent
from lnhistoryclient.model.platform_internal.PlatformEventMetadata import PlatformEventMetadata
from lnhistoryclient.parser.common import strip_known_message_type
from lnhistoryclient.parser.core_lightning_internal.parser import parse_channel_dying

from BlockchainRequester import get_block_by_block_height
from common import handle_platform_problem, split_scid
from model.ChannelInDb import ChannelInDb
from model.ChannelUpdateInDb import ChannelUpdateInDb
from PostgreSQLDataStore import PostgreSQLDataStore


# Think about more sophisticated testing utilizing the scid
def handle_channel_dying(
    platformEvent: PlatformEvent, datastore: PostgreSQLDataStore, logger: Logger, producer: KafkaProducer
) -> None:
    """
    Workflow handling of channel_dying:
    1. Parse channel_dying to get value: scid
    2. Get block_timestamp by the first part (block_height) of the scid -> If this fails: abort
    3. Start a transaction:
        1. Get channel by scid
        2. If channel has no value for upper(validity):
            1. Set upper(validity) to block_timestamp
        3. If channel already has a value for upper(validity):
            1. Should not happend -> Publish PlatformError
        4. Get most recent channel_update (both directions) by scid
            1. If channel_update exist:
                1. Update to_timestamp to block_timestamp
            2. If channel_update is missing:
                1.  Should not happend -> Publish PlatformError
    In case the transaction breaks the database -> everything gets aborted (Atomic: No modification on the database per message):
    The PlatformEvent containing the channel_dying gets sent to Kafka topic 'problem.channel.*' -> Will be inspected at a later point
    """

    try:
        platform_event_metadata: PlatformEventMetadata = platformEvent.metadata
        gossip_id: str = platform_event_metadata.id
        raw_gossip_bytes: bytes = bytes.fromhex(platformEvent.raw_gossip_hex)

        # 1 Parse ChannelDying
        parsed: ChannelDying = parse_channel_dying(strip_known_message_type(raw_gossip_bytes))
        scid: str = parsed.scid_str

        # 2 Get block_timestamp
        block_timestamp: int
        try:
            height, tx_idx, output_idx = split_scid(scid, platformEvent, logger, producer)
            block = get_block_by_block_height(height, logger)
            if not block:
                raise ValueError("Block not found")

            block_timestamp = block.get("time")
            if not block_timestamp:
                raise ValueError("Block timestamp missing")

        except Exception as e:
            logger.error(f"Failed SCID analysis for gossip_id={gossip_id}: {e}")
            handle_platform_problem(platformEvent, "problem.channel.data", logger, producer)
            return

        # 3 transaction
        try:
            with datastore.transaction() as cur:
                channel: Optional[ChannelInDb] = datastore.get_channel_by_scid(cur, scid)
                if channel:
                    if channel.to_timestamp:
                        logger.error(
                            f"Found channel_dying with gossip_id {gossip_id} for channel with scid {scid} although channel was already closed. Existing timestamp {channel.to_timestamp}, tried to close with new timestamp {block_timestamp}."
                        )
                    else:
                        datastore.update_channel_close(cur, scid, block_timestamp)
                else:
                    logger.error(
                        f"Found channel_dying with gossip_id {gossip_id} for channel with scid {scid} without existing channel."
                    )
                    handle_platform_problem(platformEvent, "problem.channel.orphan", logger, producer)
                    return

                channel_updates: List[ChannelUpdateInDb] = datastore.get_channel_update_by_scid(cur, scid)

                updates_false = [cu for cu in channel_updates if cu.direction is False]
                updates_true = [cu for cu in channel_updates if cu.direction is True]

                from_update_timestamp_false_result = max(updates_false, key=lambda cu: cu.from_timestamp, default=None)
                from_update_timestamp_true_result = max(updates_true, key=lambda cu: cu.from_timestamp, default=None)

                if from_update_timestamp_false_result:
                    datastore.update_channel_update_to_timestamp_by_cu_data(
                        cur, scid, False, int(from_update_timestamp_false_result.from_timestamp), block_timestamp
                    )
                else:
                    logger.warning(f"No channel_update for channel with scid {scid} and direction {0} found")

                if from_update_timestamp_true_result:
                    datastore.update_channel_update_to_timestamp_by_cu_data(
                        cur,
                        scid,
                        True,
                        int(from_update_timestamp_true_result.from_timestamp.timestamp()),
                        block_timestamp,
                    )
                else:
                    logger.warning(f"No channel_update for channel with scid {scid} and direction {1} found")

            logger.debug(f"Successfully handled channel_dying for scid {scid} and gossip_id {gossip_id}")
        except Exception as e:
            logger.error(f"Transaction failed for channel_dying with gossip_id={gossip_id}: {e}")
            handle_platform_problem(platformEvent, "problem.channel_dying.db_transaction", logger, producer)
            return

    except Exception as e:
        logger.critical(f"Unhandled exception in handle_channel_dying: {e}")
        handle_platform_problem(platformEvent, "problem.channel_dying", logger, producer)
