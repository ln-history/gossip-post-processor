from typing import List, Optional

from lnhistoryclient.model.types import PlatformEvent, PlatformEventMetadata
from lnhistoryclient.model.ChannelUpdate import ChannelUpdate
from lnhistoryclient.constants import MSG_TYPE_CHANNEL_ANNOUNCEMENT, MSG_TYPE_CHANNEL_UPDATE
from lnhistoryclient.parser.parser import parse_channel_update
from lnhistoryclient.parser.common import strip_known_message_type

from CustomLogger import CustomLogger
from common import split_scid, handle_platform_problem
from model.GossipIdMsgTypeTimestamp import GossipIdMsgTypeTimestamp
from PostgreSQLDataStore import PostgreSQLDataStore

def add_channel_update_to_db(platformEvent: PlatformEvent) -> None:
    global datastore, logger
    datastore: PostgreSQLDataStore
    logger: CustomLogger

    try:
        gossip_metadata: PlatformEventMetadata = platformEvent.get("metadata")
        gossip_id: bytes = bytes.fromhex(gossip_metadata.get("id"))
        gossip_id_str: str = gossip_id.hex()

        gossip_timestamp: int = gossip_metadata.get("timestamp")
        raw_gossip_bytes: bytes = bytes.fromhex(platformEvent.get("raw_hex"))  # "raw_gossip_bytes"

        # 1 - 1: Parse ChannelUpdate
        parsed: ChannelUpdate = parse_channel_update(strip_known_message_type(raw_gossip_bytes))
        scid: str = parsed.scid_str
        timestamp: int = parsed.timestamp
        direction: int = parsed.direction

        # 1 - 2: Check if scid is valid
        height, tx_idx, output_idx = split_scid(scid, platformEvent)

        from_update_timestamp: int = timestamp
        to_update_timestamp: Optional[int] = None

        # 2 - 1: Analyze the msg_type of previously collected messages with identical scid
        history: List[GossipIdMsgTypeTimestamp] = datastore.get_gossip_id_msg_type_timestamp_by_scid(scid)

        if len(history) > 0:
            # 2 - 1 - 1: Detect exact duplicate
            current_entry = GossipIdMsgTypeTimestamp(
                gossip_id=gossip_id, msg_type=MSG_TYPE_CHANNEL_UPDATE, timestamp=timestamp
            )
            if current_entry in history:
                logger.error(
                    f"Got duplicate PlatformEvent: ChannelUpdate with scid {scid} and gossip_id {gossip_id_str} already exists! Skipping."
                )
                handle_platform_problem(platformEvent, "duplicate")
                return

            # 2 - 1 - 2: Multiple ChannelAnnouncements found for a single scid
            announcements = [entry for entry in history if entry.msg_type == MSG_TYPE_CHANNEL_ANNOUNCEMENT]
            if len(announcements) > 1:
                logger.error(f"Found multiple ChannelAnnouncements for identical scid {scid}. Skipping.")
                handle_platform_problem(platformEvent, "problem.channel")
                return

            # 2 - 1 - 3 : Check if an ChannelAnnouncement exists for the scid
            has_announcement = len(announcements) > 0
            if not has_announcement:
                logger.warning(
                    f"No ChannelAnnouncement found for scid {scid} while handling ChannelUpdate with gossip_id {gossip_id_str}."
                )

            # 2 - 1 - 4: Determine, if the channel_update is the earliest, in the middle or the latest update, suppose channel_updates with timestamps x
            # 2 - 1 - 4 - 1: Earliest => previously added timestamps: [2, 3], handling channel_update with timestamp x=1
            # 2 - 1 - 4 - 2: Middle => previously added timestamps: [1, 3], handling channel_update with timestamp x=2
            # 2 - 1 - 4 - 3: Latest => previously added timestamps: [1, 2], handling channel_update with timestamp x=3

            channel_updates = sorted(
                [entry for entry in history if entry.msg_type == MSG_TYPE_CHANNEL_UPDATE], key=lambda x: x.timestamp
            )

            same_ts = [cu for cu in channel_updates if cu.timestamp == timestamp and cu.gossip_id != gossip_id]
            if same_ts:
                logger.warning(
                    f"Another ChannelUpdate for scid {scid} exists with same timestamp {timestamp} and different {gossip_id_str}."
                )

            # Locate position of this update
            earlier = [cu for cu in channel_updates if cu.timestamp < timestamp]
            later = [cu for cu in channel_updates if cu.timestamp > timestamp]

            if not earlier and later:
                # 2 - 1 - 4 - 1: Earliest [now_handled, previously_earliest]
                to_update_timestamp = later[0].timestamp
                logger.warning(
                    f"Adding new earliest ChannelUpdate for scid {scid} from {timestamp} to {to_update_timestamp}."
                )

                if not has_announcement:
                    logger.warning(
                        f"Earlier ChannelUpdate found for scid {scid} with no announcement. Updating from_timestamp to {timestamp}."
                    )
                    datastore.update_channel_update_from_timestamp_by_gossip_id(gossip_id, timestamp)

            elif earlier and later:
                # 2 - 1 - 4 - 2: Middle [one_before, now_handled, one_after]
                gossip_id_before = earlier[-1].gossip_id
                gossip_id_after = later[0].gossip_id
                to_update_timestamp = later[0].timestamp

                logger.warning(
                    f"Adding 'in-between' ChannelUpdate for scid {scid} from {timestamp} to {to_update_timestamp}."
                )
                datastore.update_channel_update_to_timestamp_by_gossip_id(gossip_id_before, timestamp)
                datastore.update_channel_update_from_timestamp_by_gossip_id(gossip_id_after, timestamp)

            elif earlier and not later:
                # 2 - 1 - 4 - 3: Latest [previously_latest, now_handled]
                gossip_id_before = earlier[-1].gossip_id
                logger.info(f"New latest ChannelUpdate for scid {scid} from {timestamp} to None (open-ended).")
                datastore.update_channel_update_to_timestamp_by_gossip_id(gossip_id_before, timestamp)

            else:
                # 2 - 1 - 4 - 4: Should never happen logically, but good to log
                logger.warning(f"ChannelUpdate timestamp {timestamp} does not fit known timeline for scid {scid}.")

        # 2 - 2: This is the very first time the scid appears
        else:
            # 2 - 2 - 1: Add scid into Channels table as new row, set the `from_timestamp` to the timestamp of update message
            datastore.add_incomplete_channel(gossip_id, scid, timestamp)

        # 3: Add ChannelUpdate to RawGossip
        logger.info(f"Adding ChannelUpdate with gossip_id {gossip_id_str} to RawGossip table.")
        datastore.add_raw_gossip(gossip_id, MSG_TYPE_CHANNEL_UPDATE, timestamp, raw_gossip_bytes)

        # 4: Add ChannelUpdate to ChannelsRawGossip
        logger.info(f"Adding ChannelUpdate: gossip_id {gossip_id_str}, scid {scid} to ChannelsRawGossip.")
        datastore.add_channels_raw_gossip(gossip_id, scid)

        # 5: Add ChannelUpdate to ChannelUpdate table
        logger.info(
            f"Adding ChannelUpdate: gossip_id {gossip_id_str}, scid {scid}, direction {direction} and from_timestamp {timestamp} to ChannelUpdate"
        )
        datastore.add_channel_update(gossip_id, scid, direction, from_update_timestamp, to_update_timestamp)

    except Exception as e:
        logger.critical(f"An Error occured when handing platformEvent {platformEvent}: {e}.")