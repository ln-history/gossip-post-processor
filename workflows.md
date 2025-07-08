## Error Types:

- InfrastructureError:
    - Could not retrieve Block with given height from CouchDB
    - Could not TxId with OutputIdx not 

- PlatformError:
    - gossip-post-processor got duplicates
    - Got different ChannelAnnouncements with identical scid

- GossipError:
    - Parsing raw_gossip_bytes let to invalid values such as scid with block_height = 1_000_000

- LightningError:
    - Got ChannelAnnouncement with wrongly ordered node_ids

```python
class PlatformEventMetadata(TypedDict):
    type: int
    id: bytes
    timestamp: int


class PlatformEvent(TypedDict):
    metadata: PlatformEventMetadata
    raw_gossip_bytes: bytes
```

## Helper functions
```python
def handle_platform_problem(platformEvent: PlatformEvent, topic: str):
    logger.error(f"Publishing PlatformEvent {platformEvent} to {topic}")
    producer.send(platformEvent, topic=topic)

def split_scid(scid, platformEvent): (int, int, int):
    try:
        block, tx, out = map(int, scid.split("x"))
        return block, tx, out
    except ValueError:
        logger.error(f"Could not split `scid` {scid} of platformEvent {platformEvent}. - Skipping further handling")
        handle_platform_problem(platformEvent, "channel.problem")
        return
```

## Helper classes
```python
@dataclass(frozen=True)
class GossipIdMsgType:
    gossip_id: bytes
    msg_type: int


@dataclass(frozen=True)
class GossipIdMsgTypeTimestamp:
    gossip_id: bytes
    msg_type: int
    timestamp: int


@dataclass(frozen=True)
class ChannelIdTranslationResult:
    scid: str
    direction: bool
    source_node_id: str
    target_node_id: str
```


# 256 - channel_announcement

```python
@dataclass
class ChannelAnnouncement:
    features: bytes
    chain_hash: bytes
    scid: int
    node_id_1: bytes
    node_id_2: bytes
    bitcoin_key_1: bytes
    bitcoin_key_2: bytes
    node_signature_1: bytes
    node_signature_2: bytes
    bitcoin_signature_1: bytes
    bitcoin_signature_2: bytes

    scid_str: str # height x tx_idx x output_idx

class ChannelAnnouncementDict(TypedDict):
    features: str
    chain_hash: str
    scid: str
    node_id_1: str
    node_id_2: str
    bitcoin_key_1: str
    bitcoin_key_2: str
    node_signature_1: str
    node_signature_2: str
    bitcoin_signature_1: str
    bitcoin_signature_2: str
```

```python
def add_channel_announcement_to_db(platformEvent: PlatformEvent):   
    try: 
        gossip_id: bytes = platformEvent.metadata.id
        gossip_id_str: str = gossip_id.hex()
        gossip_timestamp: int = platformEvent.metadata.timestamp
        raw_gossip_bytes: bytes = platformEvent.raw_gossip_bytes

        # 1 - 1: Check parsing
        parsed: ChannelAnnouncement = parse_channel_announcement(raw_gossip_bytes)
        if not parsed:
            logger.error(f"Could not parse ChannelAnnouncement with gossip_id = {gossip_id_str}")
            handle_platform_problem(platformEvent, "problem.channel")
            return

        scid: str = parsed.scid_str
        original_order = (parsed.node_id_1.hex(), parsed.node_id_2.hex())
        node_id_1, node_id_2 = sorted(original_order)

        # 1 - 2: Check if the ordering of node_ids was correct
        if original_order != (node_id_1, node_id_2):
            logger.warning(f"ChannelAnnouncement with gossip_id = {gossip_id_str} had wrongly ordered node_ids: {original_order}, reordered to: {(node_id_1, node_id_2)}")

        # 1 - 3: Check if scid is valid
        height, tx_idx, output_idx = split_scid(scid, platformEvent)

        # 2 - 1: Analyze the msg_type of previously collected messages with identical scid
        gossip_id_msg_type_objects: List[GossipIdMsgType] = datastore.get_gossip_id_msg_type_by_scid(scid)

        if len(gossip_id_msg_type_objects) > 0:
            # 2 - 2: Check for an exact duplicate of this ChannelAnnouncement
            if GossipIdMsgType(gossip_id=gossip_id, msg_type=MSG_TYPE_CHANNEL_ANNOUNCEMENT) in gossip_id_msg_type_objects:
                logger.error(f"Got duplicate PlatformEvent: ChannelAnnouncement with scid {scid} and gossip_id {gossip_id_str} already exists in Channels table! - Skipping further handling")
                handle_platform_problem(platformEvent, "duplicate")
                return

            # 2 - 3: Check if there is any other ChannelAnnouncement (msg_type == 256) for this scid
            has_other_announcement = any(msg.msg_type == MSG_TYPE_CHANNEL_ANNOUNCEMENT and msg.gossip_id != gossip_id for msg in gossip_id_msg_type_objects)
            if has_other_announcement:
                logger.error(f"Found another ChannelAnnouncement with identical scid {scid}. - Skipping further handling")
                handle_platform_problem(platformEvent, "problem.channel")
                return

            # 2 - 4: Check if we only have ChannelUpdates (msg_type == 258)
            only_channel_updates = all(msg.msg_type == MSG_TYPE_CHANNEL_UPDATE for msg in gossip_id_msg_type_objects)
            if only_channel_updates:
                count = len(gossip_id_msg_type_objects)
                logger.info(f"Found ChannelAnnouncement with scid {scid} for previously {count} unconnected ChannelUpdate(s).")
            else:
                logger.error(f"Unusual msg_type found in ChannelsRawGossip while handling platformEvent {platformEvent}")
                handle_platform_problem(platformEvent, "problem.channel")
                return
        

        # 3 - 1: Check if block is retrievable
        block: dict = get_bitcoin_block_by_height(height)
        if not block:
            logger.error(f"Could not retrieve block at height {height}. - Skipping further handling")
            handle_platform_problem(platformEvent, "problem.channel")
            return

        # 3 - 2: Check if tx list exists in block
        block_tx: [str] = block.get("tx", None)
        if not block_tx:
            logger.error(f"Retrieved block {block} without `tx` at height {height}. - Skipping further handling")
            handle_platform_problem(platformEvent, "problem.channel")
            return

        # 3 - 3: Check if amount_sat is valid
        amount_sat: int = get_amount_sat_by_tx_idx_and_output_idx(tx_idx, output_idx)
        if amount_sat not in [1, 21_000_000 * 10**8]:
            logger.error(f"Amount_sat {amount_sat} for scid {scid} is out of possible range for ChannelAnnouncement with gossip_id = {gossip_id_str}. - Skipping further handling")
            handle_platform_problem(platformEvent, "problem.channel")
            return

        # 3 - 4: Check if timestamp in block exists
        block_timestamp: int = block.get("time", None)
        if not block_timestamp:
            logger.error(f"Retrieved block {block} without `time` at height {height}. - Skipping further handling")
            handle_platform_problem(platformEvent, "problem.channel")
            return 

        # 4: Check if channel scid already exists in ChannelIdTranslation table
        channel_id_translation: List[ChannelIdTranslationResult] = get_channel_id_translation_by_scid(scid)

        expected_set = {
            ChannelIdTranslationResult(scid, 0, node_id_1, node_id_2),
            ChannelIdTranslationResult(scid, 1, node_id_2, node_id_1),
        }

        actual_set = set(channel_id_translation)

        if not actual_set.issubset(expected_set):
            logger.error(f"Found another translation for scid {scid} while processing ChannelAnnouncement with gossip_id {gossip_id_str}")
            handle_platform_problem(platformEvent, "problem.channel")
            return

        if ChannelIdTranslationResult(scid, 0, node_id_1, node_id_2) in actual_set:
            logger.warning(f"ChannelIdTranslation with (scid, direction, source_node_id, target_node_id) ({scid}, {0}, {node_id_1}, {node_id_2}) already present in ChannelIdTranslation table")
        else:
            logger.info(f"Adding channel_id_translation (scid, direction, source_node_id, target_node_id) ({scid}, {0}, {node_id_1}, {node_id_2})")
            datastore.add_channel_id_translation(scid, 0, node_id_1, node_id_2)

        if ChannelIdTranslationResult(scid, 1, node_id_2, node_id_1) in actual_set:
            logger.warning(f"ChannelIdTranslation with (scid, direction, source_node_id, target_node_id) ({scid}, {1}, {node_id_2}, {node_id_1}) already present in ChannelIdTranslation table")
        else:
            logger.info(f"Adding channel_id_translation (scid, direction, source_node_id, target_node_id) ({scid}, {1}, {node_id_2}, {node_id_1})")
            datastore.add_channel_id_translation(scid, 1, node_id_2, node_id_1)

        # 5 - 1: Check if gossip_timestamp exists: If it does not exist, it is likely an old gossip message which gets the block_timestamp as its gossip_timestamp
        if not gossip_timestamp:
            logger.warning(f"Processing a ChannelAnnouncement with gossip_id {gosgossip_id_strsip_id} which is in a PlatformEvent that has metadata.timestamp = None. This is likely an old message.")
            logger.warning(f"Using the block_timestamp {block_timestamp} of the height {height} of the ChannelAnnouncement as gossip_timestamp in RawGossip table")
            gossip_timestamp = block_timestamp
        else:
            # 5 - 1 - 1: Compare timestamp from block vs timestamp from platformEvent
            logger.debug(f"Comparison of timestamps: gossip_timestamp {gossip_timestamp} vs. block_timestamp {block_timestamp}.")

        # 5 - 2: Add to RawGossip table
        logger.info(f"Adding ChannelAnnouncement with gossip_id {gossip_id_str} to RawGossip table.")
        datastore.add_raw_gossip(gossip_id, MSG_TYPE_CHANNEL_ANNOUNCEMENT, gossip_timestamp, raw_gossip_bytes)

        # 6: Add to ChannelsRawGossip table
        logger.info(f"Adding ChannelAnnouncement: scid {scid} and gossip_id {gossip_id_str} to ChannelsRawGossip table.")
        datastore.add_channels_raw_gossip(gossip_id, scid)

        # 7: Add to Channels table
        logger.info(f"Adding channel with scid {scid} to Channels table")
        datastore.add_channel(gossip_id, scid, block_timestamp, NULL, amount_sat)        

        # 8 - 1: Check if node_id_1 exists in Nodes table
        if datastore.nodes_contain_node_id(node_id_1):
            logger.info(f"Updating last_seen for node with node_id {node_id_1} in Nodes table.")
            datastore.update_last_seen(node_id_1, gossip_timestamp)
        else:
            logger.warning(f"Found an unannounced node with node_id {node_id_2} while handing a ChannelAnnouncement.")
            logger.warning(f"Adding node with node_id {node_id} to Nodes table.")
            datastore.add_node(node_id_1, gossip_timestamp)

        # 8 - 2: Check if node_id_1 exists in Nodes table
        if datastore.nodes_contain_node_id(node_id_2):
            logger.info(f"Updating last_seen for node with node_id {node_id_2}")
            store.update_last_seen(node_id_2, gossip_timestamp)
        else:
            logger.warning(f"Found an unannounced node with node_id {node_id_2} while handing a ChannelAnnouncement.")
            logger.warning(f"Adding node with node_id {node_id} to Nodes table.")
            datastore.add_node(node_id_2, gossip_timestamp)
        
        # 9: Add to NodesRawGossip
        logger.info(f"Adding ChannelAnnouncements: node_id_1 {node_id_1} to NodesRawGossip table.")
        datastore.add_nodes_raw_gossip(gossip_id, node_id_1)
        logger.info(f"Adding ChannelAnnouncements: node_id_2 {node_id_2} to NodesRawGossip table.")
        datastore.add_nodes_raw_gossip(gossip_id, node_id_2)

    except Exception as e:
        logger.critical(f"An Error occured when handing platformEvent {platformEvent}: {e}")
```


# 257 - node_announcement

```python
@dataclass
class NodeAnnouncement:
    signature: bytes
    features: bytes
    timestamp: int
    node_id: bytes
    rgb_color: bytes
    alias: bytes
    addresses: bytes 

class AddressTypeDict(TypedDict):
    id: int
    name: str


class AddressDict(TypedDict):
    typ: AddressTypeDict
    addr: str
    port: int


class NodeAnnouncementDict(TypedDict):
    signature: str
    features: str
    timestamp: int
    node_id: str
    rgb_color: str
    alias: str
    addresses: List[AddressDict]
```

```python
def add_node_announcement_to_db(platformEvent: PlatformEvent):
    try:
        gossip_id: bytes = platformEvent.metadata.id
        gossip_id_str: str = gossip_id.hex()
        gossip_timestamp: int = platformEvent.metadata.timestamp
        raw_gossip_bytes: bytes = platformEvent.raw_gossip_bytes 

        # 1 Parse NodeAnnouncement
        parsed: NodeAnnouncement = parse_node_announcement(platformEvent)

        node_id: str = parsed.node_id.hex()
        timestamp: int = parsed.timestamp

        timestamps = []
        
        # 2 - 1: Analyze the msg_type of previously collected messages with identical node_id
        gossip_id_msg_type_timestamp_objects: List[GossipIdMsgTypeTimestamp] = datastore.get_gossip_id_msg_type_timestamp_by_node_id(node_id)

        if len(gossip_id_msg_type_timestamp_objects) > 0:
            # 2 - 2: Check for an exact duplicate of this NodeAnnouncement
            if GossipIdMsgTypeTimestamp(gossip_id=gossip_id, msg_type=MSG_TYPE_NODE_ANNOUNCEMENT, timestamp=timestamp) in gossip_id_msg_type_timestamp_objects:
                logger.error(f"Got duplicate PlatformEvent: NodeAnnouncement with node_id {node_id} and gossip_id {gossip_id_str} already exists in Nodes table! - Skipping further handling")
                handle_platform_problem(platformEvent, "duplicate")
                return

            # 2 - 3: Get earliest and latest timestamps among all NodesRawGossips (could be NodeAnnouncements as well as ChannelAnnouncements)
            timestamps = [entry.timestamp for entry in gossip_id_msg_type_timestamp_objects if entry.msg_type == MSG_TYPE_NODE_ANNOUNCEMENT]

        # 3: Add NodeAnnouncement to RawGossip
        logger.info(f"Adding NodeAnnouncement with gossip_id {gossip_id_str} to RawGossip table.")
        datastore.add_raw_gossip(gossip_id, MSG_TYPE_NODE_ANNOUNCEMENT, timestamp, raw_gossip_bytes)

        # 4: Add NodeAnnouncement to NodesRawGossip
        logger.info(f"Adding NodeAnnouncement: node_id {node_id} and gossip_id {gossip_id_str} to NodesRawGossip table.")
        datastore.add_nodes_raw_gossip(gossip_id, node_id)

        # 5 - 1: If the node_id already exists in the Nodes table, update correctly (consider timestamps etc.)
        if timestamps:
            
            earliest_timestamp = min(timestamps)
            latest_timestamp = max(timestamps)

            # 5 - 1 - 1: Update from_timestamp if this handled NodeAnnouncements timestamp is older than previously known
            if earliest_timestamp > timestamp:
                logger.warning(f"Found an earlier NodeAnnouncement with gossip_id {gossip_id_str} for node_id {node_id}.")
                logger.warning(f"Updating node_id {node_id} from_timestamp to {timestamp}.")
                datastore.update_node_from_timestamp(node_id, timestamp)

            # 5 - 1 - 2: Update last_seen if this handled NodeAnnouncement has a newer timestamp than the latest
            if latest_timestamp < timestamp:
                logger.info(f"Found a more recent NodeAnnouncement with gossip_id {gossip_id_str} for node_id {node_id}. Updating last_seen to {timestamp}.")
                datastore.update_node_last_seen(node_id, timestamp)
        
        # 5 - 2: Add new node in Nodes table, set `from_timestamp` and `last_seen` to `timestamp`
        else:
            logger.info(f"Adding new node with node_id {node_id} and timestamp {timestamp} to nodes table.")
            datastore.add_node(node_id, timestamp, timestamp)

    except Exception as e:
        logger.critical(f"An Error occured when handing platformEvent {platformEvent}: {e}")
```


# 258 - channel_update

```python
@dataclass
class ChannelUpdate:
    signature: bytes
    chain_hash: bytes
    scid: int
    timestamp: int
    message_flags: bytes
    channel_flags: bytes
    cltv_expiry_delta: int
    htlc_minimum_msat: int
    fee_base_msat: int
    fee_proportional_millionths: int
    htlc_maximum_msat: int | None = None

    scid_str: str
    direction: int

class ChannelUpdateDict(TypedDict):
    signature: str
    chain_hash: str
    scid: str
    timestamp: int
    message_flags: str
    channel_flags: str
    cltv_expiry_delta: int
    htlc_minimum_msat: int
    fee_base_msat: int
    fee_proportional_millionths: int
    htlc_maximum_msat: Optional[int]
```


Duplikat erkennen und melden

liste nur mit channel updates und timestamps [2,3] unser update hat timestamp 1
liste nur mit channel updates und timestamps [1,3] unser update hat timestamp 2
liste nur mit channel updates und timestamps [1,2] unser update hat timestamp 3
-> Warning, dass noch kein channel_announcement zu diesem Update existiert

liste mit channel updates und channel announcements -> Ohne warning was darüber steht (happy)

liste mit mehreren channel announcements -> Error -> problem


```python
@dataclass(frozen=True)
class GossipIdMsgTypeTimestamp:
    gossip_id: bytes
    msg_type: int
    timestamp: int

def add_channel_update_to_db(platformEvent: PlatformEvent):
    try:
        gossip_id: bytes = platformEvent.metadata.id
        gossip_id_str: str = gossip_id.hex()
        gossip_timestamp: int = platformEvent.metadata.timestamp
        gossip_raw_bytes: bytes = platformEvent.raw_gossip_bytes

        # 1 - 1: Parse ChannelUpdate
        parsed: ChannelUpdate = parse_channel_update(gossip_raw_bytes)
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
            current_entry = GossipIdMsgTypeTimestamp(gossip_id=gossip_id, msg_type=MSG_TYPE_CHANNEL_UPDATE, timestamp=timestamp)
            if current_entry in history:
                logger.error(f"Got duplicate PlatformEvent: ChannelUpdate with scid {scid} and gossip_id {gossip_id_str} already exists! Skipping.")
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
                logger.warning(f"No ChannelAnnouncement found for scid {scid} while handling ChannelUpdate with gossip_id {gossip_id_str}.")


            # 2 - 1 - 4: Determine, if the channel_update is the earliest, in the middle or the latest update, suppose channel_updates with timestamps x
            # 2 - 1 - 4 - 1: Earliest => previously added timestamps: [2, 3], handling channel_update with timestamp x=1
            # 2 - 1 - 4 - 2: Middle => previously added timestamps: [1, 3], handling channel_update with timestamp x=2
            # 2 - 1 - 4 - 3: Latest => previously added timestamps: [1, 2], handling channel_update with timestamp x=3

            channel_updates = sorted(
                [entry for entry in history if entry.msg_type == MSG_TYPE_CHANNEL_UPDATE],
                key=lambda x: x.timestamp
            )

            same_ts = [cu for cu in channel_updates if cu.timestamp == timestamp and cu.gossip_id != gossip_id]
            if same_ts:
                logger.warning(f"Another ChannelUpdate for scid {scid} exists with same timestamp {timestamp} and different {gossip_id_str}.")

            # Locate position of this update
            earlier = [cu for cu in channel_updates if cu.timestamp < timestamp]
            later = [cu for cu in channel_updates if cu.timestamp > timestamp]

            if not earlier and later:
                # 2 - 1 - 4 - 1: Earliest [now_handled, previously_earliest]
                to_update_timestamp = later[0].timestamp
                logger.warning(f"Adding new earliest ChannelUpdate for scid {scid} from {timestamp} to {to_update_timestamp}.")

                if not has_announcement:
                    logger.warning(f"Earlier ChannelUpdate found for scid {scid} with no announcement. Updating from_timestamp to {timestamp}.")
                    datastore.update_channels_from_timestamp_by_gossip_id(gossip_id, timestamp)

            elif earlier and later:
                # 2 - 1 - 4 - 2: Middle [one_before, now_handled, one_after]
                gossip_id_before = earlier[-1].gossip_id
                gossip_id_after = later[0].gossip_id
                to_update_timestamp = later[0].timestamp

                logger.warning(f"Adding 'in-between' ChannelUpdate for scid {scid} from {timestamp} to {to_update_timestamp}.")
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
        logger.info(f"Adding ChannelUpdate: gossip_id {gossip_id_str}, scid {scid}, direction {direction} and from_timestamp {timestamp} to ChannelUpdate")
        datastore.add_channel_update(gossip_id, scid, direction, from_update_timestamp, to_update_timestamp)

    except Exception as e:
        logger.critical(f"An Error occured when handing platformEvent {platformEvent}: {e}.")
```



# 4106 - channel_dying
```python
# Think about more sophisticated testing utilizing the scid
def handle_channel_dying(platformEvent: PlatformEvent):
    try:
        gossip_id: bytes = platformEvent.metadata.id
        gossip_id_str: str = gossip_id.hex()
        gossip_timestamp: int = platformEvent.metadata.timestamp
        gossip_raw_bytes: bytes = platformEvent.raw_gossip_bytes 

        # 1 - 1: Parse ChannelDying
        parsed: ChannelDying = parse_channel_dying(gossip_raw_bytes)

        scid = parsed.scid_str
        height = int(parsed.blockheight)

        # 1 - 2: Check if scid is valid
        height, tx_idx, output_idx = split_scid(scid, platformEvent)


        # 2 - 1: Check if block is retrievable
        block: dict = get_bitcoin_block_by_height(height)
        if not block:
            logger.error(f"Could not retrieve block at height {height}. - Skipping further handling")
            handle_platform_problem(platformEvent, "problem.channel")
            return

        # 2 - 2: Check if timestamp in block exists
        block_timestamp: int = block.get("time", None)
        if not block_timestamp:
            logger.error(f"Retrieved block {block} without `time` at height {height}. - Skipping further handling")
            handle_platform_problem(platformEvent, "problem.channel")
            return 

        # 3 - 1: Check if channel with scid exists in DB
        history = datastore.get_gossip_id_msg_type_timestamp_by_scid(scid)

        if len(history) > 0:

            # 3 - 2: Check if block_timestamp fits into ChannelUpdate history of channel
            channel_updates = sorted(
                [entry for entry in history if entry.msg_type == MSG_TYPE_CHANNEL_UPDATE],
                key=lambda x: x.timestamp
            )

            same_ts = [cu for cu in channel_updates if cu.timestamp == block_timestamp and cu.gossip_id != gossip_id]
            if same_ts:
                logger.warning(f"ChannelDying for scid {scid} has the same timestamp {timestamp} as a ChannelUpdate.")

            # 3 - 3: Locate block_timestamp in timestamps of ChannelUpdates
            later = [cu for cu in channel_updates if cu.timestamp > block_timestamp] 

            if later:
                logger.warning(f"Found ChannelUpdates that happened *after* the ChannelDying timestamp ({block_timestamp}). This is inconsistent.")
                handle_platform_problem(platformEvent, "problem.channel")
                return

            # 3 - 4: Update to_timestamp of that channel to block_timestamp
            logger.info(f"Channel with scid {scid} closed at timestamp {block_timestamp}.")
            datastore.update_channel_to_timestamp_by_scid(scid, block_timestamp)

            # 3 - 5: Update to_update_timestamp of the latest channel_update to block_timestamp 
            logger.info(f"Updating latest ChannelUpdate of channel with scid {scid} setting `to_timestamp` to {block_timestamp}")
            datastore.update_latest_channel_update_by_scid(scid, block_timestamp)
        else:
            logger.error(f"Found ChannelDying event with scid {scid} but scid does not exist in DB. - Ignoring ChannelDying information")

    except Exception as e:
        logger.critical(f"An Error occured when handing platformEvent {platformEvent}: {e}.")
```



## datastore methods

256: 
- get_gossip_id_msg_type_by_scid(scid)                                                          # JOIN: Channels, ChannelUpdates, ChannelsRawGossip
- get_channel_id_translation_by_scid(scid)                                                      # ChannelTranslation
- get_node_by_node_id(node_id_1)                                                                # Nodes

- add_channel_id_translation(scid, 0, node_id_1, node_id_2)                                     # ChannelTranslation
- add_raw_gossip(gossip_id, MSG_TYPE_CHANNEL_ANNOUNCEMENT, gossip_timestamp, raw_gossip_bytes)  # RawGossip
- add_channels_raw_gossip(gossip_id, scid)                                                      # ChannelsRawGossip
- add_channel(gossip_id, scid, block_timestamp, None, amount_sat)                               # Channels
- add_nodes_raw_gossip(gossip_id, node_id_1)                                                    # NodesRawGossip

- update_node_last_seen(node_id_1, gossip_timestamp)                                            # Nodes


257:
- get_gossip_id_msg_type_timestamp_by_node_id(node_id)                                          # JOIN: Nodes, NodesRawGossip

- add_raw_gossip(gossip_id, MSG_TYPE_NODE_ANNOUNCEMENT, timestamp, raw_gossip_bytes)            # RawGossip
- add_nodes_raw_gossip(gossip_id, node_id)                                                      # NodesRawGossip
- add_node(node_id, timestamp, timestamp)                                                       # Nodes

- update_node_from_timestamp(node_id, timestamp)                                                # Nodes
- update_node_last_seen(node_id, timestamp)                                                     # Nodes


258:
- get_gossip_id_msg_type_timestamp_by_scid(scid)                                                # JOIN: Channels, ChannelUpdates, ChannelsRawGossip

- add_incomplete_channel(gossip_id, scid, timestamp)                                            # Channels
- add_raw_gossip(gossip_id, MSG_TYPE_CHANNEL_UPDATE, timestamp, raw_gossip_bytes)               # RawGossip
- add_channels_raw_gossip(gossip_id, scid)                                                      # ChannelsRawGossip

- update_channel_from_timestamp_by_gossip_id(gossip_id, timestamp)                              # ChannelUpdate
- update_channel_update_to_timestamp_by_gossip_id(gossip_id_before, timestamp)                  # ChannelUpdate
- update_channel_update_from_timestamp_by_gossip_id(gossip_id_after, timestamp)                 # ChannelUpdate

4106:
- get_gossip_id_msg_type_timestamp_by_scid(scid)                                                # JOIN: Channels, ChannelUpdates, ChannelsRawGossip

- update_channel_to_timestamp_by_scid(scid, block_timestamp)                                    # ChannelUpdates
- update_latest_channel_update_by_scid(scid, block_timestamp)                                   # ChannelUpdates




### Per table

RawGossip
- add_raw_gossip(gossip_id, MSG_TYPE_CHANNEL_UPDATE, timestamp, raw_gossip_bytes) - DONE

ChannelsRawGossip (with JOINS)
- get_gossip_id_msg_type_by_scid(scid)
- get_gossip_id_msg_type_timestamp_by_scid(scid)
- add_channels_raw_gossip(gossip_id, scid)

Channels
- add_channel(gossip_id, scid, block_timestamp, None, amount_sat) - DONE
- add_incomplete_channel(gossip_id, scid, timestamp) - DONE

ChannelUpdates
- update_channel_update_to_timestamp_by_gossip_id(gossip_id_before, timestamp) - DONE
- update_channel_update_from_timestamp_by_gossip_id(gossip_id_after, timestamp) - DONE
- update_latest_channel_update_by_scid(scid, block_timestamp) - DONE

ChannelIdTranslation
- get_channel_id_translation_by_scid(scid)
- add_channel_id_translation(scid, 0, node_id_1, node_id_2) - DONE

NodesRawGossip (with JOINS)
- get_gossip_id_msg_type_timestamp_by_node_id(node_id) - DONE
- add_nodes_raw_gossip(gossip_id, node_id) - DONE

Nodes
- get_node_by_node_id(node_id_1) - DONE
- add_node(node_id, timestamp, timestamp) - DONE
- update_node_from_timestamp(node_id, timestamp) - DONE
- update_node_last_seen(node_id, timestamp) - DONE