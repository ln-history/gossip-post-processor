# gossip-post-processor


- `handle_channel_announcement`
raw_hex parsen in ChannelAnnouncemnt object
Check -> Ist Bitcoin Block für die gegebene Höhe in CouchDB vorhanden?
timestamp aus Bitcoin Block extrahieren
txId aus bitcoinBlock extrahieren
Check -> Existiert tx auf der node?
Check -> Existiert txOutput in der tx?
amount_sat aus txOutput extrahieren
Check (Cache) -> Sind node_announcements von beiden node_ids vorhanden?

Nur hinzufügen, wenn alle checks bestanden worden sind
RawGossip in DB hinzufügen
ChannelAnnouncement in DB hinzufügen (mit korrektem Link)
last_seen von node_ids updaten

- `handle_node_announcement`
raw_hex parsen in NodeAnnouncement object
Check (Cache) -> Ist node_id bereits announced worden

RawGossip in DB hinzufügen
NodeAnnouncement in DB hinzufügen

- `handle_channel_update`
raw_hex parsen zu ChannelUpdate object
Check (Cache) -> wurde scid announced ?

Wenn scid announced wurde, können wir sich sein, dass die node_ids existieren, daher weiter:

translation von scid zu node_ids via 

RawGossip in DB hinzufügen
ChannelUpdate hinzufügen

last_seen für node_ids

- `handle_channel_dying`


## How it works

Key: nodes:{node_id}

--

channels:
{scid} -> {sha256(raw_hex) of channel_announcement}
```


-> new_raw_gossip
    Create a new row in `RawGossip` and set the values by the consumed Kafka event
        - SET `id` to the `metadata`.`id` 
        - SET `recorded_at_timestamp` to `metadata.timestamp`, if this value is null -> Call a function to retrieve the timestamp
        - SET `raw_gossip` to `raw_hex` 
        - SET `gossip_message_type` to `metadata.type`

CASE 256 - channel_announcement
-> channel_announcement (new channel)
    1. Use the lnhistoryclient.parser.parser.parse_channel_announcement_message to retrieve all information of the `raw_hex` of the Kafka event
    2. Get the timestamp of the mined block by blockheight by the parsed value of `scid` utilizing COUCHDB->blocks database 
    3. Get the amount in satoshi of the funding transaction by the parsed value of `scid` utilizing COUCHDB->blocks database AND json-rpc-explorer Bitcoin Full-node
    4. CALL new_raw_gossip
    5. Create a new row in `Channels` table that links with `RawGossip`.`id`:
        SET `gossip.id` to `metadata`.`id`
        SET `scid` to `scid` from the parsed value of `scid`
        SET `from_timestamp` to `timestamp` of the block from couchDB (contains funding tx)
        SET `to_timestamp` to NULL
        SET `amount_sat` to `amount_sat`
    
    6. Add a new row in `ChannelIdTranslation`:
        SET `scid` to `scid` from the parsed value of `scid`
        SET `direction` to 0 from the parsed value of `channel_flags`
        SET `source_node_id` to node_id_1 from the parsed value of `node_id_1` // Might think of a lexicographical-check
        SET `target_node_id` to node_id_2 from the parsed value of `node_id_2`

    7. Add a new row in `ChannelIdTranslation`:
        SET `scid` to `scid` from the parsed value of `scid`
        SET `direction` to 1 from the parsed value of `channel_flags`
        SET `source_node_id` to node_id_2 from the parsed value of `node_id_2` // Might think of a lexicographical-check
        SET `target_node_id` to node_id_1 from the parsed value of `node_id_1`

    8. Find row in `Nodes` where `node_id` = `source_node_id` 
        SET `last_seen` to `timestamp`

    9. Find row in `Nodes` where `node_id` = `target_node_id`
        SET `last_seen` to `timestamp`

CASE 257 - node_announcement 
    1. Use the lnhistoryclient.parser.node_announcement_parser to retrieve all information (like node_id) of the `raw_hex` of the Kafka event
    2. Utilize the `nodes` set in the cache to find out if this is a completly new `node_id` or if it has already been announced previously
        - IF `node_id` is not in the cache
            CALL new_raw_gossip
            Create a new row in `Nodes` table that linkes with `RawGossip`.`id`
                - SET `node_id` to `node_id`
                - SET `from_timestamp` to `timestamp` from the parsed value of `timestamp`
                - SET `last_seen` to `timestamp` from the parsed value of `timestamp`

            Create a new row in `NodesRawGossip` connecting `Nodes` and `RawGossip`.`id`
                - SET `node_id` to `node_id`
                - SET `gossip_id` to `RawGossip`.`id`

        - IF `node_id` is in the cache
            CALL new_raw_gossip
            CREATE a new row in `NodesRawGossip` connecting `Nodes` and `RawGossip`.`id`
                - SET `node_id` to `node_id`
                - SET `gossip_id` to `RawGossip`.`id`
            UPDATE the `Nodes`.`last_seen` to the `timestamp` from the parsed value


CASE 258 - channel_update 
    1. Use the lnhistoryclient.parser.chanel_update_parser to retrieve all information of the `raw_hex` of the Kafka event
    2. Find row in `ChannelUpdates` where `gossip_id` == `metadata`.`id`
        SET `update_timestamp_from` to the parsed value of `timestamp`
    3. CALL new_raw_gossip 
    4. Add a new row in `ChannelUpdates` that links with `RawGossip`.`id`
        SET `gossip_id` to `RawGossip`.`id` 
        SET `scid` to the parsed value of `scid`
        SET `direction` to the parsed value of `channel_flags` (first_bit)
        SET `update_timestamp_from` to parsed value of `timestamp`
        SET `update_timestamp_to` to NULL

    5. Query the `ChannelIdTranslation` table with `scid` and `direction` to get the `node_id` that sent the channel_update
    6. Find row in `Nodes` where `node_id` == `node_id` 
        SET `last_seen` to `timestamp` 

CASE 4103 - channel_dying (remove channel)
    1. Use the lnhistoryclient.parser.channel_dying_parser to retrieve all information of the `raw_hex` of the Kafka event
    2. Get the timestamp of the mined block by blockheight by the parsed value of `scid` utilizing COUCHDB->blocks database
    3. Utilize the `channels` cache and get the `gossip`.`id` by the parsed value of `scid`
    4. Use the `gossip`.`id` value to get the row in the `Channels` table and update: 
        SET `to_timestamp` to `timestamp` of the block from couchDB (which contains the closing tx)
