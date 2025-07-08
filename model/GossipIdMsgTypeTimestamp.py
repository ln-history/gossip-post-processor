from dataclasses import dataclass


@dataclass(frozen=True)
class GossipIdMsgTypeTimestamp:
    gossip_id: bytes
    msg_type: int
    timestamp: int
