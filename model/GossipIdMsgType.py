from dataclasses import dataclass


@dataclass(frozen=True)
class GossipIdMsgType:
    gossip_id: bytes
    msg_type: int
