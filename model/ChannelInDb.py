from dataclasses import dataclass
from datetime import datetime

@dataclass(frozen=True)
class ChannelInDb:
    scid: bytes
    source_node_id: bytes
    target_node_id: bytes
    from_timestamp: datetime  # corresponds to lower(validity)
    to_timestamp: datetime       # corresponds to upper(validity)
    amount_sat: int