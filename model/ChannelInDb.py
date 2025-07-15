from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass(frozen=True)
class ChannelInDb:
    scid: bytes
    source_node_id: Optional[bytes]
    target_node_id: Optional[bytes]
    from_timestamp: datetime  # corresponds to lower(validity)
    to_timestamp: Optional[datetime]  # corresponds to upper(validity)
    amount_sat: int
