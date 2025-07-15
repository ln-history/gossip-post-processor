from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class Node:
    node_id: bytes
    from_timestamp: datetime  # corresponds to lower(validity)
    last_seen: datetime  # corresponds to upper(validity)
