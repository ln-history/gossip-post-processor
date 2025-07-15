from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional


@dataclass(frozen=True)
class ChannelUpdateInDb:
    scid: str
    direction: bool
    from_timestamp: datetime  # corresponds to lower(validity)
    to_timestamp: Optional[datetime]  # corresponds to upper(validity)

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, ChannelUpdateInDb):
            return NotImplemented
        return (
            self.scid == other.scid
            and self.direction == other.direction
            and self.from_timestamp == other.from_timestamp
        )

    def __hash__(self) -> int:
        return hash((self.scid, self.direction, self.from_timestamp))
