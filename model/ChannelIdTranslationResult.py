from dataclasses import dataclass


@dataclass(frozen=True)
class ChannelIdTranslationResult:
    scid: str
    direction: bool
    source_node_id: str
    target_node_id: str
