import hashlib
import json
from typing import Optional

from lnhistoryclient.model.types import GossipCache
from valkey import Valkey

from config import VALKEY_HOST, VALKEY_PASSWORD, VALKEY_PORT
from CustomLogger import CustomLogger


class ValkeyCache:
    GLOBAL_KEY = "__global__"

    def __init__(self, logger: CustomLogger):
        self.logger = logger
        self.client = Valkey(
            host=VALKEY_HOST,
            port=int(VALKEY_PORT),
            password=VALKEY_PASSWORD,
            db=0,
        )
        self.logger.info("Valkey client initialized")

    @staticmethod
    def hash_raw_hex(raw_hex: str) -> str:
        return hashlib.sha256(raw_hex.encode()).hexdigest()

    def get_metadata_key(self, msg_type: str, msg_hash: str) -> str:
        return f"gossip:{msg_type}:{msg_hash}"

    def has_node(self, node_id: bytes) -> bool:
        return self.client.exists(f"node:{node_id.hex()}") == 1

    def cache_node(self, node_id: bytes) -> None:
        self.client.set(f"node:{node_id.hex()}", "1")
