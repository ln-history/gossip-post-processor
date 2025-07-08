from typing import Any, List, Optional, Union

from psycopg2 import errors, pool
from psycopg2.extensions import connection

from config import (
    POSTGRE_SQL_DB,
    POSTGRE_SQL_HOST,
    POSTGRE_SQL_PASSWORD,
    POSTGRE_SQL_PORT,
    POSTGRE_SQL_USER,
)
from CustomLogger import CustomLogger
from model.ChannelIdTranslationResult import ChannelIdTranslationResult
from model.GossipIdMsgType import GossipIdMsgType
from model.GossipIdMsgTypeTimestamp import GossipIdMsgTypeTimestamp


class PostgreSQLDataStore:
    def __init__(self, logger: CustomLogger):
        self.logger = logger
        try:
            self.pool = pool.SimpleConnectionPool(
                1,
                10,
                dbname=POSTGRE_SQL_DB,
                user=POSTGRE_SQL_USER,
                password=POSTGRE_SQL_PASSWORD,
                host=POSTGRE_SQL_HOST,
                port=POSTGRE_SQL_PORT,
            )
            self.logger.info(f"PostgreSQL connection pool initialized. Connected to database {POSTGRE_SQL_DB}")
        except Exception:
            self.logger.exception("Failed to initialize PostgreSQL connection pool.")
            raise

    def _get_conn(self) -> connection:
        return self.pool.getconn()

    def _put_conn(self, conn: connection) -> None:
        self.pool.putconn(conn)

    def close(self) -> None:
        self.pool.closeall()
        self.logger.info("PostgreSQL connection pool closed.")

    def execute(
        self,
        sql: str,
        params: Optional[Union[tuple[Any, ...], list[Any]]] = None,
        debug_message: Optional[str] = None,
    ) -> None:
        conn = self._get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                conn.commit()
                if debug_message:
                    self.logger.debug(debug_message)
        except errors.UniqueViolation as e:
            conn.rollback()
            self.logger.critical(f"Duplicate entry ignored: {e}")
        except Exception as e:
            conn.rollback()
            self.logger.critical(f"SQL execution failed: {e}")
        finally:
            self._put_conn(conn)

    # --- RawGossip ---

    def add_raw_gossip(
        self,
        gossip_id: bytes,
        gossip_type: int,
        timestamp: int,
        raw_gossip_bytes: bytes,
    ) -> None:
        self.execute(
            """
            INSERT INTO raw_gossip (gossip_id, gossip_message_type, timestamp, raw_gossip)
            VALUES (%s, %s, to_timestamp(%s), %s)
            ON CONFLICT (gossip_id) DO NOTHING
            """,
            (gossip_id, gossip_type, timestamp, raw_gossip_bytes),
            debug_message=f"[insert_raw_gossip] Inserted RawGossip: gossip_id={gossip_id.hex()}, type={gossip_type}, timestamp={timestamp}",
        )

    # --- CHANNELS ---

    def add_channel(
        self, gossip_id: bytes, scid: str, from_timestamp: int, to_timestamp: Optional[int], amount_sat: int
    ) -> None:
        self.execute(
            """
            INSERT INTO channels (gossip_id, scid, from_timestamp, to_timestamp, amount_sat)
            VALUES (%s, %s, to_timestamp(%s), to_timestamp(%s), %s)
            ON CONFLICT (scid) DO NOTHING
            """,
            (gossip_id, scid, from_timestamp, to_timestamp, amount_sat),
            debug_message=f"[add_channel] Added channel: scid={scid}",
        )

    def add_incomplete_channel(self, gossip_id: bytes, scid: str, from_timestamp: int) -> None:
        self.execute(
            """
            INSERT INTO channels (gossip_id, scid, from_timestamp)
            VALUES (%s, %s, to_timestamp(%s))
            ON CONFLICT (scid) DO NOTHING
            """,
            (gossip_id, scid, from_timestamp),
            debug_message=f"[add_incomplete_channel] Inserted incomplete channel: scid={scid}",
        )

    # --- CHANNEL RAW GOSSIP ---
    def get_gossip_id_msg_type_by_scid(self, scid: str) -> List[GossipIdMsgType]:
        conn = self._get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT crg.gossip_id, rg.gossip_message_type
                    FROM channels_raw_gossip crg
                    JOIN raw_gossip rg ON crg.gossip_id = rg.gossip_id
                    WHERE crg.scid = %s
                    """,
                    (scid,),
                )
                rows = cur.fetchall()
                return [GossipIdMsgType(gossip_id=row[0], msg_type=row[1]) for row in rows]
        except Exception as e:
            self.logger.critical(f"[get_gossip_id_msg_type_by_scid] Error: {e}")
            return []
        finally:
            self._put_conn(conn)

    def get_gossip_id_msg_type_timestamp_by_scid(self, scid: str) -> List[GossipIdMsgTypeTimestamp]:
        conn = self._get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT crg.gossip_id, rg.gossip_message_type, EXTRACT(EPOCH FROM c.from_timestamp)::INT
                    FROM channels_raw_gossip crg
                    JOIN raw_gossip rg ON crg.gossip_id = rg.gossip_id
                    JOIN channels c ON crg.gossip_id = c.gossip_id
                    WHERE crg.scid = %s
                    """,
                    (scid,),
                )
                rows = cur.fetchall()
                return [GossipIdMsgTypeTimestamp(gossip_id=row[0], msg_type=row[1], timestamp=row[2]) for row in rows]
        except Exception as e:
            self.logger.critical(f"[get_gossip_id_msg_type_timestamp_by_scid] Error: {e}")
            return []
        finally:
            self._put_conn(conn)

    def add_channels_raw_gossip(self, gossip_id: bytes, scid: str) -> None:
        self.execute(
            """
            INSERT INTO channels_raw_gossip (gossip_id, scid)
            VALUES (%s, %s)
            ON CONFLICT (gossip_id) DO NOTHING
            """,
            (gossip_id, scid),
            debug_message=f"[add_channels_raw_gossip] gossip_id={gossip_id.hex()}, scid={scid}",
        )

    # --- CHANNEL UPDATES ---

    def add_channel_update(
        self,
        gossip_id: bytes,
        scid: str,
        direction: int,
        from_update_timestamp: int,
        to_update_timestamp: Optional[int] = None,
    ) -> None:
        self.execute(
            """
            INSERT INTO channel_updates (gossip_id, scid, direction, from_update_timestamp, to_update_timestamp)
            VALUES (%s, %s, %s, to_timestamp(%s), to_timestamp(%s))
            ON CONFLICT (gossip_id) DO NOTHING
            """,
            (
                gossip_id,
                scid,
                bool(direction),
                from_update_timestamp,
                to_update_timestamp if to_update_timestamp is not None else None,
            ),
            debug_message=f"[add_channel_update] gossip_id={gossip_id.hex()}, scid={scid}, direction={direction}",
        )

    def update_channel_update_to_timestamp_by_gossip_id(self, gossip_id: bytes, timestamp: int) -> None:
        self.execute(
            """
            UPDATE channel_updates
            SET to_update_timestamp = to_timestamp(%s)
            WHERE gossip_id = %s
            """,
            (timestamp, gossip_id),
            debug_message=f"[update_channel_update_to_timestamp_by_gossip_id] gossip_id={gossip_id.hex()}",
        )

    def update_channel_update_from_timestamp_by_gossip_id(self, gossip_id: bytes, timestamp: int) -> None:
        self.execute(
            """
            UPDATE channel_updates
            SET from_update_timestamp = to_timestamp(%s)
            WHERE gossip_id = %s
            """,
            (timestamp, gossip_id),
            debug_message=f"[update_channel_update_from_timestamp_by_gossip_id] gossip_id={gossip_id.hex()}",
        )

    def update_latest_channel_update_by_scid(self, scid: str, timestamp: int) -> None:
        self.execute(
            """
            UPDATE channel_updates
            SET to_update_timestamp = to_timestamp(%s)
            WHERE scid = %s AND to_update_timestamp IS NULL
            """,
            (timestamp, scid),
            debug_message=f"[update_latest_channel_update_by_scid] scid={scid}",
        )

    # --- CHANNEL ID TRANSLATION ---

    def get_channel_id_translation_by_scid(self, scid: str) -> List[ChannelIdTranslationResult]:
        conn = self._get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT scid, direction, encode(source_node_id, 'hex'), encode(target_node_id, 'hex')
                    FROM channel_id_translation
                    WHERE scid = %s
                    """,
                    (scid,),
                )
                rows = cur.fetchall()
                return [
                    ChannelIdTranslationResult(
                        scid=row[0],
                        direction=row[1],
                        source_node_id=row[2],
                        target_node_id=row[3],
                    )
                    for row in rows
                ]
        except Exception as e:
            self.logger.critical(f"[get_channel_id_translation_by_scid] Error: {e}")
            return []
        finally:
            self._put_conn(conn)

    def add_channel_id_translation(self, scid: str, direction: int, node_id_1: bytes, node_id_2: bytes) -> None:
        self.execute(
            """
            INSERT INTO channel_id_translation (scid, direction, source_node_id, target_node_id)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (scid, direction) DO NOTHING
            """,
            (scid, bool(direction), node_id_1, node_id_2),
            debug_message=f"[add_channel_id_translation] scid={scid}, direction={direction}",
        )

    # --- NODES ---

    def get_node_by_node_id(self, node_id: bytes) -> Optional[dict]:
        conn = self._get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT node_id, from_timestamp, last_seen
                    FROM nodes
                    WHERE node_id = %s
                    """,
                    (node_id,),
                )
                row = cur.fetchone()
                if row:
                    return {
                        "node_id": row[0],
                        "from_timestamp": row[1],
                        "last_seen": row[2],
                    }
                return None
        except Exception as e:
            self.logger.critical(f"[get_node_by_node_id] Error: {e}")
            return None
        finally:
            self._put_conn(conn)

    def add_node(self, node_id: bytes, from_timestamp: int, last_seen: int) -> None:
        self.execute(
            """
            INSERT INTO nodes (node_id, from_timestamp, last_seen)
            VALUES (%s, to_timestamp(%s), to_timestamp(%s))
            ON CONFLICT (node_id) DO NOTHING
            """,
            (node_id, from_timestamp, last_seen),
            debug_message=f"[add_node] node_id={node_id.hex()}",
        )

    def update_node_from_timestamp(self, node_id: bytes, timestamp: int) -> None:
        self.execute(
            """
            UPDATE nodes
            SET from_timestamp = to_timestamp(%s)
            WHERE node_id = %s
            """,
            (timestamp, node_id),
            debug_message=f"[update_node_from_timestamp] node_id={node_id.hex()}",
        )

    def update_node_last_seen(self, node_id: bytes, timestamp: int) -> None:
        self.execute(
            """
            UPDATE nodes
            SET last_seen = to_timestamp(%s)
            WHERE node_id = %s
            """,
            (timestamp, node_id),
            debug_message=f"[update_node_last_seen] node_id={node_id.hex()}",
        )

    # --- NODES RAW GOSSIP ---

    def get_gossip_id_msg_type_timestamp_by_node_id(self, node_id: bytes) -> List[GossipIdMsgTypeTimestamp]:
        conn = self._get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT nrg.gossip_id, rg.gossip_message_type, EXTRACT(EPOCH FROM n.from_timestamp)::INT
                    FROM nodes_raw_gossip nrg
                    JOIN raw_gossip rg ON nrg.gossip_id = rg.gossip_id
                    JOIN nodes n ON nrg.node_id = n.node_id
                    WHERE nrg.node_id = %s
                    """,
                    (node_id,),
                )
                rows = cur.fetchall()
                return [GossipIdMsgTypeTimestamp(gossip_id=row[0], msg_type=row[1], timestamp=row[2]) for row in rows]
        except Exception as e:
            self.logger.critical(f"[get_gossip_id_msg_type_timestamp_by_node_id] Error: {e}")
            return []
        finally:
            self._put_conn(conn)

    def add_nodes_raw_gossip(self, gossip_id: bytes, node_id: bytes) -> None:
        self.execute(
            """
            INSERT INTO nodes_raw_gossip (gossip_id, node_id)
            VALUES (%s, %s)
            ON CONFLICT (gossip_id) DO NOTHING
            """,
            (gossip_id, node_id),
            debug_message=f"[add_nodes_raw_gossip] gossip_id={gossip_id.hex()}, node_id={node_id.hex()}",
        )
