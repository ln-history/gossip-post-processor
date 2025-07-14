from typing import Any, List, Optional, Union
from contextlib import contextmanager

from psycopg2 import errors, pool
from psycopg2.extensions import connection, cursor

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

from model.Node import Node
from datetime import datetime


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


    @contextmanager
    def transaction(self) -> cursor.connection:
        conn = self._get_conn()
        try:
            cur = conn.cursor()
            yield cur
            conn.commit()
            self.logger.debug("Sucessfully committed transaction.")
        except Exception as e:
            conn.rollback()
            self.logger.critical(f"Transaction failed and rolled back: {e}")
            raise
        finally:
            cur.close()
            self._put_conn(conn)

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

    def add_raw_gossip(self, cur, gossip_id: bytes, gossip_type: int, timestamp: int, raw_gossip_bytes: bytes):
        cur.execute(
            """
            INSERT INTO raw_gossip (gossip_id, gossip_message_type, timestamp, raw_gossip)
            VALUES (%s, %s, to_timestamp(%s), %s)
            ON CONFLICT (gossip_id) DO NOTHING
            """,
            (gossip_id, gossip_type, timestamp, raw_gossip_bytes)
        )

    # --- CHANNELS ---

    def add_channel(self, cur, scid: str, source_node_id: bytes, target_node_id: bytes, from_timestamp: int, amount_sat: int) -> None:
        cur.execute(
            """
            INSERT INTO channels (scid, source_node_id, target_node_id, validity, amount_sat)
            VALUES (%s, %s, %s, tstzrange(to_timestamp(%s), NULL), %s)
            ON CONFLICT (scid) DO NOTHING
            """,
            (scid, source_node_id, target_node_id, from_timestamp, amount_sat)
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

    def add_channels_raw_gossip(self, cur, gossip_id: bytes, scid: str) -> None:
        cur.execute(
            """
            INSERT INTO channels_raw_gossip (gossip_id, scid)
            VALUES (%s, %s)
            ON CONFLICT (gossip_id) DO NOTHING
            """,
            (gossip_id, scid)
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

    # --- NODES RAW GOSSIP ---
    def add_nodes_raw_gossip(self, cur, gossip_id, node_id) -> None:
        cur.execute(
            """
            INSERT INTO nodes_raw_gossip (gossip_id, node_id)
            VALUES (%s, %s)
            ON CONFLICT DO NOTHING
            """,
            (gossip_id, node_id)
        )

    # --- NODES ---
    def get_node_by_node_id(self, cur, node_id: bytes) -> Optional[Node]:
        cur.execute(
            "SELECT node_id, lower(validity), upper(validity) FROM nodes WHERE node_id = %s",
            (node_id,)
        )
        row = cur.fetchone()
        if row:
            raw_node_id = bytes(row[0])
            from_ts: datetime = row[1]
            last_seen: datetime = row[2]

            return Node(
                node_id=raw_node_id,
                from_timestamp=from_ts,
                last_seen=last_seen
            )
        return None

    def add_node(self, cur, node_id: bytes, from_ts: int, last_seen_ts: int) -> None:
        cur.execute(
            """
            INSERT INTO nodes (node_id, validity)
            VALUES (%s, tstzrange(to_timestamp(%s), to_timestamp(%s), '[]'))
            """,
            (node_id, from_ts, last_seen_ts)
        )

    def update_node_from_timestamp(self, cur, node_id: bytes, new_from_ts: int) -> None:
        cur.execute(
            """
            UPDATE nodes
            SET validity = tstzrange(to_timestamp(%s), upper(validity), '[]')
            WHERE node_id = %s
            """,
            (new_from_ts, node_id)
        )

    def update_node_last_seen(self, cur, node_id: bytes, new_last_seen_ts: int) -> None:
        cur.execute(
            """
            UPDATE nodes
            SET validity = tstzrange(lower(validity), to_timestamp(%s), '[]')
            WHERE node_id = %s
            """,
            (new_last_seen_ts, node_id)
        )
