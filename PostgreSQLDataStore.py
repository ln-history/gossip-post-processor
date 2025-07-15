from contextlib import contextmanager
from datetime import datetime
from logging import Logger
from typing import List, Optional

from psycopg2 import pool
from psycopg2.extensions import connection, cursor

from config import (
    POSTGRE_SQL_DB,
    POSTGRE_SQL_HOST,
    POSTGRE_SQL_PASSWORD,
    POSTGRE_SQL_PORT,
    POSTGRE_SQL_USER,
)
from model.ChannelInDb import ChannelInDb
from model.ChannelUpdateInDb import ChannelUpdateInDb
from model.Node import Node


class PostgreSQLDataStore:
    def __init__(self, logger: Logger):
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

    # --- RawGossip ---
    def add_raw_gossip(
        self, cur: cursor.connection, gossip_id: bytes, gossip_type: int, timestamp: int, raw_gossip_bytes: bytes
    ):
        cur.execute(
            """
            INSERT INTO raw_gossip (gossip_id, gossip_message_type, timestamp, raw_gossip)
            VALUES (%s, %s, to_timestamp(%s), %s)
            ON CONFLICT (gossip_id) DO NOTHING
            """,
            (gossip_id, gossip_type, timestamp, raw_gossip_bytes),
        )

    # --- CHANNELS ---
    def get_channel_by_scid(self, cur: cursor.connection, scid: str) -> Optional[ChannelInDb]:
        cur.execute(
            """
            SELECT scid, source_node_id, target_node_id, lower(validity), upper(validity), amount_sat 
            FROM channels
            WHERE scid = %s
            """,
            (scid,),
        )
        row = cur.fetchone()
        if row:
            scid = row[0]
            source_node_id: bytes = row[1].tobytes() if row[1] is not None else None
            target_node_id: bytes = row[1].tobytes() if row[2] is not None else None
            from_timestamp: datetime = row[3]
            to_timestamp: datetime = row[4]
            amount_sat: int = row[5]

            return ChannelInDb(
                scid=scid,
                source_node_id=source_node_id,
                target_node_id=target_node_id,
                from_timestamp=from_timestamp,
                to_timestamp=to_timestamp,
                amount_sat=amount_sat,
            )
        return None

    def add_channel(
        self,
        cur: cursor.connection,
        scid: str,
        source_node_id: bytes,
        target_node_id: bytes,
        from_timestamp: int,
        amount_sat: int,
    ) -> None:
        cur.execute(
            """
            INSERT INTO channels (scid, source_node_id, target_node_id, validity, amount_sat)
            VALUES (%s, %s, %s, tstzrange(to_timestamp(%s), NULL), %s)
            ON CONFLICT (scid) DO NOTHING
            """,
            (scid, source_node_id, target_node_id, from_timestamp, amount_sat),
        )

    def add_incomplete_channel(self, cur: cursor.connection, scid: str, from_timestamp: int, amount_sat: int) -> None:
        cur.execute(
            """
            INSERT INTO channels (scid, source_node_id, target_node_id, validity, amount_sat)
            VALUES (%s, NULL, NULL, tstzrange(to_timestamp(%s), NULL), %s)
            ON CONFLICT (scid) DO NOTHING
            """,
            (scid, from_timestamp, amount_sat),
        )

    def add_channels_raw_gossip(self, cur: cursor.connection, gossip_id: bytes, scid: str) -> None:
        cur.execute(
            """
            INSERT INTO channels_raw_gossip (gossip_id, scid)
            VALUES (%s, %s)
            ON CONFLICT (gossip_id) DO NOTHING
            """,
            (gossip_id, scid),
        )

    def update_channel_close(self, cur: cursor.connection, scid: str, to_timestamp: int):
        cur.execute(
            """
            UPDATE channels
            SET validity = tstzrange(upper(validity), to_timestamp(%s))
            WHERE scid = %s
            """,
            (to_timestamp, scid),
        )

    # --- CHANNEL UPDATES ---
    def get_channel_updates_by_scid_and_direction(
        self, cur: cursor.connection, scid: str, direction: bool
    ) -> List[ChannelUpdateInDb]:
        cur.execute(
            "SELECT scid, direction, lower(validity), upper(validity) FROM channel_updates WHERE scid = %s AND direction = %s",
            (scid, direction),
        )
        rows = cur.fetchall()
        result: List[ChannelUpdateInDb] = []
        for row in rows:
            scid = row[0]
            direction = bool(row[1])
            from_ts: datetime = row[2]
            last_seen: datetime = row[3]

            result.append(
                ChannelUpdateInDb(scid=scid, direction=direction, from_timestamp=from_ts, to_timestamp=last_seen)
            )
        return result

    def get_channel_update_by_scid(self, cur: cursor.connection, scid: str) -> List[ChannelUpdateInDb]:
        cur.execute(
            "SELECT scid, direction, lower(validity), upper(validity) FROM channel_updates WHERE scid = %s", (scid)
        )
        rows = cur.fetchall()
        result: List[ChannelUpdateInDb] = []
        for row in rows:
            scid = row[0]
            direction = bool(row[1])
            from_ts: datetime = row[2]
            last_seen: datetime = row[3]

            result.append(
                ChannelUpdateInDb(scid=scid, direction=direction, from_timestamp=from_ts, to_timestamp=last_seen)
            )
        return result

    def add_channel_update(self, cur: cursor.connection, scid: str, direction: bool, from_timestamp: int) -> None:
        cur.execute(
            """
            INSERT INTO channel_updates (scid, direction, validity)
            VALUES (%s, %s, tstzrange(to_timestamp(%s), NULL))
            ON CONFLICT DO NOTHING
            """,
            (scid, direction, from_timestamp),
        )

    def update_channel_update_to_timestamp_by_cu_data(
        self, cur: cursor.connection, scid: str, direction: bool, from_update_timestamp: int, to_update_timestamp: int
    ) -> None:
        cur.execute(
            """
            UPDATE channel_updates
            SET validity = tstzrange(lower(validity), to_timestamp(%s))
            WHERE scid = %s
            AND direction = %s
            AND lower(validity) = to_timestamp(%s)
            """,
            (to_update_timestamp, scid, direction, from_update_timestamp),
        )

    def update_channel_update_from_timestamp_by_cu_data(
        self, cur: cursor.connection, scid: str, direction: bool, from_update_timestamp: int, to_update_timestamp: int
    ) -> None:
        cur.execute(
            """
            UPDATE channel_updates
            SET validity = tstzrange(to_timestamp(%s), upper(validity))
            WHERE scid = %s
            AND direction = %s
            AND lower(validity) = to_timestamp(%s)
            """,
            (to_update_timestamp, scid, direction, from_update_timestamp),
        )

    # --- NODES RAW GOSSIP ---
    def add_nodes_raw_gossip(self, cur: cursor.connection, gossip_id: bytes, node_id: bytes) -> None:
        cur.execute(
            """
            INSERT INTO nodes_raw_gossip (gossip_id, node_id)
            VALUES (%s, %s)
            ON CONFLICT DO NOTHING
            """,
            (gossip_id, node_id),
        )

    # --- NODES ---
    def get_node_by_node_id(self, cur: cursor.connection, node_id: bytes) -> Optional[Node]:
        cur.execute("SELECT node_id, lower(validity), upper(validity) FROM nodes WHERE node_id = %s", (node_id,))
        row = cur.fetchone()
        if row:
            raw_node_id = bytes(row[0])
            from_ts: datetime = row[1]
            last_seen: datetime = row[2]

            return Node(node_id=raw_node_id, from_timestamp=from_ts, last_seen=last_seen)
        return None

    def add_node(self, cur: cursor.connection, node_id: bytes, from_ts: int, last_seen_ts: int) -> None:
        cur.execute(
            """
            INSERT INTO nodes (node_id, validity)
            VALUES (%s, tstzrange(to_timestamp(%s), to_timestamp(%s), '[]'))
            ON CONFLICT DO NOTHING
            """,
            (node_id, from_ts, last_seen_ts),
        )

    def update_node_from_timestamp(self, cur: cursor.connection, node_id: bytes, new_from_ts: int) -> None:
        cur.execute(
            """
            UPDATE nodes
            SET validity = tstzrange(to_timestamp(%s), upper(validity), '[]')
            WHERE node_id = %s
            """,
            (new_from_ts, node_id),
        )

    def update_node_last_seen(self, cur: cursor.connection, node_id: bytes, new_last_seen_ts: int) -> None:
        cur.execute(
            """
            UPDATE nodes
            SET validity = tstzrange(lower(validity), to_timestamp(%s), '[]')
            WHERE node_id = %s
            """,
            (new_last_seen_ts, node_id),
        )
