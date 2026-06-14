import logging
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import aiosqlite

logger = logging.getLogger(__name__)

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS metrics_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    db_id TEXT NOT NULL,
    timestamp REAL NOT NULL,
    active_connections INTEGER,
    idle_connections INTEGER,
    total_connections INTEGER,
    cache_hit_ratio REAL,
    tps REAL,
    db_size_bytes INTEGER
);

CREATE INDEX IF NOT EXISTS idx_metrics_db_ts ON metrics_history(db_id, timestamp);

CREATE TABLE IF NOT EXISTS slow_queries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    db_id TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    query_text TEXT NOT NULL,
    username TEXT,
    client_addr TEXT,
    duration_seconds REAL NOT NULL,
    captured_at REAL NOT NULL,
    query_start REAL,
    wait_event_type TEXT,
    wait_event TEXT,
    pid INTEGER
);

CREATE INDEX IF NOT EXISTS idx_slow_db_ts ON slow_queries(db_id, captured_at);
CREATE INDEX IF NOT EXISTS idx_slow_fingerprint ON slow_queries(fingerprint);

CREATE TABLE IF NOT EXISTS top_tables (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    db_id TEXT NOT NULL,
    timestamp REAL NOT NULL,
    schema_name TEXT NOT NULL,
    table_name TEXT NOT NULL,
    total_size INTEGER,
    table_size INTEGER,
    indexes_size INTEGER
);

CREATE INDEX IF NOT EXISTS idx_tables_db_ts ON top_tables(db_id, timestamp);
"""


class SQLiteStore:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._db: Optional[aiosqlite.Connection] = None

    async def initialize(self):
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._db = await aiosqlite.connect(self.db_path)
        self._db.row_factory = aiosqlite.Row
        await self._db.executescript(SCHEMA_SQL)
        await self._db.execute("PRAGMA journal_mode=WAL")
        await self._db.execute("PRAGMA synchronous=NORMAL")
        await self._db.commit()
        logger.info(f"SQLite store initialized at {self.db_path}")

    async def close(self):
        if self._db:
            await self._db.close()
            self._db = None

    async def insert_metrics(self, db_id: str, metrics: Dict[str, Any]):
        await self._db.execute(
            """INSERT INTO metrics_history
            (db_id, timestamp, active_connections, idle_connections,
             total_connections, cache_hit_ratio, tps, db_size_bytes)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                db_id,
                time.time(),
                metrics.get("active_connections", 0),
                metrics.get("idle_connections", 0),
                metrics.get("total_connections", 0),
                metrics.get("cache_hit_ratio", 0),
                metrics.get("tps", 0),
                metrics.get("db_size_bytes", 0),
            ),
        )
        await self._db.commit()

    async def insert_slow_query(self, db_id: str, query_data: Dict[str, Any]):
        await self._db.execute(
            """INSERT INTO slow_queries
            (db_id, fingerprint, query_text, username, client_addr,
             duration_seconds, captured_at, query_start, wait_event_type, wait_event, pid)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                db_id,
                query_data["fingerprint"],
                query_data["query_text"],
                query_data.get("username"),
                query_data.get("client_addr"),
                query_data["duration_seconds"],
                time.time(),
                query_data.get("query_start"),
                query_data.get("wait_event_type"),
                query_data.get("wait_event"),
                query_data.get("pid"),
            ),
        )
        await self._db.commit()

    async def insert_top_tables(self, db_id: str, tables: List[Dict[str, Any]]):
        ts = time.time()
        await self._db.executemany(
            """INSERT INTO top_tables
            (db_id, timestamp, schema_name, table_name, total_size, table_size, indexes_size)
            VALUES (?, ?, ?, ?, ?, ?, ?)""",
            [
                (
                    db_id, ts,
                    t["schema_name"], t["table_name"],
                    t["total_size"], t["table_size"], t["indexes_size"],
                )
                for t in tables
            ],
        )
        await self._db.commit()

    async def get_metrics_history(
        self, db_id: str, start_time: float, end_time: Optional[float] = None
    ) -> List[Dict[str, Any]]:
        end_time = end_time or time.time()
        cursor = await self._db.execute(
            """SELECT timestamp, active_connections, idle_connections,
                      total_connections, cache_hit_ratio, tps, db_size_bytes
            FROM metrics_history
            WHERE db_id = ? AND timestamp >= ? AND timestamp <= ?
            ORDER BY timestamp ASC""",
            (db_id, start_time, end_time),
        )
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]

    async def get_slow_queries(
        self,
        db_id: str,
        window_seconds: int = 3600,
        min_duration: float = 0,
        search: str = "",
        limit: int = 200,
    ) -> List[Dict[str, Any]]:
        since = time.time() - window_seconds
        query = """SELECT db_id, fingerprint, query_text, username, client_addr,
                          duration_seconds, captured_at, query_start,
                          wait_event_type, wait_event, pid
                   FROM slow_queries
                   WHERE db_id = ? AND query_start >= ? AND duration_seconds >= ?"""
        params: list = [db_id, since, min_duration]

        if search:
            query += " AND query_text LIKE ?"
            params.append(f"%{search}%")

        query += " ORDER BY query_start DESC LIMIT ?"
        params.append(limit)

        cursor = await self._db.execute(query, params)
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]

    async def get_latest_top_tables(self, db_id: str) -> List[Dict[str, Any]]:
        cursor = await self._db.execute(
            """SELECT schema_name, table_name, total_size, table_size, indexes_size
            FROM top_tables
            WHERE db_id = ? AND timestamp = (
                SELECT MAX(timestamp) FROM top_tables WHERE db_id = ?
            )
            ORDER BY total_size DESC""",
            (db_id, db_id),
        )
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]

    async def prune_old_data(self, retention_hours: int = 168):
        cutoff = time.time() - (retention_hours * 3600)
        await self._db.execute(
            "DELETE FROM metrics_history WHERE timestamp < ?", (cutoff,)
        )
        await self._db.execute(
            "DELETE FROM slow_queries WHERE captured_at < ?", (cutoff,)
        )
        await self._db.execute(
            "DELETE FROM top_tables WHERE timestamp < ?", (cutoff,)
        )
        await self._db.commit()
        logger.info(f"Pruned data older than {retention_hours}h")
