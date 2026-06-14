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

CREATE TABLE IF NOT EXISTS index_stats (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    db_id TEXT NOT NULL,
    timestamp REAL NOT NULL,
    schema_name TEXT NOT NULL,
    table_name TEXT NOT NULL,
    index_name TEXT NOT NULL,
    index_size_bytes INTEGER,
    idx_scan INTEGER,
    idx_tup_read INTEGER,
    idx_tup_fetch INTEGER,
    is_unique INTEGER DEFAULT 0,
    is_primary INTEGER DEFAULT 0,
    index_def TEXT,
    n_dead_tup INTEGER DEFAULT 0,
    n_tup_ins INTEGER DEFAULT 0,
    n_tup_upd INTEGER DEFAULT 0,
    n_tup_del INTEGER DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_idxstats_db_ts ON index_stats(db_id, timestamp);
CREATE INDEX IF NOT EXISTS idx_idxstats_name ON index_stats(db_id, index_name);

CREATE TABLE IF NOT EXISTS index_recommendations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    db_id TEXT NOT NULL,
    analyzed_at REAL NOT NULL,
    schema_name TEXT NOT NULL,
    table_name TEXT NOT NULL,
    index_name TEXT NOT NULL,
    category TEXT NOT NULL,
    risk_level TEXT NOT NULL,
    reason TEXT NOT NULL,
    drop_ddl TEXT,
    rollback_ddl TEXT,
    estimated_size_savings INTEGER DEFAULT 0,
    related_indexes TEXT,
    dismissed INTEGER DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_idxrec_db ON index_recommendations(db_id, analyzed_at);

CREATE TABLE IF NOT EXISTS missing_index_recommendations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    db_id TEXT NOT NULL,
    analyzed_at REAL NOT NULL,
    schema_name TEXT NOT NULL,
    table_name TEXT NOT NULL,
    suggested_columns TEXT NOT NULL,
    create_ddl TEXT NOT NULL,
    reason TEXT NOT NULL,
    source TEXT NOT NULL,
    estimated_benefit REAL DEFAULT 0,
    related_fingerprints TEXT,
    seq_scan_count INTEGER DEFAULT 0,
    seq_tup_read INTEGER DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_missidx_db ON missing_index_recommendations(db_id, analyzed_at);

CREATE TABLE IF NOT EXISTS slow_query_trends (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    db_id TEXT NOT NULL,
    hour_bucket REAL NOT NULL,
    fingerprint TEXT NOT NULL,
    query_pattern TEXT NOT NULL,
    occurrence_count INTEGER DEFAULT 0,
    total_duration REAL DEFAULT 0,
    avg_duration REAL DEFAULT 0,
    max_duration REAL DEFAULT 0,
    distinct_users INTEGER DEFAULT 0,
    top_users TEXT,
    top_clients TEXT
);
CREATE INDEX IF NOT EXISTS idx_sqtrend_db_hour ON slow_query_trends(db_id, hour_bucket);
CREATE INDEX IF NOT EXISTS idx_sqtrend_fp ON slow_query_trends(fingerprint, hour_bucket);

CREATE TABLE IF NOT EXISTS health_scores (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    db_id TEXT NOT NULL,
    timestamp REAL NOT NULL,
    overall_score REAL NOT NULL,
    dimension_scores TEXT NOT NULL,
    anomalies TEXT
);
CREATE INDEX IF NOT EXISTS idx_health_db_ts ON health_scores(db_id, timestamp);

CREATE TABLE IF NOT EXISTS alert_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    db_id TEXT NOT NULL,
    triggered_at REAL NOT NULL,
    dimension TEXT NOT NULL,
    severity TEXT NOT NULL,
    message TEXT NOT NULL,
    value REAL,
    threshold REAL,
    acknowledged INTEGER DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_alert_db_ts ON alert_history(db_id, triggered_at);
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
        await self._db.execute(
            "DELETE FROM index_stats WHERE timestamp < ?", (cutoff,)
        )
        await self._db.execute(
            "DELETE FROM index_recommendations WHERE analyzed_at < ?", (cutoff,)
        )
        await self._db.execute(
            "DELETE FROM missing_index_recommendations WHERE analyzed_at < ?", (cutoff,)
        )
        await self._db.execute(
            "DELETE FROM slow_query_trends WHERE hour_bucket < ?", (cutoff,)
        )
        await self._db.execute(
            "DELETE FROM health_scores WHERE timestamp < ?", (cutoff,)
        )
        await self._db.execute(
            "DELETE FROM alert_history WHERE triggered_at < ?", (cutoff,)
        )
        await self._db.commit()
        logger.info(f"Pruned data older than {retention_hours}h")

    # --- Index Stats ---

    async def insert_index_stats(self, db_id: str, stats: List[Dict[str, Any]]):
        ts = time.time()
        await self._db.executemany(
            """INSERT INTO index_stats
            (db_id, timestamp, schema_name, table_name, index_name,
             index_size_bytes, idx_scan, idx_tup_read, idx_tup_fetch,
             is_unique, is_primary, index_def, n_dead_tup, n_tup_ins, n_tup_upd, n_tup_del)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [
                (
                    db_id, ts,
                    s["schema_name"], s["table_name"], s["index_name"],
                    s.get("index_size_bytes", 0), s.get("idx_scan", 0),
                    s.get("idx_tup_read", 0), s.get("idx_tup_fetch", 0),
                    int(s.get("is_unique", False)), int(s.get("is_primary", False)),
                    s.get("index_def", ""),
                    s.get("n_dead_tup", 0), s.get("n_tup_ins", 0),
                    s.get("n_tup_upd", 0), s.get("n_tup_del", 0),
                )
                for s in stats
            ],
        )
        await self._db.commit()

    async def get_latest_index_stats(self, db_id: str) -> List[Dict[str, Any]]:
        cursor = await self._db.execute(
            """SELECT * FROM index_stats
            WHERE db_id = ? AND timestamp = (
                SELECT MAX(timestamp) FROM index_stats WHERE db_id = ?
            )
            ORDER BY index_size_bytes DESC""",
            (db_id, db_id),
        )
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]

    # --- Index Recommendations ---

    async def save_index_recommendations(self, db_id: str, recs: List[Dict[str, Any]]):
        await self._db.execute(
            "DELETE FROM index_recommendations WHERE db_id = ? AND dismissed = 0",
            (db_id,),
        )
        ts = time.time()
        await self._db.executemany(
            """INSERT INTO index_recommendations
            (db_id, analyzed_at, schema_name, table_name, index_name,
             category, risk_level, reason, drop_ddl, rollback_ddl,
             estimated_size_savings, related_indexes)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [
                (
                    db_id, ts,
                    r["schema_name"], r["table_name"], r["index_name"],
                    r["category"], r["risk_level"], r["reason"],
                    r.get("drop_ddl", ""), r.get("rollback_ddl", ""),
                    r.get("estimated_size_savings", 0),
                    r.get("related_indexes", ""),
                )
                for r in recs
            ],
        )
        await self._db.commit()

    async def get_index_recommendations(
        self, db_id: str, category: str = "", include_dismissed: bool = False
    ) -> List[Dict[str, Any]]:
        query = "SELECT * FROM index_recommendations WHERE db_id = ?"
        params: list = [db_id]
        if not include_dismissed:
            query += " AND dismissed = 0"
        if category:
            query += " AND category = ?"
            params.append(category)
        query += " ORDER BY analyzed_at DESC, estimated_size_savings DESC"
        cursor = await self._db.execute(query, params)
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]

    async def dismiss_index_recommendation(self, rec_id: int):
        await self._db.execute(
            "UPDATE index_recommendations SET dismissed = 1 WHERE id = ?", (rec_id,)
        )
        await self._db.commit()

    # --- Missing Index Recommendations ---

    async def save_missing_index_recommendations(self, db_id: str, recs: List[Dict[str, Any]]):
        await self._db.execute(
            "DELETE FROM missing_index_recommendations WHERE db_id = ?", (db_id,)
        )
        ts = time.time()
        await self._db.executemany(
            """INSERT INTO missing_index_recommendations
            (db_id, analyzed_at, schema_name, table_name, suggested_columns,
             create_ddl, reason, source, estimated_benefit,
             related_fingerprints, seq_scan_count, seq_tup_read)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [
                (
                    db_id, ts,
                    r["schema_name"], r["table_name"], r["suggested_columns"],
                    r["create_ddl"], r["reason"], r["source"],
                    r.get("estimated_benefit", 0),
                    r.get("related_fingerprints", ""),
                    r.get("seq_scan_count", 0), r.get("seq_tup_read", 0),
                )
                for r in recs
            ],
        )
        await self._db.commit()

    async def get_missing_index_recommendations(self, db_id: str) -> List[Dict[str, Any]]:
        cursor = await self._db.execute(
            """SELECT * FROM missing_index_recommendations
            WHERE db_id = ? ORDER BY estimated_benefit DESC""",
            (db_id,),
        )
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]

    # --- Slow Query Trends ---

    async def get_trend_hour_exists(self, db_id: str, hour_bucket: float) -> bool:
        cursor = await self._db.execute(
            "SELECT 1 FROM slow_query_trends WHERE db_id = ? AND hour_bucket = ? LIMIT 1",
            (db_id, hour_bucket),
        )
        return await cursor.fetchone() is not None

    async def insert_slow_query_trends(self, rows: List[Dict[str, Any]]):
        await self._db.executemany(
            """INSERT INTO slow_query_trends
            (db_id, hour_bucket, fingerprint, query_pattern,
             occurrence_count, total_duration, avg_duration, max_duration,
             distinct_users, top_users, top_clients)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [
                (
                    r["db_id"], r["hour_bucket"], r["fingerprint"], r["query_pattern"],
                    r["occurrence_count"], r["total_duration"],
                    r["avg_duration"], r["max_duration"],
                    r["distinct_users"], r.get("top_users", "[]"),
                    r.get("top_clients", "[]"),
                )
                for r in rows
            ],
        )
        await self._db.commit()

    async def get_slow_query_trends(
        self, db_id: str, start_time: float, end_time: float,
        fingerprint: str = "", group_by: str = "fingerprint"
    ) -> List[Dict[str, Any]]:
        query = """SELECT * FROM slow_query_trends
                   WHERE db_id = ? AND hour_bucket >= ? AND hour_bucket <= ?"""
        params: list = [db_id, start_time, end_time]
        if fingerprint:
            query += " AND fingerprint = ?"
            params.append(fingerprint)
        query += " ORDER BY hour_bucket ASC"
        cursor = await self._db.execute(query, params)
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]

    async def get_top_query_patterns(
        self, db_id: str, start_time: float, end_time: float, limit: int = 20
    ) -> List[Dict[str, Any]]:
        cursor = await self._db.execute(
            """SELECT fingerprint, query_pattern,
                      SUM(occurrence_count) AS total_occurrences,
                      SUM(total_duration) AS total_time,
                      MAX(max_duration) AS peak_duration,
                      AVG(avg_duration) AS mean_duration
            FROM slow_query_trends
            WHERE db_id = ? AND hour_bucket >= ? AND hour_bucket <= ?
            GROUP BY fingerprint
            ORDER BY total_time DESC
            LIMIT ?""",
            (db_id, start_time, end_time, limit),
        )
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]

    # --- Health Scores ---

    async def insert_health_score(
        self, db_id: str, overall: float, dimensions: Dict, anomalies: List
    ):
        import json
        await self._db.execute(
            """INSERT INTO health_scores
            (db_id, timestamp, overall_score, dimension_scores, anomalies)
            VALUES (?, ?, ?, ?, ?)""",
            (db_id, time.time(), overall, json.dumps(dimensions), json.dumps(anomalies)),
        )
        await self._db.commit()

    async def get_latest_health_score(self, db_id: str) -> Optional[Dict[str, Any]]:
        cursor = await self._db.execute(
            """SELECT * FROM health_scores
            WHERE db_id = ? ORDER BY timestamp DESC LIMIT 1""",
            (db_id,),
        )
        row = await cursor.fetchone()
        return dict(row) if row else None

    async def get_health_history(
        self, db_id: str, start_time: float, end_time: Optional[float] = None
    ) -> List[Dict[str, Any]]:
        end_time = end_time or time.time()
        cursor = await self._db.execute(
            """SELECT timestamp, overall_score, dimension_scores, anomalies
            FROM health_scores
            WHERE db_id = ? AND timestamp >= ? AND timestamp <= ?
            ORDER BY timestamp ASC""",
            (db_id, start_time, end_time),
        )
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]

    # --- Alerts ---

    async def insert_alert(
        self, db_id: str, dimension: str, severity: str,
        message: str, value: float, threshold: float
    ):
        await self._db.execute(
            """INSERT INTO alert_history
            (db_id, triggered_at, dimension, severity, message, value, threshold)
            VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (db_id, time.time(), dimension, severity, message, value, threshold),
        )
        await self._db.commit()

    async def get_alerts(
        self, db_id: str, severity: str = "", acknowledged: Optional[bool] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        query = "SELECT * FROM alert_history WHERE db_id = ?"
        params: list = [db_id]
        if severity:
            query += " AND severity = ?"
            params.append(severity)
        if acknowledged is not None:
            query += " AND acknowledged = ?"
            params.append(int(acknowledged))
        query += " ORDER BY triggered_at DESC LIMIT ?"
        params.append(limit)
        cursor = await self._db.execute(query, params)
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]

    async def acknowledge_alert(self, alert_id: int):
        await self._db.execute(
            "UPDATE alert_history SET acknowledged = 1 WHERE id = ?", (alert_id,)
        )
        await self._db.commit()

    async def count_alerts_since(self, db_id: str, since: float) -> int:
        cursor = await self._db.execute(
            "SELECT COUNT(*) as cnt FROM alert_history WHERE db_id = ? AND triggered_at >= ?",
            (db_id, since),
        )
        row = await cursor.fetchone()
        return row["cnt"] if row else 0
