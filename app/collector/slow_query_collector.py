import hashlib
import logging
import re
import time
from typing import Any, Dict, Set, Tuple

from app.database.connection_manager import DatabaseConnection
from app.database.sqlite_store import SQLiteStore

logger = logging.getLogger(__name__)

LITERAL_PATTERN = re.compile(
    r"""'[^']*'|"[^"]*"|\b\d+\.?\d*\b""", re.IGNORECASE
)


def normalize_query(query_text: str) -> str:
    normalized = LITERAL_PATTERN.sub("?", query_text)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized


def fingerprint_query(query_text: str) -> str:
    normalized = normalize_query(query_text)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:16]


class SlowQueryCollector:
    def __init__(
        self,
        connection: DatabaseConnection,
        store: SQLiteStore,
        threshold: float = 1.0,
    ):
        self.conn = connection
        self.store = store
        self.threshold = threshold
        self._seen_queries: Set[Tuple[int, float]] = set()

    async def collect(self):
        query = """
            SELECT
                pid,
                datname,
                usename,
                client_addr::text AS client_addr,
                state,
                query,
                extract(epoch from query_start) AS query_start_epoch,
                extract(epoch from (now() - query_start)) AS duration_seconds,
                wait_event_type,
                wait_event
            FROM pg_stat_activity
            WHERE state = 'active'
              AND query NOT ILIKE '%pg_stat_activity%'
              AND now() - query_start > make_interval(secs => $1)
              AND pid != pg_backend_pid()
            ORDER BY query_start ASC
        """
        try:
            rows = await self.conn.execute_query(query, self.threshold)
        except Exception as e:
            if "make_interval" in str(e):
                rows = await self._collect_legacy()
            else:
                logger.warning(f"[{self.conn.db_id}] Slow query sampling error: {e}")
                return

        if not rows:
            return

        current_keys: Set[Tuple[int, float]] = set()

        for row in rows:
            pid = row["pid"]
            query_start = row.get("query_start_epoch") or 0
            key = (pid, query_start)
            current_keys.add(key)

            if key in self._seen_queries:
                continue

            self._seen_queries.add(key)
            query_text = row.get("query") or ""
            fp = fingerprint_query(query_text)

            await self.store.insert_slow_query(
                self.conn.db_id,
                {
                    "fingerprint": fp,
                    "query_text": query_text[:4096],
                    "username": row.get("usename"),
                    "client_addr": row.get("client_addr"),
                    "duration_seconds": row.get("duration_seconds", 0),
                    "query_start": query_start,
                    "wait_event_type": row.get("wait_event_type"),
                    "wait_event": row.get("wait_event"),
                    "pid": pid,
                },
            )

        stale = self._seen_queries - current_keys
        if len(self._seen_queries) > 10000:
            self._seen_queries = current_keys

    async def _collect_legacy(self):
        query = """
            SELECT
                pid,
                datname,
                usename,
                client_addr::text AS client_addr,
                state,
                query,
                extract(epoch from query_start) AS query_start_epoch,
                extract(epoch from (now() - query_start)) AS duration_seconds,
                wait_event_type,
                wait_event
            FROM pg_stat_activity
            WHERE state = 'active'
              AND query NOT ILIKE '%pg_stat_activity%'
              AND extract(epoch from (now() - query_start)) > $1
              AND pid != pg_backend_pid()
            ORDER BY query_start ASC
        """
        try:
            return await self.conn.execute_query(query, self.threshold)
        except Exception as e:
            logger.warning(f"[{self.conn.db_id}] Legacy slow query sampling error: {e}")
            return []
