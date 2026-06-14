import hashlib
import logging
import re
import time
from typing import Any, Dict, List, Optional

from app.database.connection_manager import DatabaseConnection
from app.database.sqlite_store import SQLiteStore

logger = logging.getLogger(__name__)


class MetricsCollector:
    def __init__(self, connection: DatabaseConnection, store: SQLiteStore):
        self.conn = connection
        self.store = store
        self._prev_xacts: Optional[float] = None
        self._prev_time: Optional[float] = None

    async def collect(self) -> Dict[str, Any]:
        metrics: Dict[str, Any] = {}

        conn_data = await self._collect_connections()
        metrics.update(conn_data)

        cache_data = await self._collect_cache_hit()
        metrics.update(cache_data)

        tps_data = await self._collect_tps()
        metrics.update(tps_data)

        size_data = await self._collect_db_size()
        metrics.update(size_data)

        await self.store.insert_metrics(self.conn.db_id, metrics)
        return metrics

    async def collect_top_tables(self, limit: int = 20) -> List[Dict[str, Any]]:
        query = """
            SELECT
                schemaname AS schema_name,
                relname AS table_name,
                pg_total_relation_size(quote_ident(schemaname) || '.' || quote_ident(relname)) AS total_size,
                pg_relation_size(quote_ident(schemaname) || '.' || quote_ident(relname)) AS table_size,
                pg_indexes_size(quote_ident(schemaname) || '.' || quote_ident(relname)) AS indexes_size
            FROM pg_stat_user_tables
            ORDER BY pg_total_relation_size(quote_ident(schemaname) || '.' || quote_ident(relname)) DESC
            LIMIT $1
        """
        try:
            rows = await self.conn.execute_query(query, limit)
            tables = [
                {
                    "schema_name": r["schema_name"],
                    "table_name": r["table_name"],
                    "total_size": r["total_size"],
                    "table_size": r["table_size"],
                    "indexes_size": r["indexes_size"],
                }
                for r in rows
            ]
            await self.store.insert_top_tables(self.conn.db_id, tables)
            return tables
        except Exception as e:
            logger.warning(f"[{self.conn.db_id}] Failed to collect top tables: {e}")
            return []

    async def _collect_connections(self) -> Dict[str, Any]:
        query = """
            SELECT
                count(*) FILTER (WHERE state = 'active') AS active,
                count(*) FILTER (WHERE state = 'idle') AS idle,
                count(*) FILTER (WHERE state = 'idle in transaction') AS idle_in_transaction,
                count(*) AS total
            FROM pg_stat_activity
            WHERE backend_type = 'client backend'
        """
        try:
            rows = await self.conn.execute_query(query)
            if rows:
                r = rows[0]
                return {
                    "active_connections": r["active"] or 0,
                    "idle_connections": r["idle"] or 0,
                    "idle_in_transaction": r.get("idle_in_transaction", 0) or 0,
                    "total_connections": r["total"] or 0,
                }
        except Exception as e:
            if "backend_type" in str(e):
                return await self._collect_connections_legacy()
            logger.warning(f"[{self.conn.db_id}] Connection stats error: {e}")
        return {"active_connections": 0, "idle_connections": 0, "total_connections": 0}

    async def _collect_connections_legacy(self) -> Dict[str, Any]:
        query = """
            SELECT
                count(*) FILTER (WHERE state = 'active') AS active,
                count(*) FILTER (WHERE state = 'idle') AS idle,
                count(*) AS total
            FROM pg_stat_activity
            WHERE pid != pg_backend_pid()
        """
        try:
            rows = await self.conn.execute_query(query)
            if rows:
                r = rows[0]
                return {
                    "active_connections": r["active"] or 0,
                    "idle_connections": r["idle"] or 0,
                    "total_connections": r["total"] or 0,
                }
        except Exception as e:
            logger.warning(f"[{self.conn.db_id}] Legacy connection stats error: {e}")
        return {"active_connections": 0, "idle_connections": 0, "total_connections": 0}

    async def _collect_cache_hit(self) -> Dict[str, Any]:
        query = """
            SELECT
                sum(blks_hit) AS blks_hit,
                sum(blks_read) AS blks_read,
                CASE WHEN sum(blks_hit) + sum(blks_read) = 0 THEN 0
                     ELSE round(sum(blks_hit)::numeric / (sum(blks_hit) + sum(blks_read)) * 100, 2)
                END AS cache_hit_ratio
            FROM pg_stat_database
            WHERE datname = current_database()
        """
        try:
            rows = await self.conn.execute_query(query)
            if rows and rows[0]["cache_hit_ratio"] is not None:
                return {"cache_hit_ratio": float(rows[0]["cache_hit_ratio"])}
        except Exception as e:
            logger.warning(f"[{self.conn.db_id}] Cache hit error: {e}")
        return {"cache_hit_ratio": 0.0}

    async def _collect_tps(self) -> Dict[str, Any]:
        query = """
            SELECT
                xact_commit + xact_rollback AS total_xacts
            FROM pg_stat_database
            WHERE datname = current_database()
        """
        try:
            rows = await self.conn.execute_query(query)
            if rows:
                current_xacts = float(rows[0]["total_xacts"] or 0)
                current_time = time.time()

                if self._prev_xacts is not None and self._prev_time is not None:
                    dt = current_time - self._prev_time
                    if dt > 0:
                        tps = (current_xacts - self._prev_xacts) / dt
                        self._prev_xacts = current_xacts
                        self._prev_time = current_time
                        return {"tps": round(max(tps, 0), 2)}

                self._prev_xacts = current_xacts
                self._prev_time = current_time
        except Exception as e:
            logger.warning(f"[{self.conn.db_id}] TPS error: {e}")
        return {"tps": 0.0}

    async def _collect_db_size(self) -> Dict[str, Any]:
        query = "SELECT pg_database_size(current_database()) AS db_size_bytes"
        try:
            rows = await self.conn.execute_query(query)
            if rows:
                return {"db_size_bytes": rows[0]["db_size_bytes"] or 0}
        except Exception as e:
            logger.warning(f"[{self.conn.db_id}] DB size error: {e}")
        return {"db_size_bytes": 0}
