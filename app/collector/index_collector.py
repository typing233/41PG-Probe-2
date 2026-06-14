import logging
from typing import Any, Dict, List

from app.database.connection_manager import DatabaseConnection
from app.database.sqlite_store import SQLiteStore

logger = logging.getLogger(__name__)


class IndexCollector:
    def __init__(self, connection: DatabaseConnection, store: SQLiteStore):
        self.conn = connection
        self.store = store

    async def collect(self) -> List[Dict[str, Any]]:
        query = """
            SELECT
                n.nspname AS schema_name,
                t.relname AS table_name,
                i.relname AS index_name,
                pg_relation_size(i.oid) AS index_size_bytes,
                COALESCE(psi.idx_scan, 0) AS idx_scan,
                COALESCE(psi.idx_tup_read, 0) AS idx_tup_read,
                COALESCE(psi.idx_tup_fetch, 0) AS idx_tup_fetch,
                ix.indisunique AS is_unique,
                ix.indisprimary AS is_primary,
                pg_get_indexdef(ix.indexrelid) AS index_def,
                COALESCE(pst.n_dead_tup, 0) AS n_dead_tup,
                COALESCE(pst.n_tup_ins, 0) AS n_tup_ins,
                COALESCE(pst.n_tup_upd, 0) AS n_tup_upd,
                COALESCE(pst.n_tup_del, 0) AS n_tup_del
            FROM pg_index ix
            JOIN pg_class i ON i.oid = ix.indexrelid
            JOIN pg_class t ON t.oid = ix.indrelid
            JOIN pg_namespace n ON n.oid = t.relnamespace
            LEFT JOIN pg_stat_user_indexes psi ON psi.indexrelid = ix.indexrelid
            LEFT JOIN pg_stat_user_tables pst ON pst.relid = ix.indrelid
            WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
              AND i.relkind = 'i'
            ORDER BY pg_relation_size(i.oid) DESC
        """
        try:
            rows = await self.conn.execute_query(query, timeout=30.0)
        except Exception as e:
            if "permission" in str(e).lower() or "pg_relation_size" in str(e):
                rows = await self._collect_fallback()
            else:
                logger.warning(f"[{self.conn.db_id}] Index collection error: {e}")
                return []

        stats = [
            {
                "schema_name": r["schema_name"],
                "table_name": r["table_name"],
                "index_name": r["index_name"],
                "index_size_bytes": r.get("index_size_bytes") or 0,
                "idx_scan": r.get("idx_scan") or 0,
                "idx_tup_read": r.get("idx_tup_read") or 0,
                "idx_tup_fetch": r.get("idx_tup_fetch") or 0,
                "is_unique": bool(r.get("is_unique")),
                "is_primary": bool(r.get("is_primary")),
                "index_def": r.get("index_def") or "",
                "n_dead_tup": r.get("n_dead_tup") or 0,
                "n_tup_ins": r.get("n_tup_ins") or 0,
                "n_tup_upd": r.get("n_tup_upd") or 0,
                "n_tup_del": r.get("n_tup_del") or 0,
            }
            for r in rows
        ]

        if stats:
            await self.store.insert_index_stats(self.conn.db_id, stats)
        return stats

    async def _collect_fallback(self) -> List[Dict[str, Any]]:
        query = """
            SELECT
                n.nspname AS schema_name,
                t.relname AS table_name,
                i.relname AS index_name,
                i.relpages * 8192::bigint AS index_size_bytes,
                COALESCE(psi.idx_scan, 0) AS idx_scan,
                COALESCE(psi.idx_tup_read, 0) AS idx_tup_read,
                COALESCE(psi.idx_tup_fetch, 0) AS idx_tup_fetch,
                ix.indisunique AS is_unique,
                ix.indisprimary AS is_primary,
                pg_get_indexdef(ix.indexrelid) AS index_def,
                COALESCE(pst.n_dead_tup, 0) AS n_dead_tup,
                COALESCE(pst.n_tup_ins, 0) AS n_tup_ins,
                COALESCE(pst.n_tup_upd, 0) AS n_tup_upd,
                COALESCE(pst.n_tup_del, 0) AS n_tup_del
            FROM pg_index ix
            JOIN pg_class i ON i.oid = ix.indexrelid
            JOIN pg_class t ON t.oid = ix.indrelid
            JOIN pg_namespace n ON n.oid = t.relnamespace
            LEFT JOIN pg_stat_user_indexes psi ON psi.indexrelid = ix.indexrelid
            LEFT JOIN pg_stat_user_tables pst ON pst.relid = ix.indrelid
            WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
              AND i.relkind = 'i'
            ORDER BY i.relpages DESC
        """
        try:
            return await self.conn.execute_query(query, timeout=30.0)
        except Exception as e:
            logger.warning(f"[{self.conn.db_id}] Index fallback collection error: {e}")
            return []
