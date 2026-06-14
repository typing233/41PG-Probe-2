import hashlib
import json
import logging
import re
from typing import Any, Dict, List, Set, Tuple

from app.database.connection_manager import DatabaseConnection
from app.database.sqlite_store import SQLiteStore

logger = logging.getLogger(__name__)


class MissingIndexAnalyzer:
    def __init__(self, store: SQLiteStore, config):
        self.store = store
        self.min_seq_scan = getattr(config, 'min_seq_scan_count', 100)
        self.min_seq_tup_read = getattr(config, 'min_seq_tup_read', 10000)
        self.top_queries_limit = getattr(config, 'top_queries_limit', 50)

    async def analyze(self, db_id: str, conn: DatabaseConnection) -> List[Dict[str, Any]]:
        recommendations = []

        seq_recs = await self._analyze_seq_scans(conn)
        recommendations.extend(seq_recs)

        if conn.capabilities.get("pg_stat_statements"):
            stmt_recs = await self._analyze_statements(conn)
            recommendations.extend(stmt_recs)

        deduped = self._deduplicate(recommendations)

        if deduped:
            await self.store.save_missing_index_recommendations(db_id, deduped)
        logger.info(f"[{db_id}] Missing index analysis: {len(deduped)} recommendations")
        return deduped

    async def _analyze_seq_scans(self, conn: DatabaseConnection) -> List[Dict[str, Any]]:
        query = """
            SELECT
                schemaname AS schema_name,
                relname AS table_name,
                seq_scan,
                seq_tup_read,
                idx_scan,
                n_live_tup
            FROM pg_stat_user_tables
            WHERE seq_scan > $1
              AND seq_tup_read > $2
              AND (idx_scan = 0 OR seq_scan::float / GREATEST(idx_scan, 1) > 10)
              AND n_live_tup > 500
            ORDER BY seq_tup_read DESC
            LIMIT 30
        """
        try:
            rows = await conn.execute_query(
                query, self.min_seq_scan, self.min_seq_tup_read, timeout=15.0
            )
        except Exception as e:
            logger.warning(f"[{conn.db_id}] Seq scan analysis error: {e}")
            return []

        results = []
        for r in rows:
            n_live = max(r.get("n_live_tup", 1), 1)
            seq_scan = r.get("seq_scan", 0)
            idx_scan = r.get("idx_scan", 0)
            seq_tup_read = r.get("seq_tup_read", 0)

            benefit = min(
                (seq_tup_read / n_live) * (seq_scan / max(seq_scan + idx_scan, 1)) * 100,
                100.0,
            )

            schema = r["schema_name"]
            table = r["table_name"]
            columns = await self._guess_columns_from_table(conn, schema, table)
            col_str = ", ".join(columns) if columns else "/* 需要根据查询条件确定列 */"

            idx_name = f"idx_{table}_{'_'.join(columns[:3])}_{hashlib.md5(col_str.encode()).hexdigest()[:6]}"
            create_ddl = f"CREATE INDEX CONCURRENTLY {idx_name} ON {schema}.{table} ({col_str});"

            results.append({
                "schema_name": schema,
                "table_name": table,
                "suggested_columns": json.dumps(columns),
                "create_ddl": create_ddl,
                "reason": f"顺序扫描 {seq_scan} 次，读取 {seq_tup_read} 行（表行数 {n_live}），索引扫描仅 {idx_scan} 次",
                "source": "seq_scan",
                "estimated_benefit": round(benefit, 1),
                "related_fingerprints": "[]",
                "seq_scan_count": seq_scan,
                "seq_tup_read": seq_tup_read,
            })

        return results

    async def _analyze_statements(self, conn: DatabaseConnection) -> List[Dict[str, Any]]:
        if conn.capabilities.get("pg_stat_statements_v1_8"):
            query = """
                SELECT
                    queryid,
                    query,
                    calls,
                    total_exec_time AS total_time,
                    mean_exec_time AS mean_time,
                    rows
                FROM pg_stat_statements
                WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
                  AND calls > 10
                  AND mean_exec_time > 100
                ORDER BY total_exec_time DESC
                LIMIT $1
            """
        else:
            query = """
                SELECT
                    queryid,
                    query,
                    calls,
                    total_time,
                    mean_time,
                    rows
                FROM pg_stat_statements
                WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
                  AND calls > 10
                  AND mean_time > 100
                ORDER BY total_time DESC
                LIMIT $1
            """

        try:
            rows = await conn.execute_query(query, self.top_queries_limit, timeout=15.0)
        except Exception as e:
            logger.warning(f"[{conn.db_id}] pg_stat_statements analysis error: {e}")
            return []

        results = []
        for r in rows:
            query_text = r.get("query", "")
            if not query_text:
                continue

            table_info = self._extract_table_from_query(query_text)
            if not table_info:
                continue

            schema, table = table_info
            where_cols = self._extract_where_columns(query_text)
            if not where_cols:
                continue

            total_time = r.get("total_time", 0)
            calls = max(r.get("calls", 1), 1)
            benefit = min((total_time / 1000) / calls * 10, 100.0)

            col_str = ", ".join(where_cols)
            idx_name = f"idx_{table}_{'_'.join(where_cols[:3])}_{hashlib.md5(col_str.encode()).hexdigest()[:6]}"
            create_ddl = f"CREATE INDEX CONCURRENTLY {idx_name} ON {schema}.{table} ({col_str});"

            results.append({
                "schema_name": schema,
                "table_name": table,
                "suggested_columns": json.dumps(where_cols),
                "create_ddl": create_ddl,
                "reason": f"高频慢查询（调用 {calls} 次，总耗时 {total_time/1000:.1f}s，均值 {r.get('mean_time', 0):.1f}ms）",
                "source": "pg_stat_statements",
                "estimated_benefit": round(benefit, 1),
                "related_fingerprints": json.dumps([str(r.get("queryid", ""))]),
                "seq_scan_count": 0,
                "seq_tup_read": 0,
            })

        return results

    async def _guess_columns_from_table(
        self, conn: DatabaseConnection, schema: str, table: str
    ) -> List[str]:
        query = """
            SELECT a.attname
            FROM pg_attribute a
            JOIN pg_class c ON c.oid = a.attrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            JOIN pg_stats s ON s.schemaname = n.nspname
                AND s.tablename = c.relname
                AND s.attname = a.attname
            WHERE n.nspname = $1
              AND c.relname = $2
              AND a.attnum > 0
              AND NOT a.attisdropped
              AND s.n_distinct > 10
            ORDER BY s.n_distinct DESC
            LIMIT 3
        """
        try:
            rows = await conn.execute_query(query, schema, table, timeout=5.0)
            return [r["attname"] for r in rows]
        except Exception:
            return []

    def _extract_table_from_query(self, query: str) -> Tuple[str, str]:
        m = re.search(
            r"FROM\s+(?:ONLY\s+)?([\"']?\w+[\"']?)\.([\"']?\w+[\"']?)",
            query, re.IGNORECASE,
        )
        if m:
            return m.group(1).strip("\"'"), m.group(2).strip("\"'")
        m = re.search(r"FROM\s+(?:ONLY\s+)?([\"']?\w+[\"']?)", query, re.IGNORECASE)
        if m:
            return "public", m.group(1).strip("\"'")
        return None

    def _extract_where_columns(self, query: str) -> List[str]:
        m = re.search(r"WHERE\s+(.+?)(?:ORDER|GROUP|LIMIT|HAVING|$)", query, re.IGNORECASE | re.DOTALL)
        if not m:
            return []
        where_clause = m.group(1)
        cols = re.findall(r"(\w+)\s*(?:=|>|<|>=|<=|<>|!=|LIKE|IN|BETWEEN|IS)", where_clause, re.IGNORECASE)
        seen: Set[str] = set()
        result = []
        skip = {"AND", "OR", "NOT", "NULL", "TRUE", "FALSE", "SELECT"}
        for c in cols:
            cu = c.upper()
            if cu not in skip and c.lower() not in seen:
                seen.add(c.lower())
                result.append(c.lower())
        return result[:4]

    def _deduplicate(self, recs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        seen: Dict[str, Dict] = {}
        for r in recs:
            key = f"{r['schema_name']}.{r['table_name']}:{r.get('suggested_columns', '')}"
            if key in seen:
                existing = seen[key]
                if r.get("estimated_benefit", 0) > existing.get("estimated_benefit", 0):
                    seen[key] = r
            else:
                seen[key] = r
        return sorted(seen.values(), key=lambda x: x.get("estimated_benefit", 0), reverse=True)
