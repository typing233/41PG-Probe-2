import hashlib
import json
import logging
import re
from collections import defaultdict
from typing import Any, Dict, List, Optional, Set, Tuple

from app.database.connection_manager import DatabaseConnection
from app.database.sqlite_store import SQLiteStore

logger = logging.getLogger(__name__)

# SQL keywords that appear as identifiers in WHERE but aren't columns
_SQL_KEYWORDS = frozenset({
    "AND", "OR", "NOT", "NULL", "TRUE", "FALSE", "SELECT", "FROM",
    "WHERE", "IN", "IS", "BETWEEN", "LIKE", "ILIKE", "EXISTS", "ANY",
    "ALL", "CASE", "WHEN", "THEN", "ELSE", "END", "AS", "CAST",
    "INTERVAL", "CURRENT_TIMESTAMP", "NOW", "COALESCE", "NULLIF",
})


class MissingIndexAnalyzer:
    def __init__(self, store: SQLiteStore, config):
        self.store = store
        self.min_seq_scan = getattr(config, 'min_seq_scan_count', 100)
        self.min_seq_tup_read = getattr(config, 'min_seq_tup_read', 10000)
        self.top_queries_limit = getattr(config, 'top_queries_limit', 50)

    async def analyze(self, db_id: str, conn: DatabaseConnection) -> List[Dict[str, Any]]:
        recommendations = []

        # Source 1: Tables with heavy sequential scans — combine with column stats
        seq_recs = await self._analyze_seq_scans(conn)
        recommendations.extend(seq_recs)

        # Source 2: High-cost queries from pg_stat_statements with column extraction
        if conn.capabilities.get("pg_stat_statements"):
            stmt_recs = await self._analyze_statements(conn)
            recommendations.extend(stmt_recs)

        # Source 3: Correlate with local slow query fingerprints
        fp_recs = await self._analyze_slow_query_fingerprints(db_id, conn)
        recommendations.extend(fp_recs)

        deduped = self._deduplicate(recommendations)

        if deduped:
            await self.store.save_missing_index_recommendations(db_id, deduped)
        logger.info(f"[{db_id}] Missing index analysis: {len(deduped)} recommendations")
        return deduped

    async def _analyze_seq_scans(self, conn: DatabaseConnection) -> List[Dict[str, Any]]:
        """Find tables with disproportionate seq scans, then identify likely
        filter columns using pg_stats selectivity data."""
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
            schema = r["schema_name"]
            table = r["table_name"]
            n_live = max(r.get("n_live_tup", 1), 1)
            seq_scan = r.get("seq_scan", 0)
            idx_scan = r.get("idx_scan", 0)
            seq_tup_read = r.get("seq_tup_read", 0)

            # Get high-selectivity columns that are likely filter targets
            columns = await self._get_high_selectivity_columns(conn, schema, table)
            if not columns:
                continue  # Skip if we can't determine useful columns

            # Estimate benefit: ratio of sequential reads to table size × scan dominance
            scan_ratio = seq_scan / max(seq_scan + idx_scan, 1)
            read_amplification = min(seq_tup_read / n_live, 50)
            benefit = min(scan_ratio * read_amplification * 10, 100.0)

            # Only recommend if benefit is meaningful
            if benefit < 5.0:
                continue

            col_str = ", ".join(columns)
            idx_name = self._generate_index_name(table, columns)
            create_ddl = f"CREATE INDEX CONCURRENTLY {idx_name} ON {schema}.{table} ({col_str});"

            results.append({
                "schema_name": schema,
                "table_name": table,
                "suggested_columns": json.dumps(columns),
                "create_ddl": create_ddl,
                "reason": (
                    f"表 {schema}.{table} 顺序扫描 {seq_scan:,} 次（索引扫描 {idx_scan:,} 次），"
                    f"累计读取 {seq_tup_read:,} 行（表共 {n_live:,} 行）。"
                    f"推荐列基于列选择性分析（n_distinct）"
                ),
                "source": "seq_scan",
                "estimated_benefit": round(benefit, 1),
                "related_fingerprints": "[]",
                "seq_scan_count": seq_scan,
                "seq_tup_read": seq_tup_read,
            })

        return results

    async def _analyze_statements(self, conn: DatabaseConnection) -> List[Dict[str, Any]]:
        """Analyze pg_stat_statements for high-cost queries that lack index support.
        Extract WHERE/JOIN columns from the query text and verify the table exists."""
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
                  AND query !~ '^(COPY|SET|SHOW|COMMIT|ROLLBACK|BEGIN)'
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
                  AND query !~ '^(COPY|SET|SHOW|COMMIT|ROLLBACK|BEGIN)'
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

            # Extract table and WHERE columns
            table_info = self._extract_table_from_query(query_text)
            if not table_info:
                continue

            schema, table = table_info
            where_cols = self._extract_where_columns(query_text)
            if not where_cols:
                continue

            # Verify columns actually exist on the table
            valid_cols = await self._verify_columns_exist(conn, schema, table, where_cols)
            if not valid_cols:
                continue

            # Check if an index already covers these columns
            already_indexed = await self._check_existing_index_covers(
                conn, schema, table, valid_cols
            )
            if already_indexed:
                continue

            total_time = r.get("total_time", 0)
            calls = max(r.get("calls", 1), 1)
            mean_time = r.get("mean_time", 0)

            # Benefit estimate: total time saved if index cuts mean time by ~80%
            estimated_savings_pct = 0.8
            benefit = min((total_time * estimated_savings_pct / 1000) / max(calls, 1) * 5, 100.0)

            if benefit < 3.0:
                continue

            col_str = ", ".join(valid_cols)
            idx_name = self._generate_index_name(table, valid_cols)
            create_ddl = f"CREATE INDEX CONCURRENTLY {idx_name} ON {schema}.{table} ({col_str});"

            results.append({
                "schema_name": schema,
                "table_name": table,
                "suggested_columns": json.dumps(valid_cols),
                "create_ddl": create_ddl,
                "reason": (
                    f"高频查询（调用 {calls:,} 次，总耗时 {total_time/1000:.1f}s，"
                    f"均值 {mean_time:.1f}ms）。WHERE 条件涉及列 {col_str}，"
                    f"当前无覆盖索引"
                ),
                "source": "pg_stat_statements",
                "estimated_benefit": round(benefit, 1),
                "related_fingerprints": json.dumps([str(r.get("queryid", ""))]),
                "seq_scan_count": 0,
                "seq_tup_read": 0,
            })

        return results

    async def _analyze_slow_query_fingerprints(
        self, db_id: str, conn: DatabaseConnection
    ) -> List[Dict[str, Any]]:
        """Cross-reference local slow query fingerprints with table scan data."""
        queries = await self.store.get_slow_queries(db_id, window_seconds=86400, limit=500)
        if not queries:
            return []

        # Group by fingerprint, find most frequent patterns
        fp_groups: Dict[str, List[Dict]] = defaultdict(list)
        for q in queries:
            fp_groups[q["fingerprint"]].append(q)

        # Sort by frequency
        sorted_fps = sorted(fp_groups.items(), key=lambda x: len(x[1]), reverse=True)

        results = []
        seen_tables: Set[str] = set()

        for fp, items in sorted_fps[:20]:
            query_text = items[0].get("query_text", "")
            if not query_text:
                continue

            table_info = self._extract_table_from_query(query_text)
            if not table_info:
                continue

            schema, table = table_info
            table_key = f"{schema}.{table}"
            if table_key in seen_tables:
                continue

            where_cols = self._extract_where_columns(query_text)
            if not where_cols:
                continue

            valid_cols = await self._verify_columns_exist(conn, schema, table, where_cols)
            if not valid_cols:
                continue

            already_indexed = await self._check_existing_index_covers(
                conn, schema, table, valid_cols
            )
            if already_indexed:
                continue

            seen_tables.add(table_key)
            occurrence = len(items)
            avg_duration = sum(i["duration_seconds"] for i in items) / occurrence

            benefit = min(occurrence * avg_duration * 2, 100.0)
            if benefit < 5.0:
                continue

            col_str = ", ".join(valid_cols)
            idx_name = self._generate_index_name(table, valid_cols)
            create_ddl = f"CREATE INDEX CONCURRENTLY {idx_name} ON {schema}.{table} ({col_str});"

            related_fps = json.dumps([fp])
            results.append({
                "schema_name": schema,
                "table_name": table,
                "suggested_columns": json.dumps(valid_cols),
                "create_ddl": create_ddl,
                "reason": (
                    f"慢查询指纹 {fp[:12]}… 在 24h 内出现 {occurrence} 次"
                    f"（均值 {avg_duration:.2f}s），WHERE 涉及列 {col_str}"
                ),
                "source": "slow_query_fingerprint",
                "estimated_benefit": round(benefit, 1),
                "related_fingerprints": related_fps,
                "seq_scan_count": 0,
                "seq_tup_read": 0,
            })

        return results

    # ---- Helper methods ----

    async def _get_high_selectivity_columns(
        self, conn: DatabaseConnection, schema: str, table: str
    ) -> List[str]:
        """Get columns with high selectivity that are good index candidates.
        Uses pg_stats n_distinct: positive = exact count, negative = fraction of rows.
        Prefers columns frequently used in WHERE (high absolute n_distinct)."""
        query = """
            SELECT s.attname, s.n_distinct, s.null_frac
            FROM pg_stats s
            WHERE s.schemaname = $1
              AND s.tablename = $2
              AND s.n_distinct != 0
              AND s.n_distinct != 1
              AND s.null_frac < 0.5
            ORDER BY
                CASE WHEN s.n_distinct < 0 THEN -s.n_distinct
                     ELSE s.n_distinct END DESC
            LIMIT 3
        """
        try:
            rows = await conn.execute_query(query, schema, table, timeout=5.0)
            if not rows:
                return []
            # Only return columns with reasonable selectivity
            # n_distinct > 10 or n_distinct < -0.01 (more than 1% unique)
            result = []
            for r in rows:
                nd = r["n_distinct"]
                if nd > 10 or nd < -0.01:
                    result.append(r["attname"])
            return result
        except Exception:
            return []

    async def _verify_columns_exist(
        self, conn: DatabaseConnection, schema: str, table: str, columns: List[str]
    ) -> List[str]:
        """Verify that the given column names actually exist on the table."""
        query = """
            SELECT a.attname
            FROM pg_attribute a
            JOIN pg_class c ON c.oid = a.attrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = $1
              AND c.relname = $2
              AND a.attnum > 0
              AND NOT a.attisdropped
              AND a.attname = ANY($3)
        """
        try:
            rows = await conn.execute_query(query, schema, table, columns, timeout=5.0)
            existing = {r["attname"] for r in rows}
            # Preserve order of input columns, filter to existing only
            return [c for c in columns if c in existing]
        except Exception:
            return []

    async def _check_existing_index_covers(
        self, conn: DatabaseConnection, schema: str, table: str, columns: List[str]
    ) -> bool:
        """Check if any existing index already covers the given columns as a prefix."""
        query = """
            SELECT pg_get_indexdef(ix.indexrelid) AS index_def
            FROM pg_index ix
            JOIN pg_class t ON t.oid = ix.indrelid
            JOIN pg_namespace n ON n.oid = t.relnamespace
            WHERE n.nspname = $1 AND t.relname = $2
        """
        try:
            rows = await conn.execute_query(query, schema, table, timeout=5.0)
        except Exception:
            return False

        target_cols = [c.lower() for c in columns]

        for r in rows:
            idx_def = r.get("index_def", "")
            if not idx_def:
                continue
            # Parse the index columns
            from app.collector.index_analyzer import parse_index_columns, normalize_col
            parsed = parse_index_columns(idx_def)
            if parsed is None:
                continue
            idx_cols, _, _, _ = parsed
            norm_idx_cols = [normalize_col(c) for c in idx_cols]

            # Check if target columns are a prefix of or equal to existing index
            if len(target_cols) <= len(norm_idx_cols):
                if all(t == i for t, i in zip(target_cols, norm_idx_cols)):
                    return True

        return False

    def _extract_table_from_query(self, query: str) -> Optional[Tuple[str, str]]:
        """Extract the primary table from a SQL query."""
        # schema.table format
        m = re.search(
            r"(?:FROM|JOIN|UPDATE|INTO)\s+(?:ONLY\s+)?\"?(\w+)\"?\.\"?(\w+)\"?",
            query, re.IGNORECASE,
        )
        if m:
            return m.group(1), m.group(2)
        # Unqualified table (assume public schema)
        m = re.search(
            r"(?:FROM|JOIN|UPDATE|INTO)\s+(?:ONLY\s+)?\"?(\w+)\"?(?:\s|$|,|\()",
            query, re.IGNORECASE,
        )
        if m:
            name = m.group(1)
            # Exclude SQL keywords that might look like table names
            if name.upper() in _SQL_KEYWORDS:
                return None
            return "public", name
        return None

    def _extract_where_columns(self, query: str) -> List[str]:
        """Extract column names from WHERE clause conditions."""
        # Find WHERE clause (stop at ORDER BY, GROUP BY, LIMIT, HAVING, or subquery)
        m = re.search(
            r"WHERE\s+(.+?)(?:\s+(?:ORDER|GROUP|LIMIT|HAVING|RETURNING|FOR\s+UPDATE)\b|$)",
            query, re.IGNORECASE | re.DOTALL,
        )
        if not m:
            return []

        where_clause = m.group(1)

        # Match patterns: column op value, column IN (...), column BETWEEN, column IS
        # Use word boundary and look for table.column or plain column patterns
        patterns = [
            r"(?:(\w+)\.)?(\w+)\s*(?:=|>|<|>=|<=|<>|!=)\s*(?:\$\d+|'[^']*'|\d+|\?)",
            r"(?:(\w+)\.)?(\w+)\s+(?:IN|BETWEEN|LIKE|ILIKE)\s",
            r"(?:(\w+)\.)?(\w+)\s+IS\s+(?:NOT\s+)?NULL",
        ]

        cols_found: List[str] = []
        seen: Set[str] = set()

        for pat in patterns:
            for m in re.finditer(pat, where_clause, re.IGNORECASE):
                col = m.group(2)
                if not col:
                    continue
                col_lower = col.lower()
                if col.upper() in _SQL_KEYWORDS:
                    continue
                if col_lower in seen:
                    continue
                # Skip numeric-looking or single-character names
                if col.isdigit() or len(col) <= 1:
                    continue
                seen.add(col_lower)
                cols_found.append(col_lower)

        # Also check JOIN ON conditions for leading columns
        join_m = re.finditer(
            r"JOIN\s+\S+\s+\w*\s*ON\s+(?:(\w+)\.)?(\w+)\s*=\s*(?:(\w+)\.)?(\w+)",
            query, re.IGNORECASE,
        )
        for jm in join_m:
            for g in (2, 4):
                col = jm.group(g)
                if col and col.upper() not in _SQL_KEYWORDS and col.lower() not in seen:
                    seen.add(col.lower())
                    cols_found.append(col.lower())

        # Return at most 4 columns (more than that is rarely a useful composite index)
        return cols_found[:4]

    def _generate_index_name(self, table: str, columns: List[str]) -> str:
        """Generate a deterministic, non-conflicting index name."""
        col_part = "_".join(columns[:3])
        # Use hash to keep names short and unique
        hash_input = f"{table}_{col_part}"
        short_hash = hashlib.md5(hash_input.encode()).hexdigest()[:6]
        # Truncate to stay within 63-char PG identifier limit
        name = f"idx_{table}_{col_part}"
        if len(name) > 56:
            name = f"idx_{table[:20]}_{col_part[:20]}"
        return f"{name}_{short_hash}"

    def _deduplicate(self, recs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Deduplicate by (schema.table, column set). Keep highest benefit."""
        seen: Dict[str, Dict] = {}
        for r in recs:
            key = f"{r['schema_name']}.{r['table_name']}:{r.get('suggested_columns', '')}"
            if key in seen:
                existing = seen[key]
                # Merge fingerprints from both sources
                try:
                    fps1 = json.loads(existing.get("related_fingerprints", "[]"))
                    fps2 = json.loads(r.get("related_fingerprints", "[]"))
                    merged_fps = list(set(fps1 + fps2))
                except (json.JSONDecodeError, TypeError):
                    merged_fps = []

                if r.get("estimated_benefit", 0) > existing.get("estimated_benefit", 0):
                    r["related_fingerprints"] = json.dumps(merged_fps)
                    seen[key] = r
                else:
                    existing["related_fingerprints"] = json.dumps(merged_fps)
            else:
                seen[key] = r

        return sorted(seen.values(), key=lambda x: x.get("estimated_benefit", 0), reverse=True)
