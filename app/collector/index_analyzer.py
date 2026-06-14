import json
import logging
import re
from typing import Any, Dict, List, Optional, Set, Tuple

from app.database.sqlite_store import SQLiteStore

logger = logging.getLogger(__name__)

IDX_COLS_PATTERN = re.compile(
    r"CREATE\s+(?:UNIQUE\s+)?INDEX\s+\S+\s+ON\s+\S+(?:\s+USING\s+\w+)?\s*\((.+?)\)"
    r"(?:\s+WHERE\s+(.+))?$",
    re.IGNORECASE | re.DOTALL,
)

EXPRESSION_PATTERN = re.compile(r"[(\"]")


def parse_index_columns(index_def: str) -> Optional[Tuple[List[str], str, str]]:
    """Parse index definition -> (columns, where_clause, access_method).
    Returns None if parsing fails."""
    if not index_def:
        return None

    am_match = re.search(r"USING\s+(\w+)", index_def, re.IGNORECASE)
    access_method = am_match.group(1).lower() if am_match else "btree"

    m = IDX_COLS_PATTERN.search(index_def)
    if not m:
        return None

    cols_str = m.group(1).strip()
    where_clause = (m.group(2) or "").strip()

    cols = [c.strip() for c in _split_columns(cols_str)]
    return cols, where_clause, access_method


def _split_columns(cols_str: str) -> List[str]:
    """Split column list respecting parentheses for expressions."""
    result = []
    depth = 0
    current = []
    for ch in cols_str:
        if ch == '(' :
            depth += 1
            current.append(ch)
        elif ch == ')':
            depth -= 1
            current.append(ch)
        elif ch == ',' and depth == 0:
            result.append(''.join(current).strip())
            current = []
        else:
            current.append(ch)
    if current:
        result.append(''.join(current).strip())
    return result


def is_expression_column(col: str) -> bool:
    return bool(EXPRESSION_PATTERN.search(col))


def normalize_col(col: str) -> str:
    """Strip ASC/DESC/NULLS FIRST/LAST for comparison."""
    col = re.sub(r"\s+(ASC|DESC|NULLS\s+(FIRST|LAST))\s*$", "", col, flags=re.IGNORECASE)
    return col.strip().lower()


def is_prefix(shorter: List[str], longer: List[str]) -> bool:
    if len(shorter) >= len(longer):
        return False
    for a, b in zip(shorter, longer):
        if normalize_col(a) != normalize_col(b):
            return False
    return True


class IndexAnalyzer:
    def __init__(self, store: SQLiteStore, config):
        self.store = store
        self.low_scan_threshold = getattr(config, 'low_scan_threshold', 50)
        self.min_index_size = getattr(config, 'min_index_size_bytes', 8192)

    async def analyze(self, db_id: str) -> List[Dict[str, Any]]:
        stats = await self.store.get_latest_index_stats(db_id)
        if not stats:
            return []

        recommendations = []

        tables: Dict[str, List[Dict]] = {}
        for s in stats:
            key = f"{s['schema_name']}.{s['table_name']}"
            tables.setdefault(key, []).append(s)

        for table_key, indexes in tables.items():
            recommendations.extend(self._analyze_table_indexes(indexes))

        await self.store.save_index_recommendations(db_id, recommendations)
        logger.info(f"[{db_id}] Index analysis complete: {len(recommendations)} recommendations")
        return recommendations

    def _analyze_table_indexes(self, indexes: List[Dict]) -> List[Dict[str, Any]]:
        results = []
        parsed = []

        for idx in indexes:
            p = parse_index_columns(idx.get("index_def", ""))
            parsed.append((idx, p))

        results.extend(self._detect_redundant(parsed))
        results.extend(self._detect_unused(parsed))
        results.extend(self._detect_mergeable(parsed))

        return results

    def _detect_redundant(self, parsed: List) -> List[Dict]:
        results = []
        seen_redundant: Set[str] = set()

        for i, (idx_a, pa) in enumerate(parsed):
            if pa is None:
                continue
            cols_a, where_a, am_a = pa

            for j, (idx_b, pb) in enumerate(parsed):
                if j <= i or pb is None:
                    continue
                cols_b, where_b, am_b = pb

                if am_a != am_b:
                    continue
                if where_a != where_b:
                    continue

                norm_a = [normalize_col(c) for c in cols_a]
                norm_b = [normalize_col(c) for c in cols_b]

                redundant_idx = None
                superset_idx = None

                if norm_a == norm_b:
                    if idx_a.get("is_unique") or idx_a.get("is_primary"):
                        redundant_idx = idx_b
                        superset_idx = idx_a
                    else:
                        redundant_idx = idx_a
                        superset_idx = idx_b
                elif is_prefix(norm_a, norm_b):
                    if not idx_a.get("is_unique") and not idx_a.get("is_primary"):
                        redundant_idx = idx_a
                        superset_idx = idx_b
                elif is_prefix(norm_b, norm_a):
                    if not idx_b.get("is_unique") and not idx_b.get("is_primary"):
                        redundant_idx = idx_b
                        superset_idx = idx_a

                if redundant_idx and redundant_idx["index_name"] not in seen_redundant:
                    seen_redundant.add(redundant_idx["index_name"])
                    risk = "low"
                    if redundant_idx.get("idx_scan", 0) > self.low_scan_threshold:
                        risk = "medium"

                    results.append(self._make_recommendation(
                        redundant_idx,
                        category="redundant",
                        risk_level=risk,
                        reason=f"被索引 {superset_idx['index_name']} 覆盖（前缀冗余）",
                        related=[superset_idx["index_name"]],
                    ))

        return results

    def _detect_unused(self, parsed: List) -> List[Dict]:
        results = []
        for idx, p in parsed:
            if idx.get("is_unique") or idx.get("is_primary"):
                continue
            if idx.get("index_size_bytes", 0) < self.min_index_size:
                continue
            if idx.get("idx_scan", 0) <= self.low_scan_threshold:
                if idx.get("is_unique") or idx.get("is_primary"):
                    results.append(self._make_recommendation(
                        idx,
                        category="low_freq_critical",
                        risk_level="info",
                        reason="扫描频率低但为唯一/主键约束索引",
                        related=[],
                    ))
                else:
                    write_load = (idx.get("n_tup_ins", 0) + idx.get("n_tup_upd", 0)
                                  + idx.get("n_tup_del", 0))
                    risk = "medium" if write_load > 10000 else "low"
                    results.append(self._make_recommendation(
                        idx,
                        category="unused",
                        risk_level=risk,
                        reason=f"近期扫描次数仅 {idx.get('idx_scan', 0)}，写入开销: {write_load} ops",
                        related=[],
                    ))
        return results

    def _detect_mergeable(self, parsed: List) -> List[Dict]:
        results = []
        single_col_indexes = []

        for idx, p in parsed:
            if p is None:
                continue
            cols, where, am = p
            if (len(cols) == 1 and am == "btree" and not where
                    and not is_expression_column(cols[0])
                    and not idx.get("is_unique") and not idx.get("is_primary")):
                single_col_indexes.append((idx, normalize_col(cols[0])))

        if len(single_col_indexes) >= 3:
            col_names = [c for _, c in single_col_indexes]
            idx_names = [idx["index_name"] for idx, _ in single_col_indexes]
            merged_reason = f"同表有 {len(single_col_indexes)} 个单列索引，可考虑合并为复合索引: ({', '.join(col_names[:5])})"
            for idx, _ in single_col_indexes:
                results.append(self._make_recommendation(
                    idx,
                    category="mergeable",
                    risk_level="low",
                    reason=merged_reason,
                    related=idx_names,
                ))

        return results

    def _make_recommendation(
        self, idx: Dict, category: str, risk_level: str,
        reason: str, related: List[str]
    ) -> Dict[str, Any]:
        schema = idx["schema_name"]
        index_name = idx["index_name"]
        index_def = idx.get("index_def", "")

        drop_ddl = f"DROP INDEX CONCURRENTLY IF EXISTS {schema}.{index_name};"
        rollback_ddl = index_def.replace("CREATE INDEX", "CREATE INDEX CONCURRENTLY", 1) + ";" if index_def else ""

        return {
            "schema_name": schema,
            "table_name": idx["table_name"],
            "index_name": index_name,
            "category": category,
            "risk_level": risk_level,
            "reason": reason,
            "drop_ddl": drop_ddl,
            "rollback_ddl": rollback_ddl,
            "estimated_size_savings": idx.get("index_size_bytes", 0),
            "related_indexes": json.dumps(related),
        }
