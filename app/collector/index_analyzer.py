import json
import logging
import re
from typing import Any, Dict, List, Optional, Set, Tuple

from app.database.sqlite_store import SQLiteStore

logger = logging.getLogger(__name__)

IDX_COLS_PATTERN = re.compile(
    r"CREATE\s+(?:UNIQUE\s+)?INDEX\s+\S+\s+ON\s+(?:ONLY\s+)?\S+(?:\s+USING\s+\w+)?\s*\((.+?)\)"
    r"(?:\s+(?:INCLUDE\s*\(.+?\)\s*)?(?:WHERE\s+(.+))?)?\s*$",
    re.IGNORECASE | re.DOTALL,
)

EXPRESSION_PATTERN = re.compile(r"[(\"]")


def parse_index_columns(index_def: str) -> Optional[Tuple[List[str], str, str, bool]]:
    """Parse index definition -> (columns, where_clause, access_method, has_expressions).
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
    has_expressions = any(is_expression_column(c) for c in cols)
    return cols, where_clause, access_method, has_expressions


def _split_columns(cols_str: str) -> List[str]:
    """Split column list respecting parentheses for expressions."""
    result = []
    depth = 0
    current = []
    for ch in cols_str:
        if ch == '(':
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
    """Strip ASC/DESC/NULLS FIRST/LAST and collation for comparison."""
    col = re.sub(r"\s+(ASC|DESC)\s*$", "", col, flags=re.IGNORECASE)
    col = re.sub(r"\s+NULLS\s+(FIRST|LAST)\s*$", "", col, flags=re.IGNORECASE)
    col = re.sub(r"\s+COLLATE\s+\S+", "", col, flags=re.IGNORECASE)
    return col.strip().lower()


def columns_match(cols_a: List[str], cols_b: List[str]) -> bool:
    """Exact column list match after normalization."""
    if len(cols_a) != len(cols_b):
        return False
    return all(normalize_col(a) == normalize_col(b) for a, b in zip(cols_a, cols_b))


def is_prefix_of(shorter: List[str], longer: List[str]) -> bool:
    """Check if shorter is a strict prefix of longer."""
    if len(shorter) >= len(longer):
        return False
    return all(normalize_col(a) == normalize_col(b) for a, b in zip(shorter, longer))


def _is_constraint_index(idx: Dict) -> bool:
    """Determine if an index backs a constraint (PK, unique, exclusion)."""
    return bool(idx.get("is_primary") or idx.get("is_unique"))


def _is_expression_index(parsed) -> bool:
    """Check if the parsed index contains expression columns."""
    if parsed is None:
        return False
    return parsed[3]  # has_expressions flag


def _is_partial_index(parsed) -> bool:
    """Check if the parsed index has a WHERE clause (partial index)."""
    if parsed is None:
        return False
    return bool(parsed[1])  # where_clause


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
        """Detect truly redundant indexes.

        Rules to AVOID false positives:
        - Never mark a unique/primary index as redundant (it enforces a constraint)
        - Partial indexes (WHERE) are only redundant if another index has the SAME WHERE
        - Expression indexes are only redundant if the expressions match exactly
        - Non-btree indexes (GIN, GiST, BRIN) are only compared within same access method
        - A shorter unique index is NOT redundant of a longer non-unique superset
        """
        results = []
        seen_redundant: Set[str] = set()

        for i, (idx_a, pa) in enumerate(parsed):
            if pa is None:
                continue
            cols_a, where_a, am_a, expr_a = pa

            for j, (idx_b, pb) in enumerate(parsed):
                if j <= i or pb is None:
                    continue
                cols_b, where_b, am_b, expr_b = pb

                # Different access methods are never redundant to each other
                if am_a != am_b:
                    continue

                # Partial indexes: WHERE clauses must be semantically identical
                # (we compare textually after lowercasing/stripping)
                if where_a.lower().strip() != where_b.lower().strip():
                    continue

                # If either has expressions, only match if columns are EXACTLY identical
                # (we can't safely determine prefix relationships for expressions)
                if expr_a or expr_b:
                    if not columns_match(cols_a, cols_b):
                        continue
                    # Exact duplicate with expressions
                    redundant_idx = self._pick_redundant_for_duplicate(idx_a, idx_b)
                    if redundant_idx and redundant_idx["index_name"] not in seen_redundant:
                        superset_idx = idx_b if redundant_idx is idx_a else idx_a
                        seen_redundant.add(redundant_idx["index_name"])
                        results.append(self._make_redundant_rec(
                            redundant_idx, superset_idx,
                            reason=f"与表达式索引 {superset_idx['index_name']} 完全重复",
                        ))
                    continue

                norm_a = [normalize_col(c) for c in cols_a]
                norm_b = [normalize_col(c) for c in cols_b]

                redundant_idx = None
                superset_idx = None

                if norm_a == norm_b:
                    # Exact duplicate columns
                    redundant_idx = self._pick_redundant_for_duplicate(idx_a, idx_b)
                    if redundant_idx:
                        superset_idx = idx_b if redundant_idx is idx_a else idx_a
                elif is_prefix_of(norm_a, norm_b):
                    # idx_a's columns are a prefix of idx_b
                    # Only redundant if idx_a does NOT enforce a unique constraint
                    if not _is_constraint_index(idx_a):
                        redundant_idx = idx_a
                        superset_idx = idx_b
                elif is_prefix_of(norm_b, norm_a):
                    # idx_b's columns are a prefix of idx_a
                    if not _is_constraint_index(idx_b):
                        redundant_idx = idx_b
                        superset_idx = idx_a

                if redundant_idx and redundant_idx["index_name"] not in seen_redundant:
                    seen_redundant.add(redundant_idx["index_name"])
                    results.append(self._make_redundant_rec(
                        redundant_idx, superset_idx,
                        reason=f"被索引 {superset_idx['index_name']} 覆盖（前缀冗余）",
                    ))

        return results

    def _pick_redundant_for_duplicate(self, idx_a: Dict, idx_b: Dict) -> Optional[Dict]:
        """For two indexes with identical columns, decide which one is redundant.
        Returns None if neither can be safely removed."""
        a_constraint = _is_constraint_index(idx_a)
        b_constraint = _is_constraint_index(idx_b)

        # Both are constraints — cannot remove either
        if a_constraint and b_constraint:
            return None
        # One is a constraint, keep it, the other is redundant
        if a_constraint:
            return idx_b
        if b_constraint:
            return idx_a
        # Neither is a constraint — keep whichever has more scans
        if idx_a.get("idx_scan", 0) >= idx_b.get("idx_scan", 0):
            return idx_b
        return idx_a

    def _make_redundant_rec(self, redundant_idx: Dict, superset_idx: Dict, reason: str) -> Dict:
        risk = "low"
        # Higher risk if the index is actually being used
        if redundant_idx.get("idx_scan", 0) > self.low_scan_threshold:
            risk = "medium"
        # Higher risk if index is large
        if redundant_idx.get("index_size_bytes", 0) > 100 * 1024 * 1024:
            risk = "medium"

        return self._make_recommendation(
            redundant_idx,
            category="redundant",
            risk_level=risk,
            reason=reason,
            related=[superset_idx["index_name"]],
        )

    def _detect_unused(self, parsed: List) -> List[Dict]:
        """Detect unused indexes.

        NEVER recommend removing:
        - Primary key indexes (enforce PK constraint)
        - Unique indexes (enforce unique constraint)
        - Partial indexes (often used for constraint enforcement or specific workloads)
        - Expression indexes (often for specific application logic)
        - Very small indexes (negligible cost)

        For low-frequency-but-critical indexes, emit an informational entry
        instead of a removal recommendation.
        """
        results = []
        for idx, p in parsed:
            # Skip tiny indexes — not worth the noise
            if idx.get("index_size_bytes", 0) < self.min_index_size:
                continue

            is_used = idx.get("idx_scan", 0) > self.low_scan_threshold
            if is_used:
                continue

            # Constraint-backed indexes: never recommend removal
            if _is_constraint_index(idx):
                results.append(self._make_recommendation(
                    idx,
                    category="low_freq_critical",
                    risk_level="info",
                    reason=f"扫描频率低（{idx.get('idx_scan', 0)} 次）但支撑 {'主键' if idx.get('is_primary') else '唯一'} 约束，不可删除",
                    related=[],
                ))
                continue

            # Partial index: likely for a specific use case, mark as informational
            if _is_partial_index(p):
                results.append(self._make_recommendation(
                    idx,
                    category="low_freq_critical",
                    risk_level="info",
                    reason=f"部分索引（含 WHERE 子句），扫描 {idx.get('idx_scan', 0)} 次，可能服务于特定业务逻辑",
                    related=[],
                ))
                continue

            # Expression index: likely for a specific use case
            if _is_expression_index(p):
                results.append(self._make_recommendation(
                    idx,
                    category="low_freq_critical",
                    risk_level="info",
                    reason=f"表达式索引，扫描 {idx.get('idx_scan', 0)} 次，可能服务于特定查询场景",
                    related=[],
                ))
                continue

            # Regular non-constraint, non-partial, non-expression unused index
            write_load = (idx.get("n_tup_ins", 0) + idx.get("n_tup_upd", 0)
                          + idx.get("n_tup_del", 0))
            risk = "low"
            if write_load > 100000:
                risk = "medium"
            elif write_load > 1000000:
                risk = "high"

            results.append(self._make_recommendation(
                idx,
                category="unused",
                risk_level=risk,
                reason=f"近期扫描 {idx.get('idx_scan', 0)} 次（阈值 {self.low_scan_threshold}），表写入 {write_load:,} ops，维护成本高于收益",
                related=[],
            ))

        return results

    def _detect_mergeable(self, parsed: List) -> List[Dict]:
        """Detect potentially mergeable single-column indexes.

        Only considers plain btree indexes on simple columns (no expressions,
        no WHERE, no unique constraint). Requires at least 3 such indexes on
        the same table to suggest merging.
        """
        results = []
        single_col_indexes = []

        for idx, p in parsed:
            if p is None:
                continue
            cols, where, am, has_expr = p
            if (len(cols) == 1
                    and am == "btree"
                    and not where
                    and not has_expr
                    and not _is_constraint_index(idx)):
                single_col_indexes.append((idx, normalize_col(cols[0])))

        if len(single_col_indexes) >= 3:
            col_names = [c for _, c in single_col_indexes]
            idx_names = [idx["index_name"] for idx, _ in single_col_indexes]
            merged_reason = (
                f"同表有 {len(single_col_indexes)} 个非约束单列 btree 索引"
                f"（{', '.join(col_names[:5])}），如查询常组合使用这些列，"
                f"可考虑合并为复合索引以减少写入开销"
            )
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

        # For informational entries, don't generate DROP DDL
        if risk_level == "info":
            drop_ddl = ""
            rollback_ddl = ""
        else:
            drop_ddl = f"DROP INDEX CONCURRENTLY IF EXISTS {schema}.{index_name};"
            # Generate rollback DDL from original def, adding CONCURRENTLY
            if index_def:
                if "CONCURRENTLY" in index_def.upper():
                    rollback_ddl = index_def + ";"
                else:
                    rollback_ddl = re.sub(
                        r"CREATE\s+(UNIQUE\s+)?INDEX",
                        r"CREATE \1INDEX CONCURRENTLY",
                        index_def, count=1, flags=re.IGNORECASE
                    ).strip() + ";"
            else:
                rollback_ddl = ""

        return {
            "schema_name": schema,
            "table_name": idx["table_name"],
            "index_name": index_name,
            "category": category,
            "risk_level": risk_level,
            "reason": reason,
            "drop_ddl": drop_ddl,
            "rollback_ddl": rollback_ddl,
            "estimated_size_savings": idx.get("index_size_bytes", 0) if risk_level != "info" else 0,
            "related_indexes": json.dumps(related),
        }
