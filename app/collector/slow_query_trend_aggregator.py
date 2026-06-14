import json
import logging
import time
from collections import defaultdict
from typing import Any, Dict, List

from app.database.sqlite_store import SQLiteStore

logger = logging.getLogger(__name__)


class SlowQueryTrendAggregator:
    def __init__(self, store: SQLiteStore, top_n_clients: int = 10):
        self.store = store
        self.top_n_clients = top_n_clients

    async def aggregate(self, db_ids: List[str]):
        now = time.time()
        current_hour_start = (now // 3600) * 3600
        target_hour = current_hour_start - 3600

        for db_id in db_ids:
            try:
                await self._aggregate_hour(db_id, target_hour)
            except Exception as e:
                logger.error(f"[{db_id}] Trend aggregation error: {e}")

    async def _aggregate_hour(self, db_id: str, hour_bucket: float):
        exists = await self.store.get_trend_hour_exists(db_id, hour_bucket)
        if exists:
            return

        hour_end = hour_bucket + 3600
        queries = await self.store.get_slow_queries(
            db_id, window_seconds=int(time.time() - hour_bucket),
            min_duration=0, search="", limit=10000,
        )

        in_range = [
            q for q in queries
            if hour_bucket <= (q.get("query_start") or q.get("captured_at", 0)) < hour_end
        ]

        if not in_range:
            return

        grouped: Dict[str, List[Dict]] = defaultdict(list)
        for q in in_range:
            grouped[q["fingerprint"]].append(q)

        trend_rows = []
        for fp, items in grouped.items():
            durations = [i["duration_seconds"] for i in items]
            users = [i.get("username") or "unknown" for i in items]
            clients = [i.get("client_addr") or "unknown" for i in items]

            user_counts = defaultdict(int)
            for u in users:
                user_counts[u] += 1
            top_users = sorted(
                [{"user": u, "count": c} for u, c in user_counts.items()],
                key=lambda x: x["count"], reverse=True
            )[:5]

            client_counts = defaultdict(int)
            for c in clients:
                client_counts[c] += 1
            sorted_clients = sorted(client_counts.items(), key=lambda x: x[1], reverse=True)
            top_clients_list = [{"client": c, "count": n} for c, n in sorted_clients[:self.top_n_clients]]
            others_count = sum(n for _, n in sorted_clients[self.top_n_clients:])
            if others_count > 0:
                top_clients_list.append({"client": "others", "count": others_count})

            trend_rows.append({
                "db_id": db_id,
                "hour_bucket": hour_bucket,
                "fingerprint": fp,
                "query_pattern": (items[0].get("query_text") or "")[:200],
                "occurrence_count": len(items),
                "total_duration": sum(durations),
                "avg_duration": sum(durations) / len(durations),
                "max_duration": max(durations),
                "distinct_users": len(set(users)),
                "top_users": json.dumps(top_users, ensure_ascii=False),
                "top_clients": json.dumps(top_clients_list, ensure_ascii=False),
            })

        if trend_rows:
            await self.store.insert_slow_query_trends(trend_rows)
            logger.info(f"[{db_id}] Aggregated {len(trend_rows)} patterns for hour {int(hour_bucket)}")

        # Pre-aggregate per-client stats for fast drill-down
        client_agg: Dict[str, Dict[str, Any]] = defaultdict(
            lambda: {"count": 0, "duration": 0.0, "fingerprints": defaultdict(int)}
        )
        for q in in_range:
            addr = q.get("client_addr") or "unknown"
            fp = q["fingerprint"]
            client_agg[addr]["count"] += 1
            client_agg[addr]["duration"] += q["duration_seconds"]
            client_agg[addr]["fingerprints"][fp] += 1

        client_rows = []
        for addr, stats in client_agg.items():
            # Top 5 fingerprints per client
            sorted_fps = sorted(stats["fingerprints"].items(), key=lambda x: x[1], reverse=True)[:5]
            top_fps = [{"fp": fp, "count": c} for fp, c in sorted_fps]
            client_rows.append({
                "db_id": db_id,
                "hour_bucket": hour_bucket,
                "client_addr": addr,
                "occurrence_count": stats["count"],
                "total_duration": stats["duration"],
                "top_fingerprints": json.dumps(top_fps, ensure_ascii=False),
            })

        if client_rows:
            await self.store.insert_client_trend_stats(client_rows)
