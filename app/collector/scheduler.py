import asyncio
import logging
from typing import Any, Callable, Dict, List, Optional

from app.config import AppConfig, CollectionConfig
from app.database.connection_manager import ConnectionManager, DatabaseConnection
from app.database.circuit_breaker import CircuitOpenError
from app.database.version_detector import VersionDetector
from app.database.sqlite_store import SQLiteStore
from app.collector.metrics_collector import MetricsCollector
from app.collector.slow_query_collector import SlowQueryCollector
from app.collector.index_collector import IndexCollector
from app.collector.index_analyzer import IndexAnalyzer
from app.collector.missing_index_analyzer import MissingIndexAnalyzer
from app.collector.slow_query_trend_aggregator import SlowQueryTrendAggregator
from app.collector.health_scorer import HealthScorer
from app.collector.alert_manager import AlertManager

logger = logging.getLogger(__name__)

RECONNECT_INTERVAL = 30


class CollectorScheduler:
    def __init__(
        self,
        conn_manager: ConnectionManager,
        store: SQLiteStore,
        config: AppConfig,
        broadcast_callback: Optional[Callable] = None,
    ):
        self.conn_manager = conn_manager
        self.store = store
        self.config = config
        self.broadcast_callback = broadcast_callback
        self._metrics_tasks: Dict[str, asyncio.Task] = {}
        self._slow_query_tasks: Dict[str, asyncio.Task] = {}
        self._index_tasks: Dict[str, asyncio.Task] = {}
        self._health_tasks: Dict[str, asyncio.Task] = {}
        self._prune_task: Optional[asyncio.Task] = None
        self._trend_task: Optional[asyncio.Task] = None
        self._metrics_collectors: Dict[str, MetricsCollector] = {}
        self._slow_query_collectors: Dict[str, SlowQueryCollector] = {}
        self._latest_metrics: Dict[str, Dict[str, Any]] = {}

        self._alert_manager = AlertManager(
            config.alerts, store, broadcast_callback
        )
        self._trend_aggregator = SlowQueryTrendAggregator(
            store, config.slow_query_trends.top_n_clients
        )

    @property
    def latest_metrics(self) -> Dict[str, Dict[str, Any]]:
        return self._latest_metrics

    def update_config(self, new_config: AppConfig):
        self.config = new_config

    async def start(self):
        for db_id, conn in self.conn_manager.connections.items():
            self._start_db_tasks(db_id, conn)
        self._prune_task = asyncio.create_task(self._prune_loop())
        if self.config.slow_query_trends.enabled:
            self._trend_task = asyncio.create_task(self._trend_loop())
        logger.info("Collector scheduler started")

    def _start_db_tasks(self, db_id: str, conn: DatabaseConnection):
        mc = MetricsCollector(conn, self.store)
        sq = SlowQueryCollector(
            conn, self.store, threshold=self.config.collection.slow_query_threshold
        )
        self._metrics_collectors[db_id] = mc
        self._slow_query_collectors[db_id] = sq

        self._metrics_tasks[db_id] = asyncio.create_task(
            self._metrics_loop(db_id, conn, mc)
        )
        self._slow_query_tasks[db_id] = asyncio.create_task(
            self._slow_query_loop(db_id, conn, sq)
        )

        if self.config.index_analysis.enabled:
            self._index_tasks[db_id] = asyncio.create_task(
                self._index_analysis_loop(db_id, conn)
            )

        if self.config.health_score.enabled:
            self._health_tasks[db_id] = asyncio.create_task(
                self._health_score_loop(db_id, conn)
            )

    async def _try_reconnect(self, db_id: str, conn: DatabaseConnection) -> bool:
        if conn.is_connected:
            return True
        try:
            await conn.ensure_connected()
            if conn.is_connected:
                detector = VersionDetector()
                await detector.detect(conn)
                logger.info(f"[{db_id}] Reconnected, PG{conn.pg_version}")
                return True
        except Exception as e:
            logger.debug(f"[{db_id}] Reconnect attempt failed: {e}")
        return False

    async def _metrics_loop(
        self, db_id: str, conn: DatabaseConnection, collector: MetricsCollector
    ):
        table_counter = 0

        while True:
            interval = self.config.collection.metrics_interval
            table_interval = max(interval * 4, 60)

            if not conn.is_connected:
                if not await self._try_reconnect(db_id, conn):
                    await asyncio.sleep(RECONNECT_INTERVAL)
                    continue

            try:
                metrics = await collector.collect()
                self._latest_metrics[db_id] = metrics

                table_counter += interval
                if table_counter >= table_interval:
                    await collector.collect_top_tables(
                        self.config.collection.top_tables_limit
                    )
                    table_counter = 0

                if self.broadcast_callback:
                    await self.broadcast_callback(db_id, metrics)

            except CircuitOpenError:
                logger.debug(f"[{db_id}] Skipping metrics (circuit open)")
            except asyncio.CancelledError:
                return
            except Exception as e:
                logger.error(f"[{db_id}] Metrics collection error: {e}")

            await asyncio.sleep(interval)

    async def _slow_query_loop(
        self, db_id: str, conn: DatabaseConnection, collector: SlowQueryCollector
    ):
        while True:
            interval = self.config.collection.slow_query_interval

            if not conn.is_connected:
                await asyncio.sleep(RECONNECT_INTERVAL)
                continue

            try:
                collector.threshold = self.config.collection.slow_query_threshold
                await collector.collect()
            except CircuitOpenError:
                logger.debug(f"[{db_id}] Skipping slow query sample (circuit open)")
            except asyncio.CancelledError:
                return
            except Exception as e:
                logger.error(f"[{db_id}] Slow query collection error: {e}")

            await asyncio.sleep(interval)

    async def _index_analysis_loop(self, db_id: str, conn: DatabaseConnection):
        collector = IndexCollector(conn, self.store)
        analyzer = IndexAnalyzer(self.store, self.config.index_analysis)
        missing_analyzer = MissingIndexAnalyzer(self.store, self.config.missing_index)

        collection_interval = self.config.index_analysis.collection_interval
        analysis_interval = self.config.index_analysis.analysis_interval
        time_since_analysis = analysis_interval

        while True:
            if not conn.is_connected:
                await asyncio.sleep(RECONNECT_INTERVAL)
                continue

            try:
                await collector.collect()

                time_since_analysis += collection_interval
                if time_since_analysis >= analysis_interval:
                    await analyzer.analyze(db_id)
                    if self.config.missing_index.enabled:
                        await missing_analyzer.analyze(db_id, conn)
                    time_since_analysis = 0

            except CircuitOpenError:
                logger.debug(f"[{db_id}] Skipping index collection (circuit open)")
            except asyncio.CancelledError:
                return
            except Exception as e:
                logger.error(f"[{db_id}] Index analysis error: {e}")

            await asyncio.sleep(collection_interval)

    async def _health_score_loop(self, db_id: str, conn: DatabaseConnection):
        scorer = HealthScorer(self.store, self.config.health_score)
        interval = self.config.health_score.compute_interval

        while True:
            if not conn.is_connected:
                await asyncio.sleep(RECONNECT_INTERVAL)
                continue

            try:
                metrics = self._latest_metrics.get(db_id, {})
                result = await scorer.compute(db_id, conn, metrics)

                if result.get("anomalies"):
                    await self._alert_manager.evaluate(
                        db_id, result["dimension_scores"], result["anomalies"]
                    )

            except CircuitOpenError:
                logger.debug(f"[{db_id}] Skipping health score (circuit open)")
            except asyncio.CancelledError:
                return
            except Exception as e:
                logger.error(f"[{db_id}] Health score error: {e}")

            await asyncio.sleep(interval)

    async def _trend_loop(self):
        while True:
            try:
                await asyncio.sleep(self.config.slow_query_trends.aggregation_interval)
                db_ids = self.conn_manager.all_ids()
                await self._trend_aggregator.aggregate(db_ids)
            except asyncio.CancelledError:
                return
            except Exception as e:
                logger.error(f"Trend aggregation error: {e}")

    async def _prune_loop(self):
        while True:
            try:
                await asyncio.sleep(3600)
                await self.store.prune_old_data(self.config.collection.retention_hours)
            except asyncio.CancelledError:
                return
            except Exception as e:
                logger.error(f"Prune error: {e}")

    async def add_database(self, db_id: str, conn: DatabaseConnection):
        self._start_db_tasks(db_id, conn)

    async def remove_database(self, db_id: str):
        for task_dict in (self._metrics_tasks, self._slow_query_tasks,
                          self._index_tasks, self._health_tasks):
            if db_id in task_dict:
                task_dict[db_id].cancel()
                del task_dict[db_id]
        self._metrics_collectors.pop(db_id, None)
        self._slow_query_collectors.pop(db_id, None)
        self._latest_metrics.pop(db_id, None)

    async def stop(self):
        all_tasks = (
            list(self._metrics_tasks.values())
            + list(self._slow_query_tasks.values())
            + list(self._index_tasks.values())
            + list(self._health_tasks.values())
        )
        if self._prune_task:
            all_tasks.append(self._prune_task)
        if self._trend_task:
            all_tasks.append(self._trend_task)
        for task in all_tasks:
            task.cancel()
        await asyncio.gather(*all_tasks, return_exceptions=True)
        self._metrics_tasks.clear()
        self._slow_query_tasks.clear()
        self._index_tasks.clear()
        self._health_tasks.clear()
        logger.info("Collector scheduler stopped")
