import asyncio
import logging
from typing import Any, Callable, Coroutine, Dict, Optional

from app.config import AppConfig
from app.database.connection_manager import ConnectionManager, DatabaseConnection
from app.database.circuit_breaker import CircuitOpenError
from app.database.sqlite_store import SQLiteStore
from app.collector.metrics_collector import MetricsCollector
from app.collector.slow_query_collector import SlowQueryCollector

logger = logging.getLogger(__name__)


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
        self._prune_task: Optional[asyncio.Task] = None
        self._metrics_collectors: Dict[str, MetricsCollector] = {}
        self._slow_query_collectors: Dict[str, SlowQueryCollector] = {}
        self._latest_metrics: Dict[str, Dict[str, Any]] = {}

    @property
    def latest_metrics(self) -> Dict[str, Dict[str, Any]]:
        return self._latest_metrics

    async def start(self):
        for db_id, conn in self.conn_manager.connections.items():
            self._start_db_tasks(db_id, conn)
        self._prune_task = asyncio.create_task(self._prune_loop())
        logger.info("Collector scheduler started")

    def _start_db_tasks(self, db_id: str, conn: DatabaseConnection):
        mc = MetricsCollector(conn, self.store)
        sq = SlowQueryCollector(
            conn, self.store, threshold=self.config.collection.slow_query_threshold
        )
        self._metrics_collectors[db_id] = mc
        self._slow_query_collectors[db_id] = sq

        self._metrics_tasks[db_id] = asyncio.create_task(
            self._metrics_loop(db_id, mc)
        )
        self._slow_query_tasks[db_id] = asyncio.create_task(
            self._slow_query_loop(db_id, sq)
        )

    async def _metrics_loop(self, db_id: str, collector: MetricsCollector):
        interval = self.config.collection.metrics_interval
        table_counter = 0
        table_interval = max(interval * 4, 60)

        while True:
            try:
                metrics = await collector.collect()
                self._latest_metrics[db_id] = metrics

                table_counter += interval
                if table_counter >= table_interval:
                    tables = await collector.collect_top_tables(
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

    async def _slow_query_loop(self, db_id: str, collector: SlowQueryCollector):
        interval = self.config.collection.slow_query_interval

        while True:
            try:
                await collector.collect()
            except CircuitOpenError:
                logger.debug(f"[{db_id}] Skipping slow query sample (circuit open)")
            except asyncio.CancelledError:
                return
            except Exception as e:
                logger.error(f"[{db_id}] Slow query collection error: {e}")

            await asyncio.sleep(interval)

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
        if db_id in self._metrics_tasks:
            self._metrics_tasks[db_id].cancel()
            del self._metrics_tasks[db_id]
        if db_id in self._slow_query_tasks:
            self._slow_query_tasks[db_id].cancel()
            del self._slow_query_tasks[db_id]
        self._metrics_collectors.pop(db_id, None)
        self._slow_query_collectors.pop(db_id, None)
        self._latest_metrics.pop(db_id, None)

    async def stop(self):
        all_tasks = list(self._metrics_tasks.values()) + list(self._slow_query_tasks.values())
        if self._prune_task:
            all_tasks.append(self._prune_task)
        for task in all_tasks:
            task.cancel()
        await asyncio.gather(*all_tasks, return_exceptions=True)
        self._metrics_tasks.clear()
        self._slow_query_tasks.clear()
        logger.info("Collector scheduler stopped")
