import asyncio
import logging
import random
from typing import Any, Dict, List, Optional

import asyncpg

from app.config import DatabaseConfig, CircuitBreakerConfig
from app.database.circuit_breaker import CircuitBreaker, CircuitOpenError

logger = logging.getLogger(__name__)


async def retry_with_backoff(coro_factory, max_retries: int = 3, base_delay: float = 1.0):
    for attempt in range(max_retries + 1):
        try:
            return await coro_factory()
        except (CircuitOpenError, asyncio.CancelledError):
            raise
        except Exception as e:
            if attempt == max_retries:
                raise
            delay = base_delay * (2 ** attempt) + random.uniform(0, 0.5)
            logger.debug(f"Retry attempt {attempt + 1}, waiting {delay:.1f}s: {e}")
            await asyncio.sleep(delay)


class DatabaseConnection:
    def __init__(self, db_config: DatabaseConfig, cb_config: CircuitBreakerConfig):
        self.db_id = db_config.id
        self.db_config = db_config
        self.pool: Optional[asyncpg.Pool] = None
        self.circuit_breaker = CircuitBreaker(
            name=db_config.id,
            failure_threshold=cb_config.failure_threshold,
            recovery_timeout=cb_config.recovery_timeout,
            success_threshold=cb_config.success_threshold,
        )
        self.pg_version: Optional[int] = None
        self.capabilities: Dict[str, bool] = {}
        self._initialized = False

    async def initialize(self):
        try:
            self.pool = await asyncpg.create_pool(
                host=self.db_config.host,
                port=self.db_config.port,
                database=self.db_config.database,
                user=self.db_config.user,
                password=self.db_config.password,
                ssl="require" if self.db_config.ssl else None,
                min_size=self.db_config.pool.min_size,
                max_size=self.db_config.pool.max_size,
                command_timeout=self.db_config.pool.command_timeout,
                statement_cache_size=0,
            )
            self._initialized = True
            logger.info(f"[{self.db_id}] Connection pool initialized")
        except Exception as e:
            logger.error(f"[{self.db_id}] Failed to create pool: {e}")
            raise

    async def execute_query(
        self, query: str, *args, timeout: float = 10.0
    ) -> List[Dict[str, Any]]:
        if not await self.circuit_breaker.can_execute():
            raise CircuitOpenError(f"Circuit open for {self.db_id}")

        try:
            async with self.pool.acquire(timeout=5.0) as conn:
                rows = await asyncio.wait_for(
                    conn.fetch(query, *args), timeout=timeout
                )
            await self.circuit_breaker.record_success()
            return [dict(row) for row in rows]
        except asyncio.CancelledError:
            raise
        except Exception as e:
            await self.circuit_breaker.record_failure()
            raise

    async def execute_query_with_retry(
        self, query: str, *args, timeout: float = 10.0, max_retries: int = 2
    ) -> List[Dict[str, Any]]:
        return await retry_with_backoff(
            lambda: self.execute_query(query, *args, timeout=timeout),
            max_retries=max_retries,
        )

    async def close(self):
        if self.pool:
            await self.pool.close()
            self.pool = None
            self._initialized = False
            logger.info(f"[{self.db_id}] Connection pool closed")


class ConnectionManager:
    def __init__(self):
        self.connections: Dict[str, DatabaseConnection] = {}

    async def add_database(
        self, db_config: DatabaseConfig, cb_config: CircuitBreakerConfig
    ) -> DatabaseConnection:
        conn = DatabaseConnection(db_config, cb_config)
        await conn.initialize()
        self.connections[db_config.id] = conn
        return conn

    async def remove_database(self, db_id: str):
        if db_id in self.connections:
            await self.connections[db_id].close()
            del self.connections[db_id]

    def get(self, db_id: str) -> Optional[DatabaseConnection]:
        return self.connections.get(db_id)

    def all_ids(self) -> List[str]:
        return list(self.connections.keys())

    async def close_all(self):
        for conn in self.connections.values():
            await conn.close()
        self.connections.clear()
