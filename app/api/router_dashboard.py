import time
from typing import Any, Dict, List

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

router = APIRouter(prefix="/api")

_app_state: Dict[str, Any] = {}


def init_router(conn_manager, scheduler, store, config_manager):
    _app_state["conn_manager"] = conn_manager
    _app_state["scheduler"] = scheduler
    _app_state["store"] = store
    _app_state["config_manager"] = config_manager


@router.get("/databases")
async def list_databases():
    conn_manager = _app_state["conn_manager"]
    config_manager = _app_state["config_manager"]
    databases = []
    for db_config in config_manager.config.databases:
        conn = conn_manager.get(db_config.id)
        status = "disconnected"
        circuit_state = "unknown"
        pg_version = None
        if conn:
            circuit_state = conn.circuit_breaker.state.value
            pg_version = conn.pg_version
            if conn.circuit_breaker.is_open:
                status = "circuit_open"
            elif conn._initialized:
                status = "connected"
        databases.append({
            "id": db_config.id,
            "host": db_config.host,
            "database": db_config.database,
            "status": status,
            "circuit_state": circuit_state,
            "pg_version": pg_version,
        })
    return databases


@router.get("/metrics/{db_id}/current")
async def get_current_metrics(db_id: str):
    scheduler = _app_state["scheduler"]
    metrics = scheduler.latest_metrics.get(db_id)
    if metrics is None:
        return JSONResponse(status_code=404, content={"error": "Database not found"})
    return {"db_id": db_id, "timestamp": time.time(), "data": metrics}


@router.get("/metrics/{db_id}/history")
async def get_metrics_history(
    db_id: str,
    range: str = Query(default="1h", regex="^(1h|6h|24h|7d)$"),
):
    store = _app_state["store"]
    range_map = {"1h": 3600, "6h": 21600, "24h": 86400, "7d": 604800}
    seconds = range_map.get(range, 3600)
    start_time = time.time() - seconds
    history = await store.get_metrics_history(db_id, start_time)

    step = max(1, len(history) // 500)
    if step > 1:
        history = history[::step]

    return {"db_id": db_id, "range": range, "data": history}


@router.get("/metrics/{db_id}/tables")
async def get_top_tables(db_id: str):
    store = _app_state["store"]
    tables = await store.get_latest_top_tables(db_id)
    return {"db_id": db_id, "tables": tables}


@router.get("/status")
async def get_status():
    conn_manager = _app_state["conn_manager"]
    breakers = {}
    for db_id, conn in conn_manager.connections.items():
        breakers[db_id] = conn.circuit_breaker.get_status()
    return {"circuit_breakers": breakers}
