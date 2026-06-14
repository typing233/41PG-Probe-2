import time
from typing import Any, Dict

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

router = APIRouter(prefix="/api")

_app_state: Dict[str, Any] = {}


def init_router(store):
    _app_state["store"] = store


@router.get("/trends/{db_id}")
async def get_trends(
    db_id: str,
    range: str = Query(default="24h", regex="^(24h|7d|30d)$"),
    fingerprint: str = Query(default=""),
):
    store = _app_state["store"]
    range_map = {"24h": 86400, "7d": 604800, "30d": 2592000}
    seconds = range_map.get(range, 86400)
    end_time = time.time()
    start_time = end_time - seconds

    trends = await store.get_slow_query_trends(db_id, start_time, end_time, fingerprint)
    return {"db_id": db_id, "range": range, "data": trends}


@router.get("/trends/{db_id}/top")
async def get_top_patterns(
    db_id: str,
    range: str = Query(default="24h", regex="^(24h|7d|30d)$"),
    limit: int = Query(default=20, ge=1, le=100),
):
    store = _app_state["store"]
    range_map = {"24h": 86400, "7d": 604800, "30d": 2592000}
    seconds = range_map.get(range, 86400)
    end_time = time.time()
    start_time = end_time - seconds

    top = await store.get_top_query_patterns(db_id, start_time, end_time, limit)
    return {"db_id": db_id, "range": range, "patterns": top}


@router.get("/trends/{db_id}/compare")
async def compare_ranges(
    db_id: str,
    range1_start: float = Query(...),
    range1_end: float = Query(...),
    range2_start: float = Query(...),
    range2_end: float = Query(...),
):
    store = _app_state["store"]
    data1 = await store.get_top_query_patterns(db_id, range1_start, range1_end, 20)
    data2 = await store.get_top_query_patterns(db_id, range2_start, range2_end, 20)
    return {
        "db_id": db_id,
        "range1": {"start": range1_start, "end": range1_end, "patterns": data1},
        "range2": {"start": range2_start, "end": range2_end, "patterns": data2},
    }
