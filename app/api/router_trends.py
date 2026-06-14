import time
from typing import Any, Dict

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

router = APIRouter(prefix="/api")

_app_state: Dict[str, Any] = {}


def init_router(store):
    _app_state["store"] = store


def _parse_range(range_str: str) -> int:
    range_map = {"24h": 86400, "7d": 604800, "30d": 2592000}
    return range_map.get(range_str, 86400)


@router.get("/trends/{db_id}")
async def get_trends(
    db_id: str,
    range: str = Query(default="24h", regex="^(24h|7d|30d)$"),
    fingerprint: str = Query(default=""),
):
    store = _app_state["store"]
    seconds = _parse_range(range)
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
    seconds = _parse_range(range)
    end_time = time.time()
    start_time = end_time - seconds

    top = await store.get_top_query_patterns(db_id, start_time, end_time, limit)
    return {"db_id": db_id, "range": range, "patterns": top}


@router.get("/trends/{db_id}/by-user")
async def get_trends_by_user(
    db_id: str,
    range: str = Query(default="24h", regex="^(24h|7d|30d)$"),
    limit: int = Query(default=20, ge=1, le=100),
):
    """Drill down by user — shows total occurrences and time per user."""
    store = _app_state["store"]
    seconds = _parse_range(range)
    end_time = time.time()
    start_time = end_time - seconds

    data = await store.get_trends_by_user(db_id, start_time, end_time, limit)
    return {"db_id": db_id, "range": range, "dimension": "user", "data": data}


@router.get("/trends/{db_id}/by-client")
async def get_trends_by_client(
    db_id: str,
    range: str = Query(default="24h", regex="^(24h|7d|30d)$"),
    top_n: int = Query(default=20, ge=5, le=100),
):
    """Drill down by client IP — high-cardinality IPs are collapsed into 'others'."""
    store = _app_state["store"]
    seconds = _parse_range(range)
    end_time = time.time()
    start_time = end_time - seconds

    data = await store.get_trends_by_client(db_id, start_time, end_time, top_n)
    return {"db_id": db_id, "range": range, "dimension": "client", "data": data}


@router.get("/trends/{db_id}/drilldown")
async def get_trend_drilldown(
    db_id: str,
    dimension: str = Query(..., regex="^(fingerprint|user|client)$"),
    value: str = Query(..., min_length=1),
    range: str = Query(default="24h", regex="^(24h|7d|30d)$"),
):
    """Get hourly time series for a specific dimension value.
    - dimension=fingerprint&value=<fp>: time series for that query fingerprint
    - dimension=user&value=<username>: time series for that user
    - dimension=client&value=<ip>: time series for that client IP
    """
    store = _app_state["store"]
    seconds = _parse_range(range)
    end_time = time.time()
    start_time = end_time - seconds

    if dimension == "fingerprint":
        data = await store.get_slow_query_trends(db_id, start_time, end_time, value)
    elif dimension == "user":
        data = await store.get_trend_timeseries_by_user(db_id, start_time, end_time, value)
    elif dimension == "client":
        data = await store.get_trend_timeseries_by_client(db_id, start_time, end_time, value)
    else:
        return JSONResponse(status_code=400, content={"error": "Invalid dimension"})

    return {
        "db_id": db_id, "range": range,
        "dimension": dimension, "value": value,
        "data": data,
    }


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
