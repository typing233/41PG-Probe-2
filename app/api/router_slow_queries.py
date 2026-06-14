import time
from typing import Optional

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

router = APIRouter(prefix="/api")

_app_state = {}


def init_router(store):
    _app_state["store"] = store


@router.get("/slow-queries/{db_id}")
async def get_slow_queries(
    db_id: str,
    window: str = Query(default="1h", regex="^(1h|12h|24h)$"),
    min_duration: float = Query(default=0, ge=0),
    search: str = Query(default=""),
    limit: int = Query(default=200, le=1000),
):
    store = _app_state["store"]
    window_map = {"1h": 3600, "12h": 43200, "24h": 86400}
    seconds = window_map.get(window, 3600)

    queries = await store.get_slow_queries(
        db_id=db_id,
        window_seconds=seconds,
        min_duration=min_duration,
        search=search,
        limit=limit,
    )
    return {"db_id": db_id, "window": window, "count": len(queries), "queries": queries}
