import time
from typing import Any, Dict

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

router = APIRouter(prefix="/api")

_app_state: Dict[str, Any] = {}


def init_router(store, scheduler):
    _app_state["store"] = store
    _app_state["scheduler"] = scheduler


@router.get("/indexes/{db_id}")
async def get_index_stats(db_id: str):
    store = _app_state["store"]
    stats = await store.get_latest_index_stats(db_id)
    return {"db_id": db_id, "count": len(stats), "indexes": stats}


@router.get("/indexes/{db_id}/recommendations")
async def get_index_recommendations(
    db_id: str,
    category: str = Query(default=""),
    include_dismissed: bool = Query(default=False),
):
    store = _app_state["store"]
    recs = await store.get_index_recommendations(db_id, category, include_dismissed)
    return {"db_id": db_id, "count": len(recs), "recommendations": recs}


@router.post("/indexes/{db_id}/dismiss/{rec_id}")
async def dismiss_recommendation(db_id: str, rec_id: int):
    store = _app_state["store"]
    await store.dismiss_index_recommendation(rec_id)
    return {"status": "ok"}


@router.get("/missing-indexes/{db_id}")
async def get_missing_indexes(db_id: str):
    store = _app_state["store"]
    recs = await store.get_missing_index_recommendations(db_id)
    return {"db_id": db_id, "count": len(recs), "recommendations": recs}
