import json
import time
from typing import Any, Dict

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

router = APIRouter(prefix="/api")

_app_state: Dict[str, Any] = {}


def init_router(store, scheduler):
    _app_state["store"] = store
    _app_state["scheduler"] = scheduler


@router.get("/health/{db_id}/current")
async def get_current_health(db_id: str):
    store = _app_state["store"]
    score = await store.get_latest_health_score(db_id)
    if not score:
        return JSONResponse(status_code=404, content={"error": "No health data yet"})

    result = dict(score)
    if isinstance(result.get("dimension_scores"), str):
        result["dimension_scores"] = json.loads(result["dimension_scores"])
    if isinstance(result.get("anomalies"), str):
        result["anomalies"] = json.loads(result["anomalies"])
    return {"db_id": db_id, "health": result}


@router.get("/health/{db_id}/history")
async def get_health_history(
    db_id: str,
    range: str = Query(default="24h", regex="^(1h|6h|24h|7d)$"),
):
    store = _app_state["store"]
    range_map = {"1h": 3600, "6h": 21600, "24h": 86400, "7d": 604800}
    seconds = range_map.get(range, 86400)
    start_time = time.time() - seconds

    history = await store.get_health_history(db_id, start_time)
    for h in history:
        if isinstance(h.get("dimension_scores"), str):
            h["dimension_scores"] = json.loads(h["dimension_scores"])
        if isinstance(h.get("anomalies"), str):
            h["anomalies"] = json.loads(h["anomalies"])

    step = max(1, len(history) // 500)
    if step > 1:
        history = history[::step]

    return {"db_id": db_id, "range": range, "data": history}


@router.get("/health/{db_id}/alerts")
async def get_alerts(
    db_id: str,
    severity: str = Query(default=""),
    acknowledged: str = Query(default=""),
    limit: int = Query(default=100, ge=1, le=500),
):
    store = _app_state["store"]
    ack = None
    if acknowledged == "true":
        ack = True
    elif acknowledged == "false":
        ack = False
    alerts = await store.get_alerts(db_id, severity, ack, limit)
    return {"db_id": db_id, "count": len(alerts), "alerts": alerts}


@router.post("/health/{db_id}/alerts/{alert_id}/acknowledge")
async def acknowledge_alert(db_id: str, alert_id: int):
    store = _app_state["store"]
    await store.acknowledge_alert(alert_id)
    return {"status": "ok"}
