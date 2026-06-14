import asyncio
import json
import logging
from typing import Any, Dict, Set

from fastapi import WebSocket, WebSocketDisconnect

logger = logging.getLogger(__name__)


class WebSocketBroadcaster:
    def __init__(self):
        self._clients: Set[WebSocket] = set()
        self._subscriptions: Dict[WebSocket, Set[str]] = {}
        self._lock = asyncio.Lock()

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        async with self._lock:
            self._clients.add(websocket)
            self._subscriptions[websocket] = set()

    async def disconnect(self, websocket: WebSocket):
        async with self._lock:
            self._clients.discard(websocket)
            self._subscriptions.pop(websocket, None)

    async def subscribe(self, websocket: WebSocket, db_ids: list):
        async with self._lock:
            if websocket in self._subscriptions:
                self._subscriptions[websocket] = set(db_ids)

    async def broadcast(self, db_id: str, metrics: Dict[str, Any]):
        import time

        message = json.dumps({
            "type": "metrics_update",
            "db_id": db_id,
            "timestamp": time.time(),
            "data": metrics,
        })

        async with self._lock:
            stale = []
            for ws in self._clients:
                subs = self._subscriptions.get(ws, set())
                if not subs or db_id in subs:
                    try:
                        await ws.send_text(message)
                    except Exception:
                        stale.append(ws)

            for ws in stale:
                self._clients.discard(ws)
                self._subscriptions.pop(ws, None)

    async def handle_client(self, websocket: WebSocket):
        await self.connect(websocket)
        try:
            while True:
                data = await websocket.receive_text()
                try:
                    msg = json.loads(data)
                    if msg.get("type") == "subscribe":
                        await self.subscribe(websocket, msg.get("db_ids", []))
                except (json.JSONDecodeError, KeyError):
                    pass
        except WebSocketDisconnect:
            pass
        finally:
            await self.disconnect(websocket)
