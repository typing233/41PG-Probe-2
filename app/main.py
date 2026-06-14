import asyncio
import logging
import threading
import time
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, WebSocket, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

from app.config import ConfigManager, AppConfig
from app.database.connection_manager import ConnectionManager
from app.database.version_detector import VersionDetector
from app.database.sqlite_store import SQLiteStore
from app.collector.scheduler import CollectorScheduler
from app.api.websocket_handler import WebSocketBroadcaster
from app.api import router_dashboard, router_slow_queries

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


class ConfigFileHandler(FileSystemEventHandler):
    def __init__(self, config_manager: ConfigManager, loop: asyncio.AbstractEventLoop, coro_factory):
        self._config_manager = config_manager
        self._loop = loop
        self._coro_factory = coro_factory
        self._debounce_timer = None
        self._lock = threading.Lock()

    def on_modified(self, event):
        if not event.src_path.endswith("config.yaml"):
            return
        with self._lock:
            if self._debounce_timer:
                self._debounce_timer.cancel()
            self._debounce_timer = threading.Timer(1.5, self._do_reload)
            self._debounce_timer.start()

    def _do_reload(self):
        logger.info("Config file changed, reloading...")
        new_config = self._config_manager.reload()
        if new_config:
            asyncio.run_coroutine_threadsafe(self._coro_factory(new_config), self._loop)


conn_manager = ConnectionManager()
config_manager = ConfigManager()
store = SQLiteStore(config_manager.config.sqlite.path)
broadcaster = WebSocketBroadcaster()
scheduler: CollectorScheduler = None
_observer: Observer = None


async def _on_config_reload(new_config: AppConfig):
    global scheduler

    current_ids = set(conn_manager.all_ids())
    new_ids = {db.id for db in new_config.databases}

    # Add new databases
    for db_config in new_config.databases:
        if db_config.id not in current_ids:
            logger.info(f"[hot-reload] Adding database: {db_config.id}")
            try:
                conn = await conn_manager.add_database(db_config, new_config.circuit_breaker)
                detector = VersionDetector()
                await detector.detect(conn)
            except Exception:
                conn = conn_manager.register_database(db_config, new_config.circuit_breaker)
            await scheduler.add_database(db_config.id, conn)

    # Remove deleted databases
    for db_id in current_ids - new_ids:
        logger.info(f"[hot-reload] Removing database: {db_id}")
        await scheduler.remove_database(db_id)
        await conn_manager.remove_database(db_id)

    # Update scheduler config (intervals, thresholds, retention)
    scheduler.update_config(new_config)
    logger.info("[hot-reload] Configuration applied successfully")


@asynccontextmanager
async def lifespan(app: FastAPI):
    global scheduler, _observer

    await store.initialize()

    detector = VersionDetector()
    for db_config in config_manager.config.databases:
        try:
            conn = await conn_manager.add_database(db_config, config_manager.config.circuit_breaker)
            await detector.detect(conn)
            logger.info(f"[{db_config.id}] PG{conn.pg_version}, capabilities: {conn.capabilities}")
        except Exception as e:
            logger.error(f"Failed to connect to {db_config.id}: {e} (will retry in background)")
            conn = conn_manager.register_database(db_config, config_manager.config.circuit_breaker)

    scheduler = CollectorScheduler(
        conn_manager=conn_manager,
        store=store,
        config=config_manager.config,
        broadcast_callback=broadcaster.broadcast,
    )
    await scheduler.start()

    loop = asyncio.get_running_loop()
    config_handler = ConfigFileHandler(config_manager, loop, _on_config_reload)
    _observer = Observer()
    _observer.schedule(config_handler, path=".", recursive=False)
    _observer.start()

    router_dashboard.init_router(conn_manager, scheduler, store, config_manager)
    router_slow_queries.init_router(store)

    logger.info("PG-Probe started successfully")
    yield

    logger.info("Shutting down PG-Probe...")
    if _observer:
        _observer.stop()
        _observer.join(timeout=5)
    await scheduler.stop()
    await conn_manager.close_all()
    await store.close()


app = FastAPI(title="PG-Probe", lifespan=lifespan)

BASE_DIR = Path(__file__).resolve().parent.parent
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")
templates = Jinja2Templates(directory=str(Path(__file__).resolve().parent / "templates"))

app.include_router(router_dashboard.router)
app.include_router(router_slow_queries.router)


@app.get("/", response_class=RedirectResponse)
async def root():
    return RedirectResponse(url="/dashboard")


@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard_page(request: Request):
    return templates.TemplateResponse("dashboard.html", {"request": request, "active_page": "dashboard"})


@app.get("/slow-queries", response_class=HTMLResponse)
async def slow_queries_page(request: Request):
    return templates.TemplateResponse("slow_queries.html", {"request": request, "active_page": "slow_queries"})


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await broadcaster.handle_client(websocket)
