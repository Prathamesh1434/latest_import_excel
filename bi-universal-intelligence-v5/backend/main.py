"""
main.py — B&I Universal Intelligence API v5

CHANGES v5 (6 Apr 2026):
  - setup_logging() called FIRST — all logs now visible in uvicorn terminal
  - Startup diagnostic block — shows exactly what's configured
  - SSE endpoint for real-time onboard progress
  - Removed all Workstream2 references
  - Health endpoint version bumped to 5.0.0

Run from project root:
    conda activate prath
    uvicorn backend.main:app --host 0.0.0.0 --port 8000 --reload
"""

# ── LOGGING MUST BE FIRST ──────────────────────────────────────────────
from backend.config import setup_logging
setup_logging()
# ── Now all subsequent imports will log correctly ──────────────────────

import logging
import time
import json
import asyncio
from collections import defaultdict
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from contextlib import asynccontextmanager

from backend.routers.api                import router as api_router
from backend.routers.chat_ingestion     import router as chat_router
from backend.routers.dynamic_router     import router as dyn_router
from backend.routers.intelligence_router import router as intel_router
from backend.context.loader import list_scorecards
from backend.config import (
    env_diagnostic, check_tableau, check_vertex, check_oracle,
    TABLEAU_SERVER, TABLEAU_USERNAME, TABLEAU_SITE,
)

log = logging.getLogger("main")


# ── SSE Progress Bus ───────────────────────────────────────────────────
# intelligence_router publishes progress events here;
# the /progress/{source_id} SSE endpoint streams them to the frontend.
_progress_queues: dict[str, list[asyncio.Queue]] = defaultdict(list)

def publish_progress(source_id: str, step: int, total: int, label: str, status: str = "running"):
    """Called by intelligence_router to broadcast onboard progress."""
    event = {
        "step": step, "total": total, "label": label,
        "status": status, "ts": time.strftime("%H:%M:%S"),
    }
    for q in _progress_queues.get(source_id, []):
        try:
            q.put_nowait(event)
        except asyncio.QueueFull:
            pass


@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info("═" * 65)
    log.info("B&I Universal Intelligence Hub — API v5")
    log.info("═" * 65)

    # ── Diagnostic block — tells us EXACTLY what's configured ──────────
    diag = env_diagnostic()
    log.info("┌─ ENVIRONMENT DIAGNOSTIC ─────────────────────────────────")
    log.info(f"│ Tableau : {'✓ READY' if diag['tableau']['configured'] else '✗ NOT CONFIGURED'}"
             f"  server={diag['tableau']['server']}  user={diag['tableau']['user']}"
             f"  site={diag['tableau']['site']}  api={diag['tableau']['api_ver']}")
    if diag['tableau']['configured']:
        log.info(f"│          ssl_cert={'✓ found' if diag['tableau']['ssl_exists'] else '✗ MISSING' if diag['tableau']['ssl_cert'] else 'disabled'}"
                 f"  timeouts=auth:{diag['tableau']['timeouts']['auth_s']}s"
                 f" csv:{diag['tableau']['timeouts']['csv_s']}s"
                 f" rest:{diag['tableau']['timeouts']['rest_s']}s")
    log.info(f"│ Vertex  : {'✓ READY' if diag['vertex']['configured'] else '✗ NOT CONFIGURED'}"
             f"  project={diag['vertex']['project']}  model={diag['vertex']['model']}")
    log.info(f"│ Oracle  : {'✓ READY' if diag['oracle']['configured'] else '✗ NOT CONFIGURED'}"
             f"  dsn={diag['oracle']['dsn']}  pool={diag['oracle']['pool']}")
    log.info(f"│ YAML    : {list_scorecards()}")
    log.info("└──────────────────────────────────────────────────────────")

    # ── Key endpoints ──────────────────────────────────────────────────
    log.info("Key endpoints:")
    log.info("  POST /intelligence/onboard      — zero-config dashboard onboarding")
    log.info("  POST /intelligence/query         — tiered NL query (T1/T2/T3)")
    log.info("  GET  /intelligence/insights/{id} — proactive anomaly insights")
    log.info("  GET  /intelligence/list          — all onboarded dashboards")
    log.info("  GET  /progress/{source_id}       — SSE onboard progress stream")
    log.info("  POST /chat/                      — LLM chat (legacy)")
    log.info("  GET  /health/                    — service health")
    log.info("  GET  /docs                       — Swagger UI")
    log.info("═" * 65)

    yield

    log.info("Shutting down…")


app = FastAPI(
    title       = "B&I Universal Intelligence Hub API",
    description = "Universal Tableau Intelligence: any dashboard → intelligent AI chat",
    version     = "5.0.0",
    lifespan    = lifespan,
)

app.add_middleware(CORSMiddleware,
    allow_origins  = ["*"],
    allow_methods  = ["GET", "POST", "DELETE", "OPTIONS"],
    allow_headers  = ["Content-Type", "X-User-ID", "X-View-ID"],
)


# ── Request timing middleware ──────────────────────────────────────────
@app.middleware("http")
async def timing_middleware(request: Request, call_next):
    t0 = time.time()
    response = await call_next(request)
    ms = int((time.time() - t0) * 1000)
    response.headers["X-Response-Ms"] = str(ms)
    # Log slow requests
    if ms > 5000:
        log.warning(f"SLOW {request.method} {request.url.path} → {ms}ms")
    return response


# ── SSE Progress Endpoint ──────────────────────────────────────────────
@app.get("/progress/{source_id}")
async def progress_stream(source_id: str):
    """
    Server-Sent Events stream for real-time onboard progress.
    Frontend connects here BEFORE calling POST /intelligence/onboard.
    Each event: {step, total, label, status, ts}
    """
    queue: asyncio.Queue = asyncio.Queue(maxsize=50)
    _progress_queues[source_id].append(queue)

    async def event_generator():
        try:
            while True:
                try:
                    event = await asyncio.wait_for(queue.get(), timeout=120)
                    yield f"data: {json.dumps(event)}\n\n"
                    if event.get("status") in ("done", "error"):
                        break
                except asyncio.TimeoutError:
                    yield f"data: {json.dumps({'status': 'timeout', 'label': 'Connection timed out'})}\n\n"
                    break
        finally:
            _progress_queues[source_id].remove(queue)
            if not _progress_queues[source_id]:
                del _progress_queues[source_id]

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


# ── Mount routers ──────────────────────────────────────────────────────
app.include_router(intel_router)
app.include_router(api_router)
app.include_router(chat_router)
app.include_router(dyn_router)


@app.get("/")
def root():
    return {
        "service": "B&I Universal Intelligence Hub API v5",
        "docs":    "/docs",
        "key_endpoints": {
            "onboard":   "POST /intelligence/onboard",
            "query":     "POST /intelligence/query",
            "insights":  "GET  /intelligence/insights/{id}",
            "list":      "GET  /intelligence/list",
            "progress":  "GET  /progress/{source_id}  (SSE)",
            "health":    "GET  /health/",
        },
    }
