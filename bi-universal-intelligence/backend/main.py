"""
main.py — B&I Controls Hub Enterprise API v4
Universal Tableau Intelligence System

Run from project root:
    conda activate prath
    uvicorn backend.main:app --host 0.0.0.0 --port 8000 --reload
"""
import logging, time
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager

from backend.routers.api               import router as api_router
from backend.routers.chat_ingestion    import router as chat_router
from backend.routers.dynamic_router    import router as dyn_router
from backend.routers.intelligence_router import router as intel_router
from backend.context.loader import list_scorecards

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
log = logging.getLogger("main")

@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info("═"*60)
    log.info("B&I Controls Hub — Universal Intelligence System v4")
    log.info(f"YAML scorecards: {list_scorecards()}")
    log.info("Key endpoints:")
    log.info("  POST /intelligence/onboard  — zero-config dashboard onboarding")
    log.info("  POST /intelligence/query    — tiered NL query (Tier 1/2/3)")
    log.info("  GET  /intelligence/insights/{id} — proactive anomaly insights")
    log.info("  GET  /intelligence/list     — all onboarded dashboards")
    log.info("  POST /connect               — legacy connect endpoint")
    log.info("  POST /query/{id}            — legacy rule-based query")
    log.info("  POST /chat/                 — LLM chat")
    log.info("  GET  /health/               — service health")
    log.info("  GET  /docs                  — Swagger UI")
    log.info("═"*60)
    yield
    log.info("Shutting down…")

app = FastAPI(
    title       = "B&I Controls Hub — Universal Intelligence API",
    description = "Universal Tableau Intelligence System: any dashboard → intelligent chatbot",
    version     = "4.0.0",
    lifespan    = lifespan,
)

app.add_middleware(CORSMiddleware,
    allow_origins  = ["*"],
    allow_methods  = ["GET","POST","DELETE","OPTIONS"],
    allow_headers  = ["Content-Type","X-User-ID","X-View-ID"],
)

@app.middleware("http")
async def timing(request: Request, call_next):
    t0 = time.time()
    r  = await call_next(request)
    r.headers["X-Response-Ms"] = str(int((time.time()-t0)*1000))
    return r

app.include_router(intel_router)
app.include_router(api_router)
app.include_router(chat_router)
app.include_router(dyn_router)

@app.get("/")
def root():
    return {
        "service": "B&I Controls Hub — Universal Intelligence API v4",
        "docs":    "/docs",
        "key_endpoints": {
            "onboard":  "POST /intelligence/onboard",
            "query":    "POST /intelligence/query",
            "insights": "GET  /intelligence/insights/{id}",
            "list":     "GET  /intelligence/list",
            "health":   "GET  /health/",
        }
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("backend.main:app", host="0.0.0.0", port=8000, reload=True)
