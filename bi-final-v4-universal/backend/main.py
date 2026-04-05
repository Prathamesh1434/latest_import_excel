"""
main.py — B&I Controls Hub Enterprise API v3
Run:
    conda activate prath
    cd bi-final/backend
    uvicorn main:app --host 0.0.0.0 --port 8000 --reload
"""
import logging, time
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from backend.routers.api            import router as api_router
from backend.routers.chat_ingestion import router as chat_router
from backend.routers.dynamic_router  import router as dyn_router
from backend.context.loader import list_scorecards

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
log = logging.getLogger("main")

@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info("═"*55)
    log.info("B&I Controls Hub — Enterprise API v3")
    log.info(f"YAML scorecards: {list_scorecards()}")
    log.info("Endpoints:")
    log.info("  POST /connect            — connect any dashboard")
    log.info("  GET  /schema/{id}        — auto-inferred schema")
    log.info("  GET  /questions/{id}     — auto-generated questions")
    log.info("  POST /query/{id}         — rule-based answers")
    log.info("  POST /query/{id}/smart   — rules + LLM fallback")
    log.info("  POST /chat/              — full LLM chat")
    log.info("  POST /chat/live          — LLM + live Tableau data")
    log.info("  GET  /dashboard/discover — list Tableau workbooks")
    log.info("  GET  /health/            — service health")
    log.info("  GET  /docs               — Swagger UI")
    log.info("═"*55)
    yield
    log.info("Shutting down…")

app = FastAPI(
    title="B&I Controls Hub API",
    description="Dynamic Tableau Analytics + AI Chatbot",
    version="3.0.0",
    lifespan=lifespan,
)

app.add_middleware(CORSMiddleware,
    allow_origins=["*"], allow_methods=["GET","POST","DELETE"],
    allow_headers=["Content-Type","X-User-ID","X-View-ID"])

@app.middleware("http")
async def timing(request: Request, call_next):
    t0 = time.time()
    r  = await call_next(request)
    r.headers["X-Response-Ms"] = str(int((time.time()-t0)*1000))
    return r

app.include_router(api_router)
app.include_router(chat_router)
app.include_router(dyn_router)

@app.get("/")
def root():
    return {
        "service": "B&I Controls Hub API v3",
        "docs":    "/docs",
        "key_endpoints": {
            "connect":   "POST /connect",
            "query":     "POST /query/{source_id}",
            "smart":     "POST /query/{source_id}/smart",
            "schema":    "GET /schema/{source_id}",
            "questions": "GET /questions/{source_id}",
            "chat":      "POST /chat/",
            "health":    "GET /health/",
        }
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
