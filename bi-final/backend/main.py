"""
main.py — B&I Controls Hub Enterprise API v3

Run:
    conda activate prath
    cd bi-final/backend
    uvicorn main:app --host 0.0.0.0 --port 8000 --reload

All packages from prath env:
    fastapi==0.121.1  uvicorn==0.34.0  pydantic==2.12.3
    vertexai==1.43.0  tableauserverclient  oracledb==3.4.0
    python-dotenv==1.2.1  PyYAML==6.0.3  pandas==2.0.3
"""
import logging, time
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from backend.routers.api import router
from backend.context.loader import list_scorecards

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
log = logging.getLogger("main")


@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info("═"*55)
    log.info("B&I Controls Hub — Enterprise API v3")
    log.info(f"Scorecards loaded: {list_scorecards()}")
    log.info("Docs: http://localhost:8000/docs")
    log.info("═"*55)
    yield
    log.info("Shutting down…")


app = FastAPI(
    title="B&I Controls Hub API",
    description="Enterprise B&I Data Metrics and Controls",
    version="3.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST", "DELETE"],
    allow_headers=["Content-Type", "X-User-ID", "X-View-ID"],
)

@app.middleware("http")
async def timing(request: Request, call_next):
    t0 = time.time()
    response = await call_next(request)
    response.headers["X-Response-Ms"] = str(int((time.time()-t0)*1000))
    return response

app.include_router(router)

@app.get("/")
def root():
    return {
        "service": "B&I Controls Hub API v3",
        "docs":    "/docs",
        "health":  "/health/",
        "chat":    "POST /chat/",
        "snapshot":"GET /snapshot/{view_id}",
        "history": "GET /history/sessions",
        "analytics":"GET /analytics/summary",
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
