"""
routers/dynamic_dashboard.py

All API endpoints for the dynamic dashboard system.
No hardcoding. Any Tableau dashboard works immediately.

Endpoints:
  POST /dashboards/connect          — connect any Tableau view
  GET  /dashboards/                 — list connected sessions
  GET  /dashboards/{id}             — session details + questions
  POST /dashboards/{id}/query       — query (rules first, AI fallback)
  POST /dashboards/{id}/chat        — full AI chat with live context
  GET  /dashboards/{id}/schema      — inferred schema
  GET  /dashboards/{id}/questions   — suggested questions
  POST /dashboards/{id}/refresh     — force re-extract
  DELETE /dashboards/{id}           — disconnect
  GET  /discover/workbooks          — list Tableau workbooks
  GET  /discover/{workbook_id}/views — list views in workbook
"""

import time, logging, uuid
from typing import Optional, List
from fastapi import APIRouter, HTTPException, Request, Query, BackgroundTasks
from pydantic import BaseModel, Field

from backend.config import check_tableau, check_vertex, VERTEX_MODEL
from backend.ingestion.dynamic_pipeline import DynamicPipeline, DashboardSession, _SESSIONS

log    = logging.getLogger("dynamic_dashboard")
router = APIRouter(prefix="/dashboards", tags=["Dynamic Dashboards"])

# ── Pipeline singleton ──────────────────────────────────────────────────────
_pipeline: Optional[DynamicPipeline] = None

def get_pipeline() -> DynamicPipeline:
    global _pipeline
    if _pipeline is None:
        if not check_tableau():
            raise HTTPException(503, "Tableau not configured. Set TABLEAU_* in .env")
        oracle_pool = None
        try:
            from backend.services.oracle_service import _get_pool
            from backend.config import check_oracle
            if check_oracle():
                oracle_pool = _get_pool()
        except Exception:
            pass
        _pipeline = DynamicPipeline.from_env(oracle_pool=oracle_pool)
        log.info("DynamicPipeline initialised")
    return _pipeline

def get_session(source_id: str) -> DashboardSession:
    pipeline = get_pipeline()
    session  = pipeline.get_session(source_id)
    if not session:
        raise HTTPException(
            404,
            f"Session '{source_id}' not found. "
            f"Call POST /dashboards/connect first with view_id."
        )
    return session


# ── Request / Response models ───────────────────────────────────────────────

class ConnectRequest(BaseModel):
    view_id:      str              = Field(..., description="Tableau View ID")
    source_id:    Optional[str]    = Field(default="", description="Internal ID (auto if blank)")
    source_name:  Optional[str]    = Field(default="", description="Display name (auto if blank)")
    filters:      Optional[dict]   = Field(default=None, description="Tableau action filters")
    force_refresh:bool             = Field(default=False)
    max_questions:int              = Field(default=8, ge=1, le=20)
    run_async:    bool             = Field(default=False, description="Return immediately, extract in background")


class QueryRequest(BaseModel):
    question:  str  = Field(..., min_length=2, description="Natural language question")
    mode:      str  = Field(default="auto", description="auto | rules | ai")
    max_tokens:int  = Field(default=600, ge=50, le=2000)


class ChatRequest(BaseModel):
    messages:     List[dict]       = Field(..., description="[{role,content}...]")
    session_id:   Optional[str]    = Field(default=None)
    user_id:      Optional[str]    = Field(default="ANONYMOUS")
    max_tokens:   int              = Field(default=800)


# ── POST /dashboards/connect ────────────────────────────────────────────────

@router.post("/connect")
async def connect_dashboard(
    req:              ConnectRequest,
    background_tasks: BackgroundTasks,
    request:          Request,
):
    """
    Connect any Tableau dashboard. Extracts data, analyses schema,
    generates questions. Ready to query in < 10 seconds.

    - Provide view_id (from Tableau URL or /discover endpoints)
    - Everything else is automatic
    """
    pipeline  = get_pipeline()
    source_id = req.source_id or ("dash-" + req.view_id.replace("-","")[:12])

    if req.run_async:
        # Return immediately — extraction runs in background
        session_id = str(uuid.uuid4())
        def _bg():
            try:
                pipeline.connect(
                    view_id       = req.view_id,
                    source_id     = source_id,
                    source_name   = req.source_name or "",
                    filters       = req.filters,
                    force_refresh = req.force_refresh,
                    max_questions = req.max_questions,
                )
            except Exception as e:
                log.error(f"Background connect failed: {e}")
        background_tasks.add_task(_bg)
        return {
            "status":    "connecting",
            "source_id": source_id,
            "message":   f"Extracting in background. Poll GET /dashboards/{source_id} to check status.",
        }

    # Synchronous
    t0 = time.time()
    try:
        session = pipeline.connect(
            view_id       = req.view_id,
            source_id     = source_id,
            source_name   = req.source_name or "",
            filters       = req.filters,
            force_refresh = req.force_refresh,
            max_questions = req.max_questions,
        )
    except Exception as e:
        raise HTTPException(500, f"Connection failed: {e}")

    if not session.ready:
        raise HTTPException(500, f"Session not ready: {session.schema_summary}")

    return {
        "status":           "connected",
        "session_id":       session.session_id,
        "source_id":        session.source_id,
        "source_name":      session.source_name,
        "dashboard_type":   session.dashboard_type,
        "total_rows":       session.total_rows,
        "total_cols":       session.total_cols,
        "extraction_ms":    session.extraction_ms,
        "suggested_questions": session.suggested_questions,
        "capabilities":     session.to_dict()["capabilities"],
        "next_step":        f"POST /dashboards/{session.source_id}/query?question=..."
    }


# ── GET /dashboards/ ─────────────────────────────────────────────────────────

@router.get("/")
def list_dashboards():
    """List all currently connected dashboard sessions."""
    pipeline = get_pipeline()
    return {
        "sessions": pipeline.list_sessions(),
        "count":    len(pipeline.list_sessions()),
    }


# ── GET /dashboards/{source_id} ───────────────────────────────────────────────

@router.get("/{source_id}")
def get_dashboard(source_id: str):
    """Get session details, schema, and suggested questions."""
    session = get_session(source_id)
    return session.to_dict()


# ── GET /dashboards/{source_id}/schema ────────────────────────────────────────

@router.get("/{source_id}/schema")
def get_schema(source_id: str):
    """Detailed inferred schema for this dashboard."""
    session = get_session(source_id)
    if not session.profile:
        raise HTTPException(503, "Schema not yet computed.")
    p = session.profile
    return {
        "source_id":        p.source_id,
        "dashboard_type":   p.dashboard_type,
        "confidence":       p.confidence,
        "dimensions":       p.dimensions,
        "measures":         p.measures,
        "time_cols":        p.time_cols,
        "status_cols":      p.status_cols,
        "hierarchies":      p.hierarchies,
        "kpi_patterns": [
            {
                "kpi_name":       kp.kpi_name,
                "value_col":      kp.value_col,
                "threshold_col":  kp.threshold_col,
                "previous_col":   kp.previous_col,
                "status_col":     kp.status_col,
                "is_percentage":  kp.is_percentage,
                "breach_detected":kp.breach_detected,
                "breach_count":   kp.breach_count,
            }
            for kp in p.kpi_patterns
        ],
        "capabilities": {
            "has_time_series": p.has_time_series,
            "has_rag_status":  p.has_rag_status,
            "has_thresholds":  p.has_thresholds,
            "has_variance":    p.has_variance,
        },
        "schema_summary": p.schema_summary,
    }


# ── GET /dashboards/{source_id}/questions ─────────────────────────────────────

@router.get("/{source_id}/questions")
def get_questions(source_id: str, max_q: int = Query(default=8, ge=1, le=20)):
    """Get auto-generated sample questions for this dashboard."""
    session = get_session(source_id)
    if not session.profile:
        raise HTTPException(503, "Schema not yet computed.")

    from backend.ingestion.question_generator import DynamicQuestionGenerator
    generator = DynamicQuestionGenerator()
    questions = generator.generate(session.profile, max_q=max_q)

    return {
        "source_id":   source_id,
        "source_name": session.source_name,
        "dashboard_type": session.dashboard_type,
        "questions":   questions,
        "chips":       [{"text": q, "source_id": source_id} for q in questions],
    }


# ── POST /dashboards/{source_id}/query ────────────────────────────────────────

@router.post("/{source_id}/query")
async def query_dashboard(source_id: str, req: QueryRequest):
    """
    Intelligent query — tries rules first, AI fallback only if needed.

    mode=auto   → rules first, then AI if needed (recommended)
    mode=rules  → pandas only, instant, no API cost
    mode=ai     → always use AI (costs tokens)
    """
    session = get_session(source_id)
    if not session.ready:
        raise HTTPException(503, f"Session not ready: {session.schema_summary}")

    question = req.question
    t0       = time.time()

    # ── Rules-first (auto or rules mode) ─────────────────────────────────
    if req.mode in ("auto", "rules"):
        direct = session.answer_directly(question)
        if direct:
            return {
                "source_id":   source_id,
                "question":    question,
                "answer_mode": "rules",   # no AI used
                "confidence":  direct.confidence,
                "reply":       direct.reply,
                "chart":       direct.chart,
                "query_type":  direct.query_type,
                "pandas_op":   direct.sql_like,
                "response_ms": int((time.time()-t0)*1000),
                "ai_used":     False,
            }

    # ── AI fallback ───────────────────────────────────────────────────────
    if req.mode == "rules":
        return {
            "source_id":   source_id,
            "question":    question,
            "answer_mode": "rules_unmatched",
            "reply":       "Could not answer with rules alone. Use mode=auto or mode=ai.",
            "chart":       None,
            "ai_used":     False,
            "response_ms": int((time.time()-t0)*1000),
        }

    if not check_vertex():
        raise HTTPException(503, "Vertex AI not configured. Use mode=rules or set GOOGLE_PROJECT_ID.")

    from backend.services  import vertex_service
    from backend.context.loader import build_system_prompt

    system_prompt = (
        f"You are an AI analyst for the dashboard: {session.source_name}.\n"
        f"Dashboard type: {session.dashboard_type}.\n\n"
        f"{session.llm_context}\n\n"
        "Answer ONLY based on the data provided. Be concise. "
        "Respond in JSON: {reply, chart_type, chart}"
    )

    try:
        reply, chart_raw, in_tok, out_tok, resp_ms = vertex_service.chat(
            system_prompt = system_prompt,
            messages      = [{"role":"user","content":question}],
            max_tokens    = req.max_tokens,
        )
        chart = None
        if chart_raw and isinstance(chart_raw, dict):
            from backend.models.schemas import ChartData
            try:
                chart = ChartData(**{
                    "chart_type": chart_raw.get("chart_type","text"),
                    "title":      chart_raw.get("title",""),
                    "subtitle":   chart_raw.get("subtitle",""),
                    "labels":     chart_raw.get("labels"),
                    "datasets":   chart_raw.get("datasets"),
                    "kpis":       chart_raw.get("kpis"),
                    "table_cols": chart_raw.get("columns"),
                    "table_rows": chart_raw.get("rows"),
                    "colors":     chart_raw.get("colors"),
                }).dict()
            except Exception:
                chart = chart_raw

        return {
            "source_id":   source_id,
            "question":    question,
            "answer_mode": "ai",
            "reply":       reply,
            "chart":       chart,
            "ai_used":     True,
            "model":       VERTEX_MODEL,
            "input_tokens":in_tok,
            "output_tokens":out_tok,
            "response_ms": resp_ms,
        }
    except Exception as e:
        raise HTTPException(500, f"AI query failed: {e}")


# ── POST /dashboards/{source_id}/chat ─────────────────────────────────────────

@router.post("/{source_id}/chat")
async def chat_dashboard(source_id: str, req: ChatRequest):
    """
    Full multi-turn AI chat with live dashboard context.
    Maintains conversation history. Uses Vertex AI Gemini.
    """
    session = get_session(source_id)
    if not session.ready:
        raise HTTPException(503, "Session not ready.")
    if not check_vertex():
        raise HTTPException(503, "Vertex AI not configured.")

    from backend.services import vertex_service

    system_prompt = (
        f"You are an expert AI analyst for {session.source_name}.\n"
        f"Dashboard type: {session.dashboard_type}.\n\n"
        f"{session.llm_context}\n\n"
        "Rules:\n"
        "1. Answer ONLY about this dashboard data.\n"
        "2. Use specific values from the data — never make up numbers.\n"
        "3. Respond in JSON: {reply, chart_type, chart}\n"
        "4. Be concise in the reply — the chart shows the detail."
    )

    try:
        reply, chart_raw, in_tok, out_tok, resp_ms = vertex_service.chat(
            system_prompt = system_prompt,
            messages      = req.messages,
            max_tokens    = req.max_tokens,
        )
        return {
            "source_id":    source_id,
            "source_name":  session.source_name,
            "reply":        reply,
            "chart":        chart_raw,
            "model":        VERTEX_MODEL,
            "input_tokens": in_tok,
            "output_tokens":out_tok,
            "response_ms":  resp_ms,
        }
    except Exception as e:
        raise HTTPException(500, f"Chat failed: {e}")


# ── POST /dashboards/{source_id}/refresh ──────────────────────────────────────

@router.post("/{source_id}/refresh")
async def refresh_dashboard(source_id: str, background_tasks: BackgroundTasks):
    """Force re-extraction from Tableau."""
    session = get_session(source_id)
    pipeline = get_pipeline()

    def _refresh():
        pipeline.connect(
            view_id       = session.view_id,
            source_id     = source_id,
            source_name   = session.source_name,
            force_refresh = True,
        )
    background_tasks.add_task(_refresh)
    return {"status": "refreshing", "source_id": source_id}


# ── DELETE /dashboards/{source_id} ────────────────────────────────────────────

@router.delete("/{source_id}")
def disconnect_dashboard(source_id: str):
    """Disconnect dashboard and clear cache."""
    pipeline = get_pipeline()
    pipeline.disconnect(source_id)
    return {"status": "disconnected", "source_id": source_id}


# ── Discovery endpoints ────────────────────────────────────────────────────────

discover_router = APIRouter(prefix="/discover", tags=["Discovery"])

@discover_router.get("/workbooks")
def discover_workbooks():
    """List all Tableau workbooks on the site."""
    return {"workbooks": get_pipeline().discover_workbooks()}

@discover_router.get("/{workbook_id}/views")
def discover_views(workbook_id: str):
    """List all views in a workbook."""
    return {"views": get_pipeline().discover_views(workbook_id)}
