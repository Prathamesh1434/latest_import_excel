"""
routers/dynamic_router.py

Dynamic Dashboard Handling — all endpoints for:
  POST /connect            — connect any Tableau dashboard, auto-analyse
  GET  /schema/{id}        — get auto-inferred schema profile
  GET  /questions/{id}     — get auto-generated questions
  POST /query/{id}         — rule-based answers (no LLM)
  POST /query/{id}/smart   — rule-based first, LLM fallback
  GET  /dashboard/discover — list all workbooks on Tableau site
"""

import io
import uuid
import time
import logging
from typing import Dict, List, Optional

import pandas as pd
from fastapi import APIRouter, HTTPException, Request, Query, BackgroundTasks
from fastapi.responses import JSONResponse

from backend.ingestion.tableau_extractor  import TableauConnection, TableauExtractor, ViewTarget
from backend.ingestion.data_transformer   import DataTransformer
from backend.ingestion.schema_analyser    import SchemaAnalyser
from backend.ingestion.question_generator import DynamicQuestionGenerator
from backend.ingestion.universal_answerer import UniversalAnswerer
from backend.ingestion.context_store      import ContextStore
from backend.ingestion.pipeline           import IngestionPipeline
from backend.config import check_tableau, check_vertex, check_oracle

log    = logging.getLogger("dynamic_router")
router = APIRouter()

# ── Singletons ────────────────────────────────────────────────────────────
_analyser   = SchemaAnalyser()
_generator  = DynamicQuestionGenerator()
_transformer= DataTransformer()
_pipeline: Optional[IngestionPipeline] = None

# In-process schema + df cache (keyed by source_id)
# Maps source_id → { "df": DataFrame, "profile": SchemaProfile, "meta": dict }
_session_cache: Dict[str, Dict] = {}


def _get_pipeline() -> IngestionPipeline:
    global _pipeline
    if not _pipeline:
        oracle_pool = None
        if check_oracle():
            try:
                from backend.services.oracle_service import _get_pool
                oracle_pool = _get_pool()
            except Exception:
                pass
        _pipeline = IngestionPipeline.from_env(oracle_pool=oracle_pool)
    return _pipeline


def _rebuild_df(source_id: str) -> pd.DataFrame:
    """Rebuild DataFrame from cached transformed chunks."""
    entry = _session_cache.get(source_id)
    if entry and "df" in entry:
        return entry["df"]

    pipeline = _get_pipeline()
    dataset  = pipeline.store.load(source_id)
    if not dataset:
        raise HTTPException(404, f"No data for '{source_id}'. Call POST /connect first.")

    row_chunks = [c.content for c in dataset.chunks if c.chunk_type == "rows"]
    if not row_chunks:
        return pd.DataFrame()

    parts = []
    for chunk in row_chunks:
        lines = chunk.split("\n", 1)
        if len(lines) > 1:
            parts.append(lines[1])
    combined = "\n".join(parts)
    df = pd.read_csv(io.StringIO(combined))
    return df


# ═══════════════════════════════════════════════════════════════
# POST /connect
# Connect any Tableau dashboard — extract, analyse, generate questions
# ═══════════════════════════════════════════════════════════════

@router.post("/connect")
async def connect_dashboard(
    body:    dict,
    request: Request,
    bg:      BackgroundTasks = None,
):
    """
    Zero-config dashboard connection.

    Body:
        {
          "source_id":   "my-dashboard",        # your internal ID
          "source_name": "Q1 Sales Dashboard",   # human label
          "view_id":     "c69d5ca6-...",          # Tableau View ID
          "filters":     {}                       # optional action filters
        }

    Returns:
        source_id, dashboard_type, dimensions, measures,
        auto-generated questions, schema summary
    """
    source_id   = body.get("source_id") or str(uuid.uuid4())[:8]
    source_name = body.get("source_name", source_id)
    view_id     = body.get("view_id", "")
    filters     = body.get("filters", {})

    if not view_id and not check_tableau():
        raise HTTPException(400, "Provide view_id and set TABLEAU_* env vars.")

    t0 = time.time()

    # ── Extract DataFrame ────────────────────────────────────────────────
    df = pd.DataFrame()
    if view_id and check_tableau():
        try:
            conn      = TableauConnection.from_env()
            extractor = TableauExtractor(conn)
            target    = ViewTarget(view_id=view_id, scorecard_id=source_id, view_name=source_name)
            df        = extractor.get_dataframe(target, filters=filters or None)
            log.info(f"Extracted {len(df)} rows for {source_id}")
        except Exception as e:
            log.error(f"Tableau extraction failed: {e}")
            raise HTTPException(502, f"Tableau extraction failed: {e}")

    if df.empty:
        raise HTTPException(422, "Extracted data is empty. Check view_id and permissions.")

    # ── Auto-analyse schema ──────────────────────────────────────────────
    profile = _analyser.analyse(df, source_id=source_id, source_name=source_name)

    # ── Generate questions ───────────────────────────────────────────────
    questions = _generator.generate(profile, max_q=8)

    # ── Transform + cache ────────────────────────────────────────────────
    dataset   = _transformer.transform(df, source_id=source_id, source_name=source_name)
    pipeline  = _get_pipeline()
    pipeline.store.save(dataset)

    # Store in-process cache (df + profile)
    _session_cache[source_id] = {
        "df":      df,
        "profile": profile,
        "meta":    {"view_id": view_id, "source_name": source_name, "connected_at": time.strftime("%Y-%m-%d %H:%M:%S")},
    }

    elapsed = round(time.time() - t0, 2)
    log.info(f"connect_dashboard: {source_id} | {profile.dashboard_type} | {elapsed}s")

    return {
        "status":           "connected",
        "source_id":        source_id,
        "source_name":      source_name,
        "dashboard_type":   profile.dashboard_type,
        "confidence":       profile.confidence,
        "total_rows":       profile.total_rows,
        "total_cols":       profile.total_cols,
        "dimensions":       profile.dimensions,
        "measures":         profile.measures,
        "time_cols":        profile.time_cols,
        "status_cols":      profile.status_cols,
        "kpi_count":        len(profile.kpi_patterns),
        "has_time_series":  profile.has_time_series,
        "has_rag_status":   profile.has_rag_status,
        "has_thresholds":   profile.has_thresholds,
        "has_variance":     profile.has_variance,
        "questions":        questions,
        "schema_summary":   profile.schema_summary,
        "elapsed_sec":      elapsed,
    }


# ═══════════════════════════════════════════════════════════════
# GET /schema/{source_id}
# ═══════════════════════════════════════════════════════════════

@router.get("/schema/{source_id}")
def get_schema(source_id: str):
    """Returns the auto-inferred schema profile for a connected dashboard."""
    entry = _session_cache.get(source_id)
    if not entry:
        raise HTTPException(404, f"'{source_id}' not connected. Call POST /connect first.")

    profile = entry["profile"]
    return {
        "source_id":      profile.source_id,
        "source_name":    profile.source_name,
        "dashboard_type": profile.dashboard_type,
        "confidence":     profile.confidence,
        "dimensions":     profile.dimensions,
        "measures":       profile.measures,
        "time_cols":      profile.time_cols,
        "status_cols":    profile.status_cols,
        "id_cols":        profile.id_cols,
        "text_cols":      profile.text_cols,
        "hierarchies":    profile.hierarchies,
        "kpi_patterns": [
            {
                "name":           kp.kpi_name,
                "value_col":      kp.value_col,
                "threshold_col":  kp.threshold_col,
                "previous_col":   kp.previous_col,
                "status_col":     kp.status_col,
                "is_percentage":  kp.is_percentage,
                "breach_detected":kp.breach_detected,
                "breach_count":   kp.breach_count,
            }
            for kp in profile.kpi_patterns
        ],
        "capabilities": {
            "time_series": profile.has_time_series,
            "rag_status":  profile.has_rag_status,
            "thresholds":  profile.has_thresholds,
            "variance":    profile.has_variance,
        },
        "schema_summary": profile.schema_summary,
        "meta":           entry.get("meta", {}),
    }


# ═══════════════════════════════════════════════════════════════
# GET /questions/{source_id}
# ═══════════════════════════════════════════════════════════════

@router.get("/questions/{source_id}")
def get_questions(
    source_id: str,
    max_q:     int  = Query(default=8, ge=1, le=20),
    as_chips:  bool = Query(default=False),
):
    """Returns auto-generated questions for a connected dashboard."""
    entry = _session_cache.get(source_id)
    if not entry:
        raise HTTPException(404, f"'{source_id}' not connected. Call POST /connect first.")

    profile   = entry["profile"]
    questions = _generator.generate(profile, max_q=max_q)

    if as_chips:
        return {"questions": _generator.generate_as_chips(profile, max_q=max_q)}

    return {
        "source_id":      source_id,
        "dashboard_type": profile.dashboard_type,
        "questions":      questions,
        "count":          len(questions),
    }


# ═══════════════════════════════════════════════════════════════
# POST /query/{source_id}  — rule-based, zero LLM
# ═══════════════════════════════════════════════════════════════

@router.post("/query/{source_id}")
def query_rule_based(
    source_id: str,
    body:      dict,
):
    """
    Answer a question using pure pandas — no LLM, no API cost.
    Works on any connected dashboard.

    Body: { "question": "Which KRIs are breaching?" }
    """
    question = body.get("question", "").strip()
    if not question:
        raise HTTPException(400, "Provide 'question' in body.")

    # Get df + profile from cache
    entry = _session_cache.get(source_id)
    if not entry:
        # Try rebuilding from pipeline store
        try:
            df      = _rebuild_df(source_id)
            profile = _analyser.analyse(df, source_id=source_id)
            _session_cache[source_id] = {"df": df, "profile": profile, "meta": {}}
            entry   = _session_cache[source_id]
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(404, f"'{source_id}' not connected. Call POST /connect first.")

    df      = entry["df"]
    profile = entry["profile"]

    t0       = time.time()
    answerer = UniversalAnswerer(df, profile)
    answer   = answerer.answer(question)
    elapsed  = round(time.time() - t0, 3)

    result = answer.to_dict()
    result["question"]   = question
    result["source_id"]  = source_id
    result["elapsed_sec"]= elapsed
    result["row_count"]  = answer.row_count
    return result


# ═══════════════════════════════════════════════════════════════
# POST /query/{source_id}/smart  — rules first, LLM fallback
# ═══════════════════════════════════════════════════════════════

@router.post("/query/{source_id}/smart")
async def query_smart(
    source_id: str,
    body:      dict,
    request:   Request,
):
    """
    Tiered query:
      1. Try rule-based (instant, free)
      2. If confidence < 0.5 → escalate to LLM
    """
    question = body.get("question", "").strip()
    if not question:
        raise HTTPException(400, "Provide 'question' in body.")

    # ── Step 1: Rule-based ───────────────────────────────────────────────
    entry = _session_cache.get(source_id)
    if not entry:
        try:
            df      = _rebuild_df(source_id)
            profile = _analyser.analyse(df, source_id=source_id)
            _session_cache[source_id] = {"df": df, "profile": profile, "meta": {}}
            entry   = _session_cache[source_id]
        except Exception as e:
            raise HTTPException(404, f"'{source_id}' not connected: {e}")

    df      = entry["df"]
    profile = entry["profile"]
    answerer= UniversalAnswerer(df, profile)
    answer  = answerer.answer(question)

    if answer.confidence >= 0.7:
        result = answer.to_dict()
        result.update({"question": question, "source_id": source_id, "escalated_to_llm": False})
        return result

    # ── Step 2: LLM escalation ────────────────────────────────────────────
    if not check_vertex():
        result = answer.to_dict()
        result.update({"question": question, "source_id": source_id, "escalated_to_llm": False,
                       "note": "Low confidence but LLM not configured."})
        return result

    try:
        from backend.services import vertex_service
        from backend.context.loader import build_system_prompt
        from backend.ingestion.pipeline import IngestionPipeline

        pipeline   = _get_pipeline()
        meta       = entry.get("meta", {})
        view_id    = meta.get("view_id", "")
        source_name= meta.get("source_name", source_id)

        # Build context from schema + live data
        schema_ctx = profile.schema_summary
        data_ctx   = df.head(80).to_csv(index=False) if not df.empty else ""

        system = (
            f"You are an AI analyst for {source_name}.\n"
            f"Dashboard type: {profile.dashboard_type}.\n\n"
            f"{schema_ctx}\n\n"
            f"=== DATA SAMPLE ===\n{data_ctx}\n"
            f"=== END DATA ===\n\n"
            "Answer concisely. Return JSON: "
            '{"reply":"...", "chart_type":"bar|line|pie|kpi|table|text", "chart": {...}}'
        )

        reply, chart_raw, in_t, out_t, ms = vertex_service.chat(
            system_prompt = system,
            messages      = [{"role": "user", "content": question}],
            max_tokens    = 800,
        )
        return {
            "reply":             reply,
            "chart":             chart_raw,
            "question":          question,
            "source_id":         source_id,
            "method":            "llm",
            "confidence":        0.9,
            "escalated_to_llm":  True,
            "input_tokens":      in_t,
            "output_tokens":     out_t,
            "response_ms":       ms,
        }
    except Exception as e:
        log.error(f"LLM escalation failed: {e}")
        result = answer.to_dict()
        result.update({"question": question, "source_id": source_id,
                       "escalated_to_llm": False, "llm_error": str(e)})
        return result


# ═══════════════════════════════════════════════════════════════
# GET /dashboard/discover  — list all workbooks on Tableau site
# ═══════════════════════════════════════════════════════════════

@router.get("/dashboard/discover")
def discover_dashboards():
    """Auto-discover all workbooks on the connected Tableau site."""
    if not check_tableau():
        raise HTTPException(503, "Tableau not configured — set TABLEAU_* in .env")
    try:
        conn      = TableauConnection.from_env()
        extractor = TableauExtractor(conn)
        workbooks = extractor.list_workbooks(max_results=200)
        return {"workbooks": workbooks, "count": len(workbooks)}
    except Exception as e:
        raise HTTPException(502, f"Tableau discovery failed: {e}")


@router.get("/dashboard/views/{workbook_id}")
def list_views(workbook_id: str):
    """List all views inside a workbook."""
    if not check_tableau():
        raise HTTPException(503, "Tableau not configured")
    try:
        conn      = TableauConnection.from_env()
        extractor = TableauExtractor(conn)
        views     = extractor.list_views(workbook_id)
        return {"views": views, "count": len(views)}
    except Exception as e:
        raise HTTPException(502, f"Failed to list views: {e}")


# ═══════════════════════════════════════════════════════════════
# GET /connected  — list all currently connected dashboards
# ═══════════════════════════════════════════════════════════════

@router.get("/connected")
def list_connected():
    """List all dashboards connected in this session."""
    return {
        "connected": [
            {
                "source_id":      sid,
                "source_name":    e.get("meta", {}).get("source_name", sid),
                "dashboard_type": e["profile"].dashboard_type if "profile" in e else "unknown",
                "rows":           len(e["df"]) if "df" in e else 0,
                "connected_at":   e.get("meta", {}).get("connected_at", ""),
            }
            for sid, e in _session_cache.items()
        ],
        "count": len(_session_cache),
    }
