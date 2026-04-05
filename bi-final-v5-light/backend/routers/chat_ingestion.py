"""
routers/chat_with_ingestion.py

Extended chat router that uses the full ingestion pipeline.
Drop-in replacement for the /chat/ endpoint in api.py.

Key difference from original:
  - Fetches LIVE data from Tableau CSV (not just YAML)
  - Transforms it into LLM-ready context via IngestionPipeline
  - Caches in Oracle so subsequent questions are instant
  - Still falls back to YAML if view_id not provided
"""

import time
import logging
from fastapi import APIRouter, HTTPException, Request, Query, BackgroundTasks
from backend.models.schemas import ChatRequest, ChatResponse, ChartData
from backend.services       import vertex_service, oracle_service
from backend.context.loader import build_system_prompt, list_scorecards
from backend.config         import check_vertex, check_tableau, check_oracle, MAX_HISTORY

# Import the new ingestion layer
from backend.ingestion import IngestionPipeline

log    = logging.getLogger("chat_ingestion")
router = APIRouter()

# ── Pipeline singleton (initialised once at startup) ─────────────────────
_pipeline: IngestionPipeline = None

def get_pipeline() -> IngestionPipeline:
    global _pipeline
    if _pipeline is None:
        oracle_pool = None
        if check_oracle():
            try:
                from backend.services.oracle_service import _get_pool
                oracle_pool = _get_pool()
            except Exception:
                pass
        _pipeline = IngestionPipeline.from_env(oracle_pool=oracle_pool)
        log.info("IngestionPipeline initialised")
    return _pipeline


def uid(request: Request) -> str:
    return request.headers.get("X-User-ID", "ANONYMOUS")


# ── /ingest — trigger data extraction for a scorecard ─────────────────────
@router.post("/ingest/{source_id}")
async def ingest_scorecard(
    source_id:     str,
    request:       Request,
    view_id:       str = Query(..., description="Tableau View ID"),
    source_name:   str = Query(default=""),
    force_refresh: bool= Query(default=False),
    background_tasks: BackgroundTasks = None,
):
    """
    Trigger data extraction from Tableau for a scorecard.
    Stores transformed data in Oracle cache.
    Subsequent /chat/ calls use this cached data automatically.

    This replaces manually updating YAML files.
    """
    if not check_tableau():
        raise HTTPException(503, "Tableau not configured — set TABLEAU_* in .env")

    pipeline = get_pipeline()

    # Run in background for large datasets
    def _run():
        try:
            dataset = pipeline.ingest(
                source_id    = source_id,
                view_id      = view_id,
                source_name  = source_name or source_id,
                force_refresh= force_refresh,
            )
            log.info(f"Background ingest complete: {source_id} — {dataset.total_rows} rows")
        except Exception as e:
            log.error(f"Background ingest failed: {source_id}: {e}")

    if background_tasks:
        background_tasks.add_task(_run)
        return {
            "status":    "ingestion_queued",
            "source_id": source_id,
            "view_id":   view_id,
            "message":   "Extraction running in background. Use /cache/status to check."
        }

    # Synchronous ingest
    t0 = time.time()
    try:
        dataset = pipeline.ingest(
            source_id    = source_id,
            view_id      = view_id,
            source_name  = source_name or source_id,
            force_refresh= force_refresh,
        )
        return {
            "status":      "ok",
            "source_id":   source_id,
            "rows":        dataset.total_rows,
            "columns":     dataset.total_cols,
            "chunks":      len(dataset.chunks),
            "total_tokens":dataset.total_tokens,
            "elapsed_ms":  int((time.time()-t0)*1000),
            "summary":     dataset.summary_text[:500],
        }
    except Exception as e:
        raise HTTPException(500, f"Ingestion failed: {e}")


# ── /cache/status — what's in the cache ────────────────────────────────────
@router.get("/cache/status")
def cache_status():
    """List all scorecards currently in the data cache."""
    pipeline = get_pipeline()
    return {
        "cached_sources": pipeline.cached_sources(),
        "tableau_configured": check_tableau(),
        "oracle_configured":  check_oracle(),
    }


# ── /cache/{source_id}/invalidate ──────────────────────────────────────────
@router.delete("/cache/{source_id}")
def invalidate_cache(source_id: str):
    """Force re-extraction on next chat query."""
    get_pipeline().invalidate(source_id)
    return {"status": "invalidated", "source_id": source_id}


# ── /chat/live — chat using live Tableau data (not just YAML) ─────────────
@router.post("/chat/live", response_model=ChatResponse)
async def chat_live(req: ChatRequest, request: Request):
    """
    Enhanced chat endpoint.
    Builds context from:
      1. Live Tableau CSV (via IngestionPipeline — cached in Oracle)
      2. YAML scorecard metadata
      3. Combined as system prompt for Gemini

    Falls back to YAML-only if Tableau not configured.
    """
    if not check_vertex():
        raise HTTPException(503, "Vertex AI not configured — set GOOGLE_PROJECT_ID in .env")

    user         = req.user_id or uid(request)
    scorecard_id = req.scorecard_id
    sc_name      = req.scorecard_name or scorecard_id
    view_id      = req.view_id or request.headers.get("X-View-ID", "")
    t0           = time.time()

    # ── Get session ─────────────────────────────────────────────────────
    session_id = (
        oracle_service.get_or_create_session(user, scorecard_id, sc_name, req.session_id)
        if check_oracle()
        else (req.session_id or __import__("uuid").uuid4().__str__())
    )

    # ── Build messages ────────────────────────────────────────────────────
    messages = [{"role": m.role, "content": m.content} for m in req.messages]
    if req.session_id and check_oracle():
        db_history = oracle_service.get_recent_for_llm(session_id, MAX_HISTORY)
        if db_history:
            last = req.messages[-1]
            messages = db_history
            if not messages or messages[-1]["content"] != last.content:
                messages.append({"role": last.role, "content": last.content})

    # ── Build system prompt ────────────────────────────────────────────────
    # Layer 1: YAML baseline context
    system_prompt = build_system_prompt(scorecard_id)

    # Layer 2: Inject LIVE Tableau data (if view_id available)
    live_context = ""
    if view_id and check_tableau():
        try:
            pipeline     = get_pipeline()
            live_context = pipeline.build_llm_context(
                source_id   = scorecard_id,
                view_id     = view_id,
                source_name = sc_name,
                max_tokens  = 5000,
            )
            log.info(f"Live context injected for {scorecard_id}: {len(live_context)} chars")
        except Exception as e:
            log.warning(f"Live context failed (YAML-only fallback): {e}")

    if live_context:
        system_prompt = (
            system_prompt
            + "\n\n=== LIVE TABLEAU DATA ===\n"
            + live_context
            + "\n========================\n"
            + "IMPORTANT: When live data is present above, use it to answer. "
            "It reflects the current state of the dashboard."
        )

    # ── Call Vertex AI ────────────────────────────────────────────────────
    try:
        reply, chart_raw, in_tok, out_tok, resp_ms = vertex_service.chat(
            system_prompt = system_prompt,
            messages      = messages,
            max_tokens    = req.max_tokens or 800,
        )
    except Exception as e:
        log.error(f"Vertex AI error: {e}")
        raise HTTPException(500, f"AI error: {e}")

    # ── Parse chart ─────────────────────────────────────────────────────
    chart = None
    if chart_raw and isinstance(chart_raw, dict):
        try:
            from backend.models.schemas import ChartData
            chart = ChartData(
                chart_type = chart_raw.get("chart_type", "text"),
                title      = chart_raw.get("title", ""),
                subtitle   = chart_raw.get("subtitle", ""),
                labels     = chart_raw.get("labels"),
                datasets   = chart_raw.get("datasets"),
                kpis       = chart_raw.get("kpis"),
                table_cols = chart_raw.get("columns"),
                table_rows = chart_raw.get("rows"),
                colors     = chart_raw.get("colors"),
            )
        except Exception as e:
            log.warning(f"ChartData parse failed: {e}")

    # ── Persist ──────────────────────────────────────────────────────────
    if check_oracle():
        oracle_service.save_message(session_id, user, scorecard_id, "user",  req.messages[-1].content)
        oracle_service.save_message(session_id, user, scorecard_id, "assistant", reply, f"gemini-live/{view_id or 'yaml'}", in_tok, out_tok, resp_ms)

    return ChatResponse(
        reply         = reply,
        session_id    = str(session_id),
        scorecard_id  = scorecard_id,
        model         = f"vertex-live/{scorecard_id}",
        chart         = chart,
        input_tokens  = in_tok,
        output_tokens = out_tok,
        response_ms   = resp_ms,
    )
