"""
routers/api.py — All API routes in one file (keeps it simple)
"""
import time, logging
from fastapi import APIRouter, HTTPException, Request, Query
from fastapi.responses import Response
from backend.models.schemas import ChatRequest, ChatResponse, ChartData
from backend.services import tableau_service, vertex_service, oracle_service
from backend.context.loader import build_system_prompt, list_scorecards
from backend.config import check_tableau, check_vertex, check_oracle, MAX_HISTORY

router = APIRouter()
log = logging.getLogger("api")


def uid(request: Request) -> str:
    return request.headers.get("X-User-ID", "ANONYMOUS")


# ══════════════════════════════════════════════════════
# HEALTH
# ══════════════════════════════════════════════════════

@router.get("/health/")
def health():
    services = []

    if check_tableau():
        ok, ms = tableau_service.ping()
        services.append({"name":"Tableau","status":"ok" if ok else "down","latency_ms":ms})
    else:
        services.append({"name":"Tableau","status":"not_configured"})

    if check_vertex():
        ok, ms = vertex_service.ping()
        services.append({"name":"VertexAI","status":"ok" if ok else "down","latency_ms":ms})
    else:
        services.append({"name":"VertexAI","status":"not_configured"})

    if check_oracle():
        t0 = time.time()
        ok = oracle_service.ping()
        services.append({"name":"Oracle","status":"ok" if ok else "down","latency_ms":int((time.time()-t0)*1000)})
    else:
        services.append({"name":"Oracle","status":"not_configured"})

    overall = "ok"
    if any(s["status"]=="down" for s in services if s["status"]!="not_configured"):
        overall = "degraded"

    return {
        "status": overall,
        "version": "3.0.0",
        "services": services,
        "cache": tableau_service.cache_stats(),
        "scorecards_loaded": list_scorecards(),
    }


# ══════════════════════════════════════════════════════
# TABLEAU SNAPSHOTS
# ══════════════════════════════════════════════════════

@router.get("/snapshot/{view_id}")
def snapshot_png(view_id: str, request: Request,
                 scorecard_id: str=Query(""), refresh: bool=Query(False)):
    if not check_tableau():
        raise HTTPException(503, "Tableau not configured — set TABLEAU_* in .env")
    user = uid(request)
    t0   = time.time()
    try:
        data, fetch_ms = tableau_service.get_view_image(view_id, force=refresh)
        oracle_service.log_snapshot(view_id, scorecard_id, user, "PNG", True, "", len(data), int((time.time()-t0)*1000))
        return Response(content=data, media_type="image/png",
                        headers={"Cache-Control":"max-age=300","X-Fetch-Ms":str(fetch_ms),"X-Cached":"true" if fetch_ms==0 else "false"})
    except FileNotFoundError as e:
        raise HTTPException(404, str(e))
    except Exception as e:
        oracle_service.log_snapshot(view_id, scorecard_id, user, "PNG", False, str(e))
        raise HTTPException(500, f"Tableau error: {e}")


@router.get("/snapshot/{view_id}/pdf")
def snapshot_pdf(view_id: str, request: Request,
                 scorecard_id: str=Query(""), refresh: bool=Query(False)):
    if not check_tableau():
        raise HTTPException(503, "Tableau not configured")
    user = uid(request)
    t0   = time.time()
    try:
        data, ms = tableau_service.get_view_pdf(view_id, force=refresh)
        oracle_service.log_snapshot(view_id, scorecard_id, user, "PDF", True, "", len(data), int((time.time()-t0)*1000))
        return Response(content=data, media_type="application/pdf",
                        headers={"Content-Disposition":f'attachment; filename="scorecard-{view_id}.pdf"',"Cache-Control":"max-age=300"})
    except Exception as e:
        oracle_service.log_snapshot(view_id, scorecard_id, user, "PDF", False, str(e))
        raise HTTPException(500, f"Tableau error: {e}")


@router.get("/snapshot/{view_id}/csv")
def snapshot_csv(view_id: str, request: Request, scorecard_id: str=Query("")):
    if not check_tableau():
        raise HTTPException(503, "Tableau not configured")
    user = uid(request)
    try:
        data, ms = tableau_service.get_view_csv(view_id)
        oracle_service.log_snapshot(view_id, scorecard_id, user, "CSV", True, "", len(data), ms)
        return Response(content=data, media_type="text/csv",
                        headers={"Content-Disposition":f'attachment; filename="data-{view_id}.csv"'})
    except Exception as e:
        oracle_service.log_snapshot(view_id, scorecard_id, user, "CSV", False, str(e))
        raise HTTPException(500, f"Tableau error: {e}")


@router.get("/cache/stats")
def cache_stats():
    return tableau_service.cache_stats()

@router.delete("/cache/{view_id}")
def clear_cache(view_id: str):
    from backend.services.tableau_service import _cache, _cache_key
    removed = [ft for ft in ["PNG","PDF"] if _cache.pop(_cache_key(view_id, ft), None)]
    return {"cleared": removed, "view_id": view_id}


# ══════════════════════════════════════════════════════
# CHAT — returns reply + chart_data
# ══════════════════════════════════════════════════════

@router.post("/chat/", response_model=ChatResponse)
async def chat(req: ChatRequest, request: Request):
    if not check_vertex():
        raise HTTPException(503, "Vertex AI not configured — set GOOGLE_PROJECT_ID in .env")

    user         = req.user_id or uid(request)
    scorecard_id = req.scorecard_id
    sc_name      = req.scorecard_name or scorecard_id
    t0           = time.time()

    # Session management (graceful — works without Oracle)
    session_id = oracle_service.get_or_create_session(user, scorecard_id, sc_name, req.session_id) \
                 if check_oracle() else (req.session_id or __import__("uuid").uuid4().__str__())

    # Build message list — use Oracle history if continuing session
    messages = [{"role": m.role, "content": m.content} for m in req.messages]
    if req.session_id and check_oracle():
        db_history = oracle_service.get_recent_for_llm(session_id, MAX_HISTORY)
        if db_history:
            last = req.messages[-1]
            messages = db_history
            if not messages or messages[-1]["content"] != last.content:
                messages.append({"role": last.role, "content": last.content})

    # Optionally inject live CSV from Tableau
    csv_bytes = None
    if req.view_id and check_tableau():
        try:
            csv_bytes, _ = tableau_service.get_view_csv(req.view_id)
            log.info(f"CSV injected for {scorecard_id}: {len(csv_bytes)}b")
        except Exception as e:
            log.warning(f"CSV fetch skipped: {e}")

    system_prompt = build_system_prompt(scorecard_id, csv_bytes)

    # Call Vertex AI
    try:
        reply, chart_raw, in_tok, out_tok, resp_ms = vertex_service.chat(
            system_prompt=system_prompt,
            messages=messages,
            max_tokens=req.max_tokens or 800,
        )
    except Exception as e:
        log.error(f"Vertex AI error: {e}")
        raise HTTPException(500, f"AI error: {e}")

    # Parse chart data
    chart = None
    if chart_raw and isinstance(chart_raw, dict):
        try:
            chart = ChartData(
                chart_type=chart_raw.get("chart_type", chart_raw.get("type", "text")),
                title=chart_raw.get("title",""),
                subtitle=chart_raw.get("subtitle",""),
                labels=chart_raw.get("labels"),
                datasets=chart_raw.get("datasets"),
                kpis=chart_raw.get("kpis"),
                table_cols=chart_raw.get("columns"),
                table_rows=chart_raw.get("rows"),
                colors=chart_raw.get("colors"),
            )
        except Exception as e:
            log.warning(f"ChartData parse failed: {e}")

    # Persist to Oracle (non-blocking)
    if check_oracle():
        oracle_service.save_message(session_id, user, scorecard_id, "user", req.messages[-1].content)
        oracle_service.save_message(session_id, user, scorecard_id, "assistant", reply,
                                    f"gemini/{scorecard_id}", in_tok, out_tok, resp_ms)

    oracle_service.log_api("/chat/", "POST", user, scorecard_id, 200,
                           int((time.time()-t0)*1000), "", request.client.host if request.client else "")

    return ChatResponse(
        reply=reply,
        session_id=str(session_id),
        scorecard_id=scorecard_id,
        model=VERTEX_MODEL if check_vertex() else "offline",
        chart=chart,
        input_tokens=in_tok,
        output_tokens=out_tok,
        response_ms=resp_ms,
    )


# ══════════════════════════════════════════════════════
# HISTORY
# ══════════════════════════════════════════════════════

@router.get("/history/sessions")
def history_sessions(request: Request, scorecard_id: str=Query(""), limit: int=Query(20)):
    if not check_oracle():
        return {"sessions":[], "note":"Oracle not configured"}
    return {"sessions": oracle_service.get_user_sessions(uid(request), scorecard_id or None, limit)}


@router.get("/history/sessions/{session_id}")
def history_session(session_id: str, request: Request):
    if not check_oracle():
        raise HTTPException(503, "Oracle not configured")
    data = oracle_service.get_session_messages(session_id, uid(request))
    if not data:
        raise HTTPException(404, "Session not found")
    return data


# ══════════════════════════════════════════════════════
# ANALYTICS
# ══════════════════════════════════════════════════════

@router.get("/analytics/summary")
def analytics(days: int=Query(30)):
    if not check_oracle():
        return {"note":"Oracle not configured — analytics unavailable"}
    return oracle_service.get_analytics(days)


# ══════════════════════════════════════════════════════
# SCORECARDS REGISTRY
# ══════════════════════════════════════════════════════

@router.get("/scorecards/")
def scorecards_list():
    return {"scorecards": list_scorecards()}
