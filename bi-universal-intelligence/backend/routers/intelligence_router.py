"""
routers/intelligence_router.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
All endpoints for the Universal Tableau Intelligence System.

Endpoints:
  POST /intelligence/onboard      — Full pipeline: Extract → Analyse → Index → Cache
  POST /intelligence/query        — Tiered query (Tier 1/2/3 auto-routing)
  GET  /intelligence/status/{id}  — Dashboard intelligence status + health
  GET  /intelligence/insights/{id}— Auto-generated proactive insights
  GET  /intelligence/list         — All onboarded dashboards
  DELETE /intelligence/{id}       — Remove a dashboard
  POST /intelligence/refresh/{id} — Force re-extraction
"""
import io
import time
import logging
from typing import Dict, List, Optional

import pandas as pd
from fastapi import APIRouter, HTTPException, BackgroundTasks, Query

from backend.ingestion.tableau_extractor  import TableauConnection, TableauExtractor, ViewTarget
from backend.ingestion.visual_extractor   import VisualDashboardExtractor
from backend.ingestion.schema_analyser    import SchemaAnalyser
from backend.ingestion.question_generator import DynamicQuestionGenerator
from backend.intelligence.semantic_layer  import SemanticLayer
from backend.intelligence.dashboard_schema import build_schema, UniversalDashboardSchema
from backend.intelligence.query_engine    import QueryEngine, QueryResponse
from backend.config import check_tableau, check_vertex, check_oracle

log    = logging.getLogger("intelligence_router")
router = APIRouter(prefix="/intelligence", tags=["intelligence"])

# ── In-process registry ────────────────────────────────────────────────────
# {source_id: {"schema": UDS, "df": DataFrame, "sem": SemanticProfile, "meta": dict}}
_REGISTRY: Dict[str, Dict] = {}

_vde  = VisualDashboardExtractor()
_sa   = SchemaAnalyser()
_qgen = DynamicQuestionGenerator()
_sl   = SemanticLayer()


def _get_entry(source_id: str) -> Dict:
    entry = _REGISTRY.get(source_id)
    if not entry:
        raise HTTPException(404, f"'{source_id}' not onboarded. Call POST /intelligence/onboard first.")
    return entry


# ══════════════════════════════════════════════════════════════════════
# POST /intelligence/onboard
# Full zero-config onboarding pipeline
# ══════════════════════════════════════════════════════════════════════

@router.post("/onboard")
async def onboard(body: dict, bg: BackgroundTasks = None):
    """
    Zero-configuration dashboard onboarding.

    Required: source_id, view_id
    Optional: source_name, filters

    Pipeline:
      1. Extract data from Tableau (CSV → REST → Image fallback)
      2. Infer visual structure (18 visual types)
      3. Build semantic layer (entities, KPIs, relationships, anomalies)
      4. Index with TF-IDF for semantic search
      5. Generate context-aware questions
      6. Build UniversalDashboardSchema (portable JSON)
      7. Cache everything in-process + Oracle
    """
    source_id   = body.get("source_id", "").strip()
    source_name = body.get("source_name", source_id).strip()
    view_id     = body.get("view_id", "").strip()
    filters     = body.get("filters", {})

    if not source_id:
        raise HTTPException(400, "source_id is required")
    if not view_id and not check_tableau():
        raise HTTPException(400, "view_id required (and TABLEAU_* env vars configured)")

    t0 = time.time()

    # ── Step 1: Extract ────────────────────────────────────────────────
    df       = pd.DataFrame()
    strategy = "none"

    if view_id and check_tableau():
        try:
            conn      = TableauConnection.from_env()
            extractor = TableauExtractor(conn)
            target    = ViewTarget(view_id=view_id, scorecard_id=source_id, view_name=source_name)

            # Try CSV first
            try:
                df       = extractor.get_dataframe(target, filters=filters or None)
                strategy = "csv"
            except Exception:
                pass

            # Fallback: REST API
            if df.empty:
                try:
                    rows     = extractor.get_underlying_json(target)
                    df       = pd.DataFrame(rows) if rows else pd.DataFrame()
                    strategy = "rest"
                except Exception:
                    pass

            # Fallback: image only
            if df.empty:
                strategy = "image"
                log.warning(f"No underlying data for {source_id} — image-only mode")

        except Exception as e:
            log.error(f"Tableau connection failed: {e}")
            raise HTTPException(502, f"Tableau connection failed: {e}")

    if df.empty and strategy != "image":
        raise HTTPException(422, "Extracted data is empty. Check view_id and permissions.")

    # ── Step 2: Visual structure ──────────────────────────────────────
    snap = _vde.extract(df, source_id=source_id, source_name=source_name, view_id=view_id)

    # ── Step 3: Semantic layer ────────────────────────────────────────
    sem = _sl.analyse(df, source_id=source_id, source_name=source_name)

    # ── Step 4: Schema profile (for question generation) ─────────────
    schema_profile = _sa.analyse(df, source_id=source_id, source_name=source_name)

    # ── Step 5: Generate questions ────────────────────────────────────
    questions = _qgen.generate(schema_profile, max_q=10)

    # Supplement with semantic questions
    if sem.anomalies:
        for a in sem.anomalies[:2]:
            if a.signal_type == "breach":
                questions.insert(0, f"Why is {a.col} breaching its threshold?")
    if sem.temporal:
        t = sem.temporal[0]
        questions.insert(0, f"What is the trend for {t.measure_col} over {t.time_col}?")

    questions = list(dict.fromkeys(questions))[:10]  # deduplicate, keep top 10

    # ── Step 6: Build universal schema ───────────────────────────────
    uds = build_schema(
        source_id   = source_id,
        source_name = source_name,
        view_id     = view_id,
        snap        = snap,
        sem         = sem,
        questions   = questions,
        strategy    = strategy,
    )

    # ── Step 7: Cache ─────────────────────────────────────────────────
    _REGISTRY[source_id] = {
        "schema": uds,
        "df":     df,
        "sem":    sem,
        "snap":   snap,
        "meta": {
            "view_id":      view_id,
            "source_name":  source_name,
            "onboarded_at": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "strategy":     strategy,
            "rows":         len(df),
            "cols":         len(df.columns) if not df.empty else 0,
        },
    }

    # Optional: save to Oracle
    if check_oracle():
        _save_to_oracle(source_id, uds)

    elapsed = round(time.time() - t0, 2)
    log.info(f"Onboarded: {source_id} | {snap.dashboard_type} | {len(df)} rows | {elapsed}s")

    return {
        "status":          "onboarded",
        "source_id":       source_id,
        "source_name":     source_name,
        "dashboard_type":  snap.dashboard_type,
        "extraction_strategy": strategy,
        "total_rows":      len(df),
        "total_cols":      len(df.columns) if not df.empty else 0,
        "kpis_detected":   len(sem.kpis),
        "anomalies":       len(sem.anomalies),
        "entities":        len(sem.entities),
        "index_docs":      len(sem.index_docs),
        "questions":       questions,
        "narrative":       uds.narrative,
        "capabilities": {
            "time_series":   snap.has_time,
            "geographic":    snap.has_geo,
            "rag_status":    snap.has_rag,
            "thresholds":    snap.has_threshold,
            "multi_measure": snap.has_multi_measure,
        },
        "visual_types": [
            {"type": v.vtype, "confidence": round(v.confidence, 2)}
            for v in snap.visual_types[:5]
        ],
        "kpis": [
            {"name": k.name, "status": k.status, "trend": k.trend,
             "current_val": k.current_val, "threshold": k.threshold}
            for k in sem.kpis[:6]
        ],
        "top_anomaly": {
            "description": sem.anomalies[0].description,
            "severity":    sem.anomalies[0].severity,
        } if sem.anomalies else None,
        "elapsed_sec": elapsed,
    }


# ══════════════════════════════════════════════════════════════════════
# POST /intelligence/query
# Tiered query routing
# ══════════════════════════════════════════════════════════════════════

@router.post("/query")
def query(body: dict):
    """
    Intelligent query routing across 3 tiers.

    Body: { "source_id": "...", "question": "Which region is underperforming?" }

    Returns: reply + tier + confidence + chart + source attribution
    """
    source_id = body.get("source_id", "").strip()
    question  = body.get("question",  "").strip()
    use_llm   = body.get("use_llm",   True)

    if not source_id or not question:
        raise HTTPException(400, "source_id and question are required")

    entry  = _get_entry(source_id)
    df     = entry.get("df", pd.DataFrame())
    sem    = entry.get("sem")
    schema = entry.get("schema")

    engine = QueryEngine(use_llm=use_llm and check_vertex())
    resp   = engine.query(question=question, df=df, schema=schema, sem=sem)

    return resp.to_dict()


# ══════════════════════════════════════════════════════════════════════
# GET /intelligence/status/{source_id}
# ══════════════════════════════════════════════════════════════════════

@router.get("/status/{source_id}")
def status(source_id: str):
    """Intelligence status, health, and summary for a dashboard."""
    entry  = _get_entry(source_id)
    schema = entry["schema"]
    sem    = entry["sem"]
    meta   = entry.get("meta", {})

    return {
        "source_id":       source_id,
        "source_name":     schema.source_name,
        "status":          "ready",
        "onboarded_at":    meta.get("onboarded_at"),
        "data_freshness":  meta.get("onboarded_at"),
        "extraction_strategy": meta.get("strategy"),
        "rows":            meta.get("rows", 0),
        "cols":            meta.get("cols", 0),
        "dashboard_type":  schema.dashboard_type,
        "narrative":       schema.narrative,
        "index_size":      len(sem.index_docs),
        "kpi_summary": {
            "total":     len(sem.kpis),
            "breaching": sum(1 for k in sem.kpis if k.status == "breaching"),
            "at_risk":   sum(1 for k in sem.kpis if k.status == "at_risk"),
            "on_track":  sum(1 for k in sem.kpis if k.status == "on_track"),
        },
        "anomaly_count":   len(sem.anomalies),
        "top_anomalies": [
            {"type": a.signal_type, "severity": a.severity, "desc": a.description}
            for a in sem.anomalies[:3]
        ],
        "capabilities": {
            "time_series":   schema.has_time_series,
            "geographic":    schema.has_geo,
            "rag_status":    schema.has_rag,
            "thresholds":    schema.has_threshold,
            "multi_measure": schema.has_multi_measure,
        },
    }


# ══════════════════════════════════════════════════════════════════════
# GET /intelligence/insights/{source_id}
# Proactive AI-generated insights
# ══════════════════════════════════════════════════════════════════════

@router.get("/insights/{source_id}")
def insights(source_id: str):
    """
    Proactive insights — what the system noticed without being asked.
    Generated from the semantic profile at onboarding time.
    """
    entry  = _get_entry(source_id)
    sem    = entry["sem"]
    schema = entry["schema"]

    insight_cards = []

    # Anomaly-based insights
    for a in sem.anomalies[:5]:
        severity_color = {
            "critical": "red", "high": "red",
            "medium":   "amber", "low": "blue"
        }.get(a.severity, "blue")
        insight_cards.append({
            "type":        a.signal_type,
            "severity":    a.severity,
            "color":       severity_color,
            "title":       a.description,
            "detail":      a.context,
            "category":    "anomaly",
        })

    # KPI insights
    breaching = [k for k in sem.kpis if k.status == "breaching"]
    for k in breaching[:3]:
        insight_cards.append({
            "type":     "kpi_breach",
            "severity": "critical",
            "color":    "red",
            "title":    f"{k.name} is breaching its threshold",
            "detail":   (
                f"Current: {k.current_val} | Threshold: {k.threshold} | "
                f"Trend: {k.trend} | Change: {k.pct_change:+.1f}%" if k.pct_change else ""
            ),
            "category": "kpi",
        })

    # Temporal insights
    for t in sem.temporal[:2]:
        if abs(t.pct_change) > 10:
            insight_cards.append({
                "type":     "trend",
                "severity": "medium" if abs(t.pct_change) < 20 else "high",
                "color":    "amber" if t.trend_dir in ("down","volatile") else "green",
                "title":    f"{t.measure_col} {t.trend_dir} trend: {t.pct_change:+.1f}% over {t.n_periods} periods",
                "detail":   f"From {t.min_period} ({t.baseline_val:.3f}) to {t.max_period} ({t.latest_val:.3f})",
                "category": "temporal",
            })

    # Summary insight
    if sem.narrative:
        insight_cards.insert(0, {
            "type":     "summary",
            "severity": "info",
            "color":    "blue",
            "title":    "Dashboard Summary",
            "detail":   sem.narrative,
            "category": "summary",
        })

    return {
        "source_id":   source_id,
        "source_name": schema.source_name,
        "insight_count": len(insight_cards),
        "insights":    insight_cards,
        "questions":   schema.questions[:8],
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


# ══════════════════════════════════════════════════════════════════════
# GET /intelligence/list
# ══════════════════════════════════════════════════════════════════════

@router.get("/list")
def list_onboarded():
    """List all onboarded dashboards with their intelligence summary."""
    return {
        "count": len(_REGISTRY),
        "dashboards": [
            {
                "source_id":      sid,
                "source_name":    e["schema"].source_name,
                "dashboard_type": e["schema"].dashboard_type,
                "rows":           e["meta"].get("rows", 0),
                "onboarded_at":   e["meta"].get("onboarded_at"),
                "kpis":           len(e["sem"].kpis),
                "anomalies":      len(e["sem"].anomalies),
                "has_rag":        e["schema"].has_rag,
                "has_time":       e["schema"].has_time_series,
            }
            for sid, e in _REGISTRY.items()
        ]
    }


# ══════════════════════════════════════════════════════════════════════
# DELETE /intelligence/{source_id}
# ══════════════════════════════════════════════════════════════════════

@router.delete("/{source_id}")
def remove(source_id: str):
    """Remove a dashboard from the intelligence registry."""
    if source_id not in _REGISTRY:
        raise HTTPException(404, f"'{source_id}' not found")
    del _REGISTRY[source_id]
    return {"status": "removed", "source_id": source_id}


# ══════════════════════════════════════════════════════════════════════
# POST /intelligence/refresh/{source_id}
# ══════════════════════════════════════════════════════════════════════

@router.post("/refresh/{source_id}")
async def refresh(source_id: str, bg: BackgroundTasks = None):
    """Force re-extraction and re-indexing for a dashboard."""
    entry = _get_entry(source_id)
    meta  = entry.get("meta", {})
    view_id    = meta.get("view_id", "")
    source_name= meta.get("source_name", source_id)

    body = {"source_id": source_id, "source_name": source_name, "view_id": view_id}
    return await onboard(body, bg)


# ══════════════════════════════════════════════════════════════════════
# GET /intelligence/schema/{source_id}
# ══════════════════════════════════════════════════════════════════════

@router.get("/schema/{source_id}")
def get_schema(source_id: str):
    """Return the full UniversalDashboardSchema as JSON."""
    entry = _get_entry(source_id)
    return entry["schema"].to_dict() if hasattr(entry["schema"], "to_dict") else {}


# ── Helpers ────────────────────────────────────────────────────────────

def _save_to_oracle(source_id: str, uds: UniversalDashboardSchema) -> None:
    try:
        from backend.services.oracle_service import _get_pool
        pool = _get_pool()
        if not pool:
            return
        schema_json = uds.to_json()
        with pool.acquire() as conn:
            conn.execute(
                """MERGE INTO BI_DATA_CONTEXT t
                   USING (SELECT :1 AS SOURCE_ID, :2 AS CHUNK_ID FROM DUAL) s
                   ON (t.SOURCE_ID = s.SOURCE_ID AND t.CHUNK_ID = s.CHUNK_ID)
                   WHEN MATCHED THEN UPDATE SET
                     SOURCE_NAME=:3, CHUNK_TYPE='universal_schema',
                     CONTENT=:4, TOTAL_ROWS=:5, TOTAL_COLS=:6,
                     EXTRACTED_DT=SYSTIMESTAMP
                   WHEN NOT MATCHED THEN INSERT
                     (SOURCE_ID,CHUNK_ID,SOURCE_NAME,CHUNK_TYPE,CONTENT,TOTAL_ROWS,TOTAL_COLS)
                   VALUES (:1,:2,:3,:4,:5,:6)""",
                [source_id, "universal_schema", uds.source_name, schema_json,
                 uds.total_rows, uds.total_cols]
            )
            conn.commit()
    except Exception as e:
        log.debug(f"Oracle save failed (non-critical): {e}")
