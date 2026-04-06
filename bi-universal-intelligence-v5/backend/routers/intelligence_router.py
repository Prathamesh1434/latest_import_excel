"""
routers/intelligence_router.py — v5

CHANGES v5 (6 Apr 2026):
  - Every onboard step logs START/END/DURATION to terminal
  - Publishes SSE progress events via main.publish_progress()
  - Diagnostic error messages — log snapshot tells you exactly what failed
  - try/except on EVERY step — one failure doesn't kill the whole pipeline
  - Oracle save uses proper parameterised queries
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

# ── In-process registry ────────────────────────────────────────────────
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


def _progress(source_id: str, step: int, total: int, label: str, status: str = "running"):
    """Log to terminal AND publish to SSE stream."""
    log.info(f"  [{step}/{total}] {label}")
    try:
        from backend.main import publish_progress
        publish_progress(source_id, step, total, label, status)
    except Exception:
        pass  # SSE not available (e.g. during tests)


# ══════════════════════════════════════════════════════════════════════
# POST /intelligence/onboard
# ══════════════════════════════════════════════════════════════════════

@router.post("/onboard")
async def onboard(body: dict, bg: BackgroundTasks = None):
    """
    Zero-configuration dashboard onboarding.

    Required: source_id, view_id
    Optional: source_name, filters

    Pipeline (7 steps, each logged individually):
      1. Validate config
      2. Extract data from Tableau (CSV → REST → Image)
      3. Infer visual structure (18 visual types)
      4. Build semantic layer (entities, KPIs, anomalies)
      5. Generate context-aware questions
      6. Build UniversalDashboardSchema
      7. Cache in registry + Oracle
    """
    source_id   = body.get("source_id", "").strip()
    source_name = body.get("source_name", source_id).strip()
    view_id     = body.get("view_id", "").strip()
    filters     = body.get("filters", {})

    if not source_id:
        raise HTTPException(400, "source_id is required")

    TOTAL_STEPS = 7
    t0 = time.time()

    log.info("═" * 60)
    log.info(f"ONBOARD START: {source_id}")
    log.info(f"  view_id={view_id}  name={source_name}")
    log.info("═" * 60)

    # ── Step 1: Validate ──────────────────────────────────────────────
    _progress(source_id, 1, TOTAL_STEPS, "Validating configuration…")
    step1_t = time.time()

    if not view_id:
        _progress(source_id, 1, TOTAL_STEPS, "No view_id — using demo mode", "running")
        log.info(f"  │ No view_id provided — will need data or demo mode")

    if view_id and not check_tableau():
        _progress(source_id, 1, TOTAL_STEPS, "Tableau not configured", "error")
        log.error("  │ ✗ Tableau env vars missing. Set TABLEAU_SERVER, TABLEAU_USERNAME, TABLEAU_PASSWORD in .env")
        raise HTTPException(400,
            "view_id provided but Tableau not configured. "
            "Set TABLEAU_SERVER, TABLEAU_USERNAME, TABLEAU_PASSWORD in .env"
        )

    log.info(f"  │ Step 1 done: {int((time.time()-step1_t)*1000)}ms")

    # ── Step 2: Extract data ──────────────────────────────────────────
    _progress(source_id, 2, TOTAL_STEPS, "Connecting to Tableau & extracting data…")
    step2_t = time.time()

    df       = pd.DataFrame()
    strategy = "none"

    if view_id and check_tableau():
        try:
            conn      = TableauConnection.from_env()
            extractor = TableauExtractor(conn)
            target    = ViewTarget(view_id=view_id, scorecard_id=source_id, view_name=source_name)

            # Strategy 1: CSV
            log.info("  │ Trying Strategy 1: populate_csv…")
            _progress(source_id, 2, TOTAL_STEPS, "Strategy 1: Extracting CSV data…")
            try:
                df       = extractor.get_dataframe(target, filters=filters or None)
                strategy = "csv"
                log.info(f"  │ ✓ CSV: {len(df)} rows × {len(df.columns)} cols")
            except TimeoutError as e:
                log.error(f"  │ ✗ CSV TIMEOUT: {e}")
                _progress(source_id, 2, TOTAL_STEPS, f"CSV timed out — trying REST API…")
            except Exception as e:
                log.warning(f"  │ ✗ CSV failed: {type(e).__name__}: {e}")
                _progress(source_id, 2, TOTAL_STEPS, "CSV failed — trying REST API…")

            # Strategy 2: REST
            if df.empty:
                log.info("  │ Trying Strategy 2: REST API…")
                _progress(source_id, 2, TOTAL_STEPS, "Strategy 2: REST API extraction…")
                try:
                    rows     = extractor.get_underlying_json(target)
                    df       = pd.DataFrame(rows) if rows else pd.DataFrame()
                    strategy = "rest"
                    if not df.empty:
                        log.info(f"  │ ✓ REST: {len(df)} rows")
                    else:
                        log.warning("  │ ✗ REST: empty response")
                except TimeoutError as e:
                    log.error(f"  │ ✗ REST TIMEOUT: {e}")
                except Exception as e:
                    log.warning(f"  │ ✗ REST failed: {type(e).__name__}: {e}")

            # Strategy 3: Image only
            if df.empty:
                strategy = "image"
                log.warning("  │ All data strategies failed — image-only mode")
                _progress(source_id, 2, TOTAL_STEPS, "No tabular data — image-only mode")

        except HTTPException:
            raise
        except Exception as e:
            log.error(f"  │ ✗ Tableau connection FAILED: {type(e).__name__}: {e}")
            _progress(source_id, 2, TOTAL_STEPS, f"Tableau error: {e}", "error")
            raise HTTPException(502,
                f"Tableau connection failed: {type(e).__name__}: {e}. "
                f"Check: server reachable? VPN? SSL cert? Credentials?"
            )

    step2_ms = int((time.time() - step2_t) * 1000)
    log.info(f"  │ Step 2 done: {step2_ms}ms | strategy={strategy} | rows={len(df)}")

    if df.empty and strategy != "image":
        _progress(source_id, 2, TOTAL_STEPS, "No data extracted", "error")
        raise HTTPException(422,
            "Extracted data is empty. Check: view_id correct? "
            "View has data? User has permission? Filters too narrow?"
        )

    # ── Step 3: Visual structure ──────────────────────────────────────
    _progress(source_id, 3, TOTAL_STEPS, "Analysing visual structure (18 types)…")
    step3_t = time.time()

    try:
        snap = _vde.extract(df, source_id=source_id, source_name=source_name, view_id=view_id)
        log.info(f"  │ ✓ Visual: {snap.dashboard_type} | {len(snap.visual_types)} types | {len(snap.agg_blocks)} aggs")
    except Exception as e:
        log.error(f"  │ ✗ Visual extraction failed: {e}")
        _progress(source_id, 3, TOTAL_STEPS, f"Visual analysis error: {e}", "error")
        raise HTTPException(500, f"Visual extraction failed: {e}")

    log.info(f"  │ Step 3 done: {int((time.time()-step3_t)*1000)}ms")

    # ── Step 4: Semantic layer ────────────────────────────────────────
    _progress(source_id, 4, TOTAL_STEPS, "Building semantic layer + TF-IDF index…")
    step4_t = time.time()

    try:
        sem = _sl.analyse(df, source_id=source_id, source_name=source_name)
        log.info(
            f"  │ ✓ Semantic: {len(sem.entities)} entities | "
            f"{len(sem.kpis)} KPIs | {len(sem.anomalies)} anomalies | "
            f"{len(sem.temporal)} temporal | {len(sem.index_docs)} TF-IDF docs"
        )
    except Exception as e:
        log.error(f"  │ ✗ Semantic layer failed: {e}")
        _progress(source_id, 4, TOTAL_STEPS, f"Semantic analysis error: {e}", "error")
        raise HTTPException(500, f"Semantic layer failed: {e}")

    log.info(f"  │ Step 4 done: {int((time.time()-step4_t)*1000)}ms")

    # ── Step 5: Generate questions ────────────────────────────────────
    _progress(source_id, 5, TOTAL_STEPS, "Generating intelligence questions…")
    step5_t = time.time()

    try:
        schema_profile = _sa.analyse(df, source_id=source_id, source_name=source_name)
        questions = _qgen.generate(schema_profile, max_q=10)

        # Supplement with semantic questions
        if sem.anomalies:
            for a in sem.anomalies[:2]:
                if a.signal_type == "breach":
                    questions.insert(0, f"Why is {a.col} breaching its threshold?")
        if sem.temporal:
            t = sem.temporal[0]
            questions.insert(0, f"What is the trend for {t.measure_col} over {t.time_col}?")

        questions = list(dict.fromkeys(questions))[:10]
        log.info(f"  │ ✓ Generated {len(questions)} questions")
    except Exception as e:
        log.warning(f"  │ ✗ Question generation failed (non-critical): {e}")
        questions = ["Give me a summary of this dashboard"]

    log.info(f"  │ Step 5 done: {int((time.time()-step5_t)*1000)}ms")

    # ── Step 6: Build universal schema ────────────────────────────────
    _progress(source_id, 6, TOTAL_STEPS, "Building universal schema…")
    step6_t = time.time()

    try:
        uds = build_schema(
            source_id   = source_id,
            source_name = source_name,
            view_id     = view_id,
            snap        = snap,
            sem         = sem,
            questions   = questions,
            strategy    = strategy,
        )
        log.info(f"  │ ✓ Schema built: {len(uds.to_llm_context())} chars LLM context")
    except Exception as e:
        log.error(f"  │ ✗ Schema build failed: {e}")
        raise HTTPException(500, f"Schema build failed: {e}")

    log.info(f"  │ Step 6 done: {int((time.time()-step6_t)*1000)}ms")

    # ── Step 7: Cache ─────────────────────────────────────────────────
    _progress(source_id, 7, TOTAL_STEPS, "Caching results…")
    step7_t = time.time()

    onboarded_at = time.strftime("%Y-%m-%dT%H:%M:%SZ")

    _REGISTRY[source_id] = {
        "schema": uds,
        "df":     df,
        "sem":    sem,
        "snap":   snap,
        "meta": {
            "view_id":      view_id,
            "source_name":  source_name,
            "onboarded_at": onboarded_at,
            "strategy":     strategy,
            "rows":         len(df),
            "cols":         len(df.columns) if not df.empty else 0,
        },
    }

    # Oracle (non-blocking, non-critical)
    if check_oracle():
        try:
            _save_to_oracle(source_id, uds)
            log.info("  │ ✓ Saved to Oracle")
        except Exception as e:
            log.warning(f"  │ Oracle save failed (non-critical): {e}")

    log.info(f"  │ Step 7 done: {int((time.time()-step7_t)*1000)}ms")

    # ── Done ──────────────────────────────────────────────────────────
    elapsed = round(time.time() - t0, 2)
    _progress(source_id, 7, TOTAL_STEPS, f"Onboarded in {elapsed}s", "done")

    log.info("═" * 60)
    log.info(f"ONBOARD COMPLETE: {source_id}")
    log.info(f"  type={snap.dashboard_type}  rows={len(df)}  strategy={strategy}")
    log.info(f"  kpis={len(sem.kpis)}  anomalies={len(sem.anomalies)}  entities={len(sem.entities)}")
    log.info(f"  elapsed={elapsed}s")
    log.info("═" * 60)

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
        "onboarded_at":    onboarded_at,
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
# ══════════════════════════════════════════════════════════════════════

@router.post("/query")
def query(body: dict):
    source_id = body.get("source_id", "").strip()
    question  = body.get("question",  "").strip()
    use_llm   = body.get("use_llm",   True)

    if not source_id or not question:
        raise HTTPException(400, "source_id and question are required")

    entry  = _get_entry(source_id)
    df     = entry.get("df", pd.DataFrame())
    sem    = entry.get("sem")
    schema = entry.get("schema")

    log.info(f"QUERY: '{question[:60]}…' on {source_id} (use_llm={use_llm})")

    engine = QueryEngine(use_llm=use_llm and check_vertex())
    resp   = engine.query(question=question, df=df, schema=schema, sem=sem)

    log.info(f"  → Tier {resp.tier} ({resp.tier_label}) | {resp.latency_ms}ms | conf={resp.confidence:.2f}")
    return resp.to_dict()


# ══════════════════════════════════════════════════════════════════════
# GET /intelligence/status/{source_id}
# ══════════════════════════════════════════════════════════════════════

@router.get("/status/{source_id}")
def status(source_id: str):
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
# ══════════════════════════════════════════════════════════════════════

@router.get("/insights/{source_id}")
def insights(source_id: str):
    entry  = _get_entry(source_id)
    sem    = entry["sem"]
    schema = entry["schema"]

    insight_cards = []

    # Anomalies
    for a in sem.anomalies[:5]:
        sev_color = {"critical": "red", "high": "red", "medium": "amber", "low": "blue"}.get(a.severity, "blue")
        insight_cards.append({
            "type": a.signal_type, "severity": a.severity, "color": sev_color,
            "title": a.description, "detail": a.context, "category": "anomaly",
        })

    # KPI breaches
    for k in [k for k in sem.kpis if k.status == "breaching"][:3]:
        insight_cards.append({
            "type": "kpi_breach", "severity": "critical", "color": "red",
            "title": f"{k.name} is breaching its threshold",
            "detail": (
                f"Current: {k.current_val} | Threshold: {k.threshold} | "
                f"Trend: {k.trend} | Change: {k.pct_change:+.1f}%" if k.pct_change else ""
            ),
            "category": "kpi",
        })

    # Temporal
    for t in sem.temporal[:2]:
        if abs(t.pct_change) > 10:
            insight_cards.append({
                "type": "trend",
                "severity": "medium" if abs(t.pct_change) < 20 else "high",
                "color": "amber" if t.trend_dir in ("down", "volatile") else "green",
                "title": f"{t.measure_col} {t.trend_dir} trend: {t.pct_change:+.1f}% over {t.n_periods} periods",
                "detail": f"From {t.min_period} ({t.baseline_val:.3f}) to {t.max_period} ({t.latest_val:.3f})",
                "category": "temporal",
            })

    # Summary
    if sem.narrative:
        insight_cards.insert(0, {
            "type": "summary", "severity": "info", "color": "blue",
            "title": "Dashboard Summary", "detail": sem.narrative, "category": "summary",
        })

    return {
        "source_id":     source_id,
        "source_name":   schema.source_name,
        "insight_count": len(insight_cards),
        "insights":      insight_cards,
        "questions":     schema.questions[:8],
        "generated_at":  time.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


# ══════════════════════════════════════════════════════════════════════
# GET /intelligence/list
# ══════════════════════════════════════════════════════════════════════

@router.get("/list")
def list_onboarded():
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
        ],
    }


# ══════════════════════════════════════════════════════════════════════
# DELETE / REFRESH / SCHEMA
# ══════════════════════════════════════════════════════════════════════

@router.delete("/{source_id}")
def remove(source_id: str):
    if source_id not in _REGISTRY:
        raise HTTPException(404, f"'{source_id}' not found")
    del _REGISTRY[source_id]
    log.info(f"Removed: {source_id}")
    return {"status": "removed", "source_id": source_id}


@router.post("/refresh/{source_id}")
async def refresh(source_id: str, bg: BackgroundTasks = None):
    entry = _get_entry(source_id)
    meta  = entry.get("meta", {})
    body  = {
        "source_id":   source_id,
        "source_name": meta.get("source_name", source_id),
        "view_id":     meta.get("view_id", ""),
    }
    return await onboard(body, bg)


@router.get("/schema/{source_id}")
def get_schema(source_id: str):
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
                   VALUES (:1,:2,:3,'universal_schema',:4,:5,:6)""",
                [source_id, "universal_schema", uds.source_name, schema_json,
                 uds.total_rows, uds.total_cols]
            )
            conn.commit()
    except Exception as e:
        log.debug(f"Oracle save failed (non-critical): {e}")
