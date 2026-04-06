"""
intelligence/dashboard_schema.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Universal JSON schema for any Tableau dashboard.

Combines:
  • DashboardSnapshot  (from visual_extractor — structure + aggregations)
  • SemanticProfile    (from semantic_layer   — meaning + intelligence)

Into a single portable, version-stamped JSON document that:
  • Can be stored in Oracle (BI_DATA_CONTEXT table)
  • Can be serialised/deserialised without losing intelligence
  • Serves as the single source of truth for the chatbot
  • Is human-readable and auditable
"""
from __future__ import annotations

import json
import time
import logging
from dataclasses import dataclass, field, asdict
from typing import Any, Dict, List, Optional

log = logging.getLogger("dashboard_schema")

SCHEMA_VERSION = "2.0"


@dataclass
class UniversalDashboardSchema:
    """
    The complete, portable representation of any connected dashboard.

    This is what gets stored in Oracle and served to the LLM.
    Everything the chatbot needs to answer questions lives here.
    """
    # ── Identity ─────────────────────────────────────────────
    source_id:      str
    source_name:    str
    view_id:        str
    schema_version: str = SCHEMA_VERSION

    # ── Extraction metadata ───────────────────────────────────
    extracted_at:   str = ""           # ISO timestamp
    extraction_strategy: str = ""      # csv | rest | image
    total_rows:     int = 0
    total_cols:     int = 0
    all_columns:    List[str] = field(default_factory=list)

    # ── Visual intelligence ───────────────────────────────────
    dashboard_type:    str = ""        # human label of primary visual type
    primary_vtype:     str = ""        # vtype constant (e.g. time_series)
    alt_vtypes:        List[Dict] = field(default_factory=list)
    has_time_series:   bool = False
    has_geo:           bool = False
    has_rag:           bool = False
    has_threshold:     bool = False
    has_multi_measure: bool = False

    # ── Semantic intelligence ─────────────────────────────────
    fingerprint:    str = ""
    narrative:      str = ""
    kpis:           List[Dict] = field(default_factory=list)
    anomalies:      List[Dict] = field(default_factory=list)
    temporal:       List[Dict] = field(default_factory=list)
    entities:       List[Dict] = field(default_factory=list)
    relationships:  List[Dict] = field(default_factory=list)

    # ── Schema profile ────────────────────────────────────────
    schema_text:    str = ""           # human-readable schema
    dimensions:     List[str] = field(default_factory=list)
    measures:       List[str] = field(default_factory=list)
    time_cols:      List[str] = field(default_factory=list)
    status_cols:    List[str] = field(default_factory=list)

    # ── Pre-computed aggregations ─────────────────────────────
    agg_blocks:     List[Dict] = field(default_factory=list)  # top N blocks
    agg_count:      int = 0

    # ── Auto-generated questions ──────────────────────────────
    questions:      List[str] = field(default_factory=list)

    def to_json(self, indent: int = 2) -> str:
        """Serialise to JSON — everything except the TF-IDF matrix."""
        data = {
            "schema_version":   self.schema_version,
            "source_id":        self.source_id,
            "source_name":      self.source_name,
            "view_id":          self.view_id,
            "extracted_at":     self.extracted_at,
            "extraction_strategy": self.extraction_strategy,
            "total_rows":       self.total_rows,
            "total_cols":       self.total_cols,
            "all_columns":      self.all_columns,
            "dashboard_type":   self.dashboard_type,
            "primary_vtype":    self.primary_vtype,
            "alt_vtypes":       self.alt_vtypes[:3],
            "capabilities": {
                "time_series":   self.has_time_series,
                "geographic":    self.has_geo,
                "rag_status":    self.has_rag,
                "thresholds":    self.has_threshold,
                "multi_measure": self.has_multi_measure,
            },
            "fingerprint":   self.fingerprint,
            "narrative":     self.narrative,
            "dimensions":    self.dimensions,
            "measures":      self.measures,
            "time_cols":     self.time_cols,
            "status_cols":   self.status_cols,
            "schema_text":   self.schema_text,
            "kpis":          self.kpis,
            "anomalies":     self.anomalies,
            "temporal":      self.temporal,
            "entities":      self.entities[:15],
            "relationships": self.relationships[:10],
            "agg_count":     self.agg_count,
            "agg_blocks":    self.agg_blocks[:8],
            "questions":     self.questions,
        }
        return json.dumps(data, indent=indent, default=str)

    @classmethod
    def from_json(cls, raw: str) -> "UniversalDashboardSchema":
        """Deserialise from JSON."""
        data = json.loads(raw)
        caps = data.get("capabilities", {})
        return cls(
            source_id         = data.get("source_id", ""),
            source_name       = data.get("source_name", ""),
            view_id           = data.get("view_id", ""),
            schema_version    = data.get("schema_version", SCHEMA_VERSION),
            extracted_at      = data.get("extracted_at", ""),
            extraction_strategy=data.get("extraction_strategy",""),
            total_rows        = data.get("total_rows", 0),
            total_cols        = data.get("total_cols", 0),
            all_columns       = data.get("all_columns", []),
            dashboard_type    = data.get("dashboard_type", ""),
            primary_vtype     = data.get("primary_vtype", ""),
            alt_vtypes        = data.get("alt_vtypes", []),
            has_time_series   = caps.get("time_series", False),
            has_geo           = caps.get("geographic", False),
            has_rag           = caps.get("rag_status", False),
            has_threshold     = caps.get("thresholds", False),
            has_multi_measure = caps.get("multi_measure", False),
            fingerprint       = data.get("fingerprint", ""),
            narrative         = data.get("narrative", ""),
            dimensions        = data.get("dimensions", []),
            measures          = data.get("measures", []),
            time_cols         = data.get("time_cols", []),
            status_cols       = data.get("status_cols", []),
            schema_text       = data.get("schema_text", ""),
            kpis              = data.get("kpis", []),
            anomalies         = data.get("anomalies", []),
            temporal          = data.get("temporal", []),
            entities          = data.get("entities", []),
            relationships     = data.get("relationships", []),
            agg_count         = data.get("agg_count", 0),
            agg_blocks        = data.get("agg_blocks", []),
            questions         = data.get("questions", []),
        )

    def to_llm_context(self, max_chars: int = 8000) -> str:
        """Build the full context string for LLM injection."""
        parts = [
            f"=== DASHBOARD: {self.source_name} ===",
            f"Type: {self.dashboard_type}  |  {self.total_rows:,} rows × {self.total_cols} columns",
            f"Extracted: {self.extracted_at}  |  Strategy: {self.extraction_strategy}",
            "",
            f"NARRATIVE: {self.narrative}",
            "",
        ]
        caps = []
        if self.has_time_series:   caps.append("Time-series capable")
        if self.has_rag:           caps.append("RAG/Status tracking")
        if self.has_threshold:     caps.append("Threshold breach detection")
        if self.has_geo:           caps.append("Geographic data")
        if self.has_multi_measure: caps.append("Multi-measure (Measure Names/Values)")
        if caps:
            parts.append(f"CAPABILITIES: {', '.join(caps)}\n")

        if self.kpis:
            parts.append("KPIs:")
            for k in self.kpis[:8]:
                status_sym = {"on_track":"✅","at_risk":"⚠️","breaching":"🔴"}.get(k.get("status",""),"❓")
                val  = k.get("current_val")
                thr  = k.get("threshold")
                chg  = k.get("pct_change")
                line = f"  {status_sym} {k.get('name','')} = {val}"
                if thr: line += f" [threshold: {thr}]"
                if chg: line += f" [{chg:+.1f}%]"
                line += f"  → {k.get('status','')} / {k.get('trend','')}"
                parts.append(line)
            parts.append("")

        if self.anomalies:
            parts.append("ANOMALIES & SIGNALS:")
            for a in self.anomalies[:5]:
                sev = {"critical":"🚨","high":"🔴","medium":"🟡"}.get(a.get("severity",""),"·")
                parts.append(f"  {sev} {a.get('description','')}")
            parts.append("")

        if self.temporal:
            parts.append("TIME PATTERNS:")
            for t in self.temporal[:3]:
                parts.append(
                    f"  {t.get('measure_col','')} over {t.get('time_col','')}: "
                    f"{t.get('trend_dir','')} trend, {t.get('pct_change',0):+.1f}% change, "
                    f"{t.get('n_periods',0)} periods"
                )
            parts.append("")

        parts.append(f"SCHEMA:\n{self.schema_text[:1000]}")
        parts.append("")

        if self.agg_blocks:
            parts.append("PRE-COMPUTED AGGREGATIONS:")
            for blk in self.agg_blocks[:4]:
                parts.append(f"  [{blk.get('agg_type','').upper()}] {blk.get('title','')}")
                for row in blk.get("data", [])[:5]:
                    parts.append("    " + "  |  ".join(f"{k}: {v}" for k, v in row.items()))
            parts.append("")

        text = "\n".join(parts)
        return text[:max_chars]


def build_schema(
    source_id:   str,
    source_name: str,
    view_id:     str,
    snap,           # DashboardSnapshot
    sem,            # SemanticProfile
    questions:   List[str],
    strategy:    str = "csv",
) -> UniversalDashboardSchema:
    """
    Factory: combine DashboardSnapshot + SemanticProfile → UniversalDashboardSchema.
    """
    from backend.ingestion.visual_extractor import DashboardSnapshot
    from backend.intelligence.semantic_layer import SemanticProfile

    extracted_at = time.strftime("%Y-%m-%dT%H:%M:%SZ")

    # Extract column role info from snap profiles
    dims     = [p.name for p in snap.col_profiles if p.role == "dimension"]
    meas     = [p.name for p in snap.col_profiles if p.role == "measure"]
    time_c   = [p.name for p in snap.col_profiles if p.role == "time"]
    status_c = [p.name for p in snap.col_profiles if p.role == "status"]

    # Serialise agg blocks (drop large row data to save space)
    agg_blocks_lite = []
    for blk in snap.agg_blocks[:8]:
        agg_blocks_lite.append({
            "agg_id":   blk.agg_id,
            "agg_type": blk.agg_type,
            "title":    blk.title,
            "data":     blk.data[:20],
        })

    return UniversalDashboardSchema(
        source_id     = source_id,
        source_name   = source_name,
        view_id       = view_id,
        extracted_at  = extracted_at,
        extraction_strategy = strategy,
        total_rows    = snap.total_rows,
        total_cols    = snap.total_cols,
        all_columns   = snap.all_columns,
        dashboard_type    = snap.dashboard_type,
        primary_vtype     = snap.visual_types[0].vtype if snap.visual_types else "",
        alt_vtypes        = [{"type":v.vtype,"confidence":v.confidence} for v in snap.visual_types[1:4]],
        has_time_series   = snap.has_time,
        has_geo           = snap.has_geo,
        has_rag           = snap.has_rag,
        has_threshold     = snap.has_threshold,
        has_multi_measure = snap.has_multi_measure,
        fingerprint   = sem.fingerprint,
        narrative     = sem.narrative,
        dimensions    = dims,
        measures      = meas,
        time_cols     = time_c,
        status_cols   = status_c,
        schema_text   = snap.schema_text,
        kpis          = [{"name":k.name,"col":k.col,"current_val":k.current_val,
                          "prev_val":k.prev_val,"threshold":k.threshold,
                          "direction":k.direction,"status":k.status,"trend":k.trend,
                          "pct_change":k.pct_change,"percentile":k.percentile}
                         for k in sem.kpis],
        anomalies     = [{"signal_type":a.signal_type,"severity":a.severity,
                          "description":a.description,"col":a.col,
                          "affected_rows":a.affected_rows,"context":a.context}
                         for a in sem.anomalies],
        temporal      = [{"time_col":t.time_col,"measure_col":t.measure_col,
                          "trend_dir":t.trend_dir,"trend_slope":t.trend_slope,
                          "min_period":t.min_period,"max_period":t.max_period,
                          "n_periods":t.n_periods,"latest_val":t.latest_val,
                          "pct_change":t.pct_change,"is_seasonal":t.is_seasonal}
                         for t in sem.temporal],
        entities      = [{"name":e.name,"entity_type":e.entity_type,
                          "sample_values":e.sample_values,"importance":e.importance}
                         for e in sem.entities],
        relationships = [{"col_a":r.col_a,"col_b":r.col_b,"rel_type":r.rel_type,
                          "strength":r.strength,"note":r.note}
                         for r in sem.relationships],
        agg_count     = len(snap.agg_blocks),
        agg_blocks    = agg_blocks_lite,
        questions     = questions,
    )
