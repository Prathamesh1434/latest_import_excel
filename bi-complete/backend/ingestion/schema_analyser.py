"""
ingestion/schema_analyser.py

Automatically analyses ANY Tableau DataFrame and infers:
  - Dimensions (categorical grouping fields)
  - Measures (numeric KPI/metric fields)
  - Time fields (dates, months, periods)
  - RAG/Status fields (traffic light indicators)
  - Hierarchy relationships (e.g. Region > Country > City)
  - KPI patterns (current vs threshold vs target)
  - Trend potential (time-series candidates)

Zero hardcoding. Works on KRI dashboards, scorecards, sales,
compliance, operations — any structured Tableau export.

Packages: pandas==2.0.3  numpy==1.26.4
"""

from __future__ import annotations

import re
import logging
import numpy as np
import pandas as pd
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Tuple, Set, Any

log = logging.getLogger("schema_analyser")


# ─────────────────────────────────────────────────────────────
# SCHEMA MODELS
# ─────────────────────────────────────────────────────────────

@dataclass
class ColumnProfile:
    """Profile of a single column."""
    name:        str
    dtype:       str
    role:        str          # dimension | measure | time | status | id | text
    sub_role:    str          # e.g. kpi_value | threshold | target | rag | date | month
    n_unique:    int
    null_pct:    float
    sample_vals: List[Any]
    stats:       Dict[str, float] = field(default_factory=dict)
    is_numeric:  bool = False
    is_temporal: bool = False
    is_boolean:  bool = False


@dataclass
class KPIPattern:
    """A detected KPI group — value + optional threshold/target/status."""
    kpi_name:        str
    value_col:       str
    threshold_col:   Optional[str] = None
    target_col:      Optional[str] = None
    status_col:      Optional[str] = None
    previous_col:    Optional[str] = None   # for variance
    is_percentage:   bool = False
    breach_detected: bool = False
    breach_count:    int  = 0


@dataclass
class SchemaProfile:
    """
    Complete auto-inferred schema profile for one Tableau view.
    Produced by SchemaAnalyser.analyse(df).
    """
    source_id:       str
    source_name:     str
    total_rows:      int
    total_cols:      int

    # Column classifications
    columns:         List[ColumnProfile]
    dimensions:      List[str]    # categorical grouping fields
    measures:        List[str]    # numeric metric fields
    time_cols:       List[str]    # date / month / period fields
    status_cols:     List[str]    # RAG / status / flag fields
    id_cols:         List[str]    # ID / key fields (not groupable)
    text_cols:       List[str]    # free-text description fields

    # Detected patterns
    kpi_patterns:    List[KPIPattern]
    hierarchies:     List[List[str]]   # e.g. [["Region","Country","City"]]
    has_time_series: bool
    has_rag_status:  bool
    has_thresholds:  bool
    has_variance:    bool

    # Dashboard type inference
    dashboard_type:  str           # kri | scorecard | sales | ops | compliance | generic
    confidence:      float         # 0.0–1.0

    # Auto-generated summary for LLM
    schema_summary:  str

    def col(self, name: str) -> Optional[ColumnProfile]:
        return next((c for c in self.columns if c.name == name), None)


# ─────────────────────────────────────────────────────────────
# KEYWORD DICTIONARIES  (domain-agnostic patterns)
# ─────────────────────────────────────────────────────────────

_TIME_PATTERNS   = re.compile(
    r"date|month|period|quarter|year|week|day|time|fiscal|fy|qtr|dt$",
    re.I
)
_STATUS_PATTERNS = re.compile(
    r"rag|status|flag|indicator|signal|alert|health|state|colour|color|"
    r"traffic|light|band|tier|category|rating|grade|score_band",
    re.I
)
_ID_PATTERNS     = re.compile(
    r"^id$|_id$|code$|key$|ref$|num$|number$|no$|index$|pk$|guid|uuid",
    re.I
)
_THRESHOLD_PATTERNS = re.compile(
    r"threshold|limit|target|benchmark|ceiling|floor|min|max|bound|sla|"
    r"tolerance|acceptable|redline|greenline|kpi_target",
    re.I
)
_PREVIOUS_PATTERNS  = re.compile(
    r"prev|previous|prior|last|before|baseline|period_before|old|historic",
    re.I
)
_CURRENT_PATTERNS   = re.compile(
    r"curr|current|actual|value|result|measure|metric|score|rate|pct|percent|"
    r"ratio|count|amount|volume|total",
    re.I
)

# RAG status canonical values
_RAG_VALUES = {
    "red", "amber", "green", "grey", "gray", "n/a", "na",
    "r", "a", "g", "fail", "pass", "breach", "compliant",
    "high", "medium", "low", "critical", "warning", "ok",
    "yes", "no", "true", "false", "1", "0",
}

# Dashboard type keyword signals
_DASH_SIGNALS = {
    "kri":        ["kri", "risk indicator", "key risk", "uk-k", "risk_id", "risk_name"],
    "scorecard":  ["scorecard", "score_card", "kpi", "performance", "metric"],
    "compliance": ["control", "compliance", "breach", "audit", "regulation", "sox", "policy"],
    "sales":      ["revenue", "sales", "pipeline", "quota", "deal", "opportunity", "customer"],
    "ops":        ["sla", "incident", "ticket", "ops", "operation", "uptime", "latency"],
    "finance":    ["p&l", "budget", "forecast", "variance", "actual", "cost", "expense"],
}


# ─────────────────────────────────────────────────────────────
# ANALYSER
# ─────────────────────────────────────────────────────────────

class SchemaAnalyser:
    """
    Automatically profiles any Tableau DataFrame.

    Usage:
        analyser = SchemaAnalyser()
        profile  = analyser.analyse(df, source_id="my-dash", source_name="Sales Q1")
        print(profile.dashboard_type)    # e.g. "sales"
        print(profile.kpi_patterns)      # detected KPIs
        print(profile.schema_summary)    # LLM-ready text
    """

    def analyse(
        self,
        df:          pd.DataFrame,
        source_id:   str = "unknown",
        source_name: str = "",
    ) -> SchemaProfile:

        if df.empty:
            return self._empty_profile(source_id, source_name)

        df = df.copy()

        # Step 1 — Profile every column
        col_profiles = [self._profile_column(df, col) for col in df.columns]

        # Step 2 — Classify columns by role
        dims     = [c.name for c in col_profiles if c.role == "dimension"]
        measures = [c.name for c in col_profiles if c.role == "measure"]
        time_c   = [c.name for c in col_profiles if c.role == "time"]
        status_c = [c.name for c in col_profiles if c.role == "status"]
        id_c     = [c.name for c in col_profiles if c.role == "id"]
        text_c   = [c.name for c in col_profiles if c.role == "text"]

        # Step 3 — Detect KPI patterns
        kpi_patterns = self._detect_kpi_patterns(df, col_profiles, measures, status_c)

        # Step 4 — Detect hierarchies
        hierarchies = self._detect_hierarchies(df, dims)

        # Step 5 — Infer dashboard type
        dash_type, confidence = self._infer_dashboard_type(
            df, col_profiles, kpi_patterns
        )

        # Step 6 — Flags
        has_ts  = len(time_c) > 0
        has_rag = len(status_c) > 0
        has_thr = any(p.threshold_col for p in kpi_patterns)
        has_var = any(p.previous_col  for p in kpi_patterns)

        profile = SchemaProfile(
            source_id       = source_id,
            source_name     = source_name or source_id,
            total_rows      = len(df),
            total_cols      = len(df.columns),
            columns         = col_profiles,
            dimensions      = dims,
            measures        = measures,
            time_cols       = time_c,
            status_cols     = status_c,
            id_cols         = id_c,
            text_cols       = text_c,
            kpi_patterns    = kpi_patterns,
            hierarchies     = hierarchies,
            has_time_series = has_ts,
            has_rag_status  = has_rag,
            has_thresholds  = has_thr,
            has_variance    = has_var,
            dashboard_type  = dash_type,
            confidence      = confidence,
            schema_summary  = "",
        )
        profile.schema_summary = self._build_summary(profile, df)
        return profile

    # ── Column profiler ───────────────────────────────────────────────────

    def _profile_column(self, df: pd.DataFrame, col: str) -> ColumnProfile:
        series   = df[col]
        n        = len(series)
        n_null   = series.isna().sum()
        n_unique = series.nunique()
        null_pct = round(n_null / n * 100, 1) if n > 0 else 0
        sample   = series.dropna().head(5).tolist()
        name_lo  = col.lower()

        is_num    = pd.api.types.is_numeric_dtype(series)
        is_bool   = pd.api.types.is_bool_dtype(series)
        dtype_str = str(series.dtype)

        stats = {}
        if is_num and n_unique > 1:
            stats = {
                "min":  float(series.min()),
                "max":  float(series.max()),
                "mean": round(float(series.mean()), 4),
                "std":  round(float(series.std()),  4),
            }

        # Determine role
        role, sub_role = self._classify_role(
            col, series, n_unique, n, is_num, is_bool, sample
        )

        return ColumnProfile(
            name        = col,
            dtype       = dtype_str,
            role        = role,
            sub_role    = sub_role,
            n_unique    = n_unique,
            null_pct    = null_pct,
            sample_vals = sample,
            stats       = stats,
            is_numeric  = is_num,
            is_temporal = role == "time",
            is_boolean  = is_bool,
        )

    def _classify_role(
        self, col: str, series: pd.Series, n_unique: int,
        n_rows: int, is_num: bool, is_bool: bool, sample: List
    ) -> Tuple[str, str]:
        name_lo = col.lower()

        # ── Time ──
        if _TIME_PATTERNS.search(name_lo):
            return "time", "date" if "date" in name_lo else "period"

        # ── ID / key ──
        if _ID_PATTERNS.search(name_lo) or (
            not is_num and n_unique == n_rows and n_rows > 10
        ):
            return "id", "key"

        # ── Status / RAG ──
        if _STATUS_PATTERNS.search(name_lo):
            return "status", "rag"
        if not is_num:
            sample_lo = {str(v).lower() for v in sample}
            if sample_lo and sample_lo.issubset(_RAG_VALUES) and n_unique <= 10:
                return "status", "rag"

        # ── Numeric measures ──
        if is_num:
            if _THRESHOLD_PATTERNS.search(name_lo):
                return "measure", "threshold"
            if _PREVIOUS_PATTERNS.search(name_lo):
                return "measure", "previous"
            if _CURRENT_PATTERNS.search(name_lo) or n_unique > 10:
                return "measure", "kpi_value"
            return "measure", "metric"

        # ── Boolean ──
        if is_bool or n_unique <= 2:
            return "status", "flag"

        # ── Long text ──
        avg_len = series.dropna().astype(str).str.len().mean()
        if avg_len and avg_len > 80:
            return "text", "description"

        # ── Dimension (low cardinality categorical) ──
        cardinality_ratio = n_unique / n_rows if n_rows > 0 else 1
        if cardinality_ratio < 0.3 or n_unique <= 50:
            return "dimension", "category"

        return "text", "freetext"

    # ── KPI pattern detection ─────────────────────────────────────────────

    def _detect_kpi_patterns(
        self,
        df:          pd.DataFrame,
        col_profiles:List[ColumnProfile],
        measures:    List[str],
        status_cols: List[str],
    ) -> List[KPIPattern]:
        patterns = []
        profiled = {c.name: c for c in col_profiles}

        # Find threshold and previous columns
        threshold_cols = [m for m in measures if profiled[m].sub_role == "threshold"]
        previous_cols  = [m for m in measures if profiled[m].sub_role == "previous"]
        value_cols     = [m for m in measures if profiled[m].sub_role in ("kpi_value", "metric")]

        # Strategy 1: Match by name similarity
        # e.g. CURRENT_VALUE + RED_THRESHOLD + RAG_STATUS
        for val_col in value_cols:
            thr_col  = self._find_partner(val_col, threshold_cols, ["threshold","limit","target","sla"])
            prev_col = self._find_partner(val_col, previous_cols,  ["previous","prior","last","baseline"])
            stat_col = status_cols[0] if status_cols else None

            # Check if values look like percentages
            col_data     = df[val_col].dropna()
            is_pct       = bool(col_data.between(0, 1.01).all() and col_data.mean() < 1)
            breach_count = 0

            # Detect breaches if threshold exists
            if thr_col:
                thr_data = df[thr_col].dropna()
                if len(thr_data) > 0:
                    # Could be < or > threshold — try both
                    breach_count = int((col_data < thr_data).sum() or (col_data > thr_data).sum())

            # Try to find a name column for this KPI
            name_col = next(
                (c.name for c in col_profiles
                 if c.role in ("id","dimension")
                 and any(k in c.name.lower() for k in ["name","id","kri","metric","indicator"])),
                None
            )

            kpi_name = f"{val_col}" if not name_col else f"KPI ({val_col})"
            if name_col and df[name_col].nunique() > 1:
                # Multiple KPIs in rows
                for kpi_val in df[name_col].dropna().unique():
                    sub_df = df[df[name_col] == kpi_val]
                    sub_breach = 0
                    if thr_col and val_col in sub_df.columns and thr_col in sub_df.columns:
                        v = sub_df[val_col].dropna()
                        t = sub_df[thr_col].dropna()
                        if len(v) and len(t):
                            sub_breach = int((v < t).sum() or (v > t).sum())
                    patterns.append(KPIPattern(
                        kpi_name      = str(kpi_val),
                        value_col     = val_col,
                        threshold_col = thr_col,
                        previous_col  = prev_col,
                        status_col    = stat_col,
                        is_percentage = is_pct,
                        breach_detected= sub_breach > 0,
                        breach_count  = sub_breach,
                    ))
                break   # avoid duplicates
            else:
                patterns.append(KPIPattern(
                    kpi_name      = kpi_name,
                    value_col     = val_col,
                    threshold_col = thr_col,
                    previous_col  = prev_col,
                    status_col    = stat_col,
                    is_percentage = is_pct,
                    breach_detected= breach_count > 0,
                    breach_count  = breach_count,
                ))

        return patterns

    def _find_partner(self, val_col: str, candidates: List[str], keywords: List[str]) -> Optional[str]:
        """Find a related column by name proximity."""
        if not candidates:
            return None
        val_lo = val_col.lower()
        # Prefer columns sharing a prefix with val_col
        for cand in candidates:
            cand_lo = cand.lower()
            if any(kw in cand_lo for kw in keywords):
                prefix = val_lo[:4]   # first 4 chars
                if prefix in cand_lo or cand_lo[:4] in val_lo:
                    return cand
        # Fall back to first keyword match
        return next((c for c in candidates if any(kw in c.lower() for kw in keywords)), None)

    # ── Hierarchy detection ───────────────────────────────────────────────

    def _detect_hierarchies(self, df: pd.DataFrame, dims: List[str]) -> List[List[str]]:
        """
        Detect dimension hierarchies by functional dependency.
        If every value of B maps to exactly one value of A → A contains B.
        """
        hierarchies = []
        for i, col_a in enumerate(dims):
            for col_b in dims[i+1:]:
                try:
                    b_per_a = df.groupby(col_b)[col_a].nunique().max()
                    a_per_b = df.groupby(col_a)[col_b].nunique().max()
                    if b_per_a == 1 and a_per_b > 1:
                        hierarchies.append([col_a, col_b])
                    elif a_per_b == 1 and b_per_a > 1:
                        hierarchies.append([col_b, col_a])
                except Exception:
                    pass
        return hierarchies

    # ── Dashboard type inference ──────────────────────────────────────────

    def _infer_dashboard_type(
        self,
        df:           pd.DataFrame,
        col_profiles: List[ColumnProfile],
        kpi_patterns: List[KPIPattern],
    ) -> Tuple[str, float]:
        all_text = " ".join(
            c.name.lower() + " " + " ".join(str(v).lower() for v in c.sample_vals)
            for c in col_profiles
        )
        scores = {}
        for dash_type, signals in _DASH_SIGNALS.items():
            score = sum(1 for s in signals if s in all_text)
            scores[dash_type] = score

        if not any(scores.values()):
            return "generic", 0.5

        best_type  = max(scores, key=scores.get)
        total_sig  = sum(_DASH_SIGNALS[best_type].__len__() for _ in [1])
        confidence = min(1.0, scores[best_type] / max(len(_DASH_SIGNALS[best_type]), 1))
        return best_type, round(confidence, 2)

    # ── Schema summary (LLM-ready text) ───────────────────────────────────

    def _build_summary(self, profile: SchemaProfile, df: pd.DataFrame) -> str:
        lines = [
            f"=== SCHEMA PROFILE: {profile.source_name} ===",
            f"Dashboard type: {profile.dashboard_type} (confidence: {profile.confidence})",
            f"Shape: {profile.total_rows} rows × {profile.total_cols} columns",
            "",
            "DIMENSIONS (grouping fields):",
        ]
        for d in profile.dimensions:
            cp = profile.col(d)
            lines.append(f"  {d}: {cp.n_unique} unique values, sample: {cp.sample_vals[:3]}")

        lines.append("\nMEASURES (numeric KPIs):")
        for m in profile.measures:
            cp = profile.col(m)
            if cp.stats:
                lines.append(f"  {m}: min={cp.stats.get('min','?')} max={cp.stats.get('max','?')} mean={cp.stats.get('mean','?')}")

        if profile.time_cols:
            lines.append(f"\nTIME FIELDS: {profile.time_cols}")
            for tc in profile.time_cols:
                vals = sorted(df[tc].dropna().astype(str).unique().tolist())
                lines.append(f"  {tc}: {vals[:6]}")

        if profile.status_cols:
            lines.append(f"\nSTATUS / RAG FIELDS: {profile.status_cols}")
            for sc in profile.status_cols:
                dist = df[sc].value_counts().to_dict()
                lines.append(f"  {sc}: {dist}")

        if profile.kpi_patterns:
            lines.append(f"\nDETECTED KPI PATTERNS ({len(profile.kpi_patterns)}):")
            for kp in profile.kpi_patterns[:10]:
                breach_note = f" ⚠ {kp.breach_count} breaches" if kp.breach_detected else ""
                lines.append(
                    f"  {kp.kpi_name}: value={kp.value_col}"
                    + (f" threshold={kp.threshold_col}" if kp.threshold_col else "")
                    + (f" prev={kp.previous_col}"       if kp.previous_col  else "")
                    + (f" status={kp.status_col}"       if kp.status_col    else "")
                    + (f" [%]" if kp.is_percentage else "")
                    + breach_note
                )

        if profile.hierarchies:
            lines.append("\nHIERARCHIES:")
            for h in profile.hierarchies:
                lines.append(f"  {' > '.join(h)}")

        flags = []
        if profile.has_time_series: flags.append("time-series capable")
        if profile.has_rag_status:  flags.append("RAG status present")
        if profile.has_thresholds:  flags.append("thresholds detected")
        if profile.has_variance:    flags.append("variance calculation available")
        if flags:
            lines.append(f"\nCAPABILITIES: {', '.join(flags)}")

        return "\n".join(lines)

    def _empty_profile(self, source_id: str, source_name: str) -> SchemaProfile:
        return SchemaProfile(
            source_id="", source_name=source_name, total_rows=0, total_cols=0,
            columns=[], dimensions=[], measures=[], time_cols=[], status_cols=[],
            id_cols=[], text_cols=[], kpi_patterns=[], hierarchies=[],
            has_time_series=False, has_rag_status=False,
            has_thresholds=False, has_variance=False,
            dashboard_type="generic", confidence=0.0,
            schema_summary="Empty dataset.",
        )
