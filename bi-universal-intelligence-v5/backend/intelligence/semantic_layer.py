"""
intelligence/semantic_layer.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Semantic understanding layer for any Tableau dashboard.

Transforms raw data (DataFrame + SchemaProfile) into a rich
semantic model that an LLM can reason about effectively.

What it produces:
  • Entity recognition  — the "things" in the data (KRIs, regions, products…)
  • KPI identification  — which measures are the key performance indicators
  • Relationship map    — how columns relate (hierarchy, threshold, variance)
  • Temporal patterns   — trend direction, period-over-period, seasonality
  • Anomaly scores      — what's noteworthy right now (outliers, breaches)
  • TF-IDF index        — semantic search over every data value + column name
  • Narrative summary   — plain-English paragraph for LLM context

All computation is offline (no network calls).
Packages: pandas==2.0.3  scikit-learn==1.7.2  numpy==1.26.4
"""
from __future__ import annotations

import re
import json
import logging
import hashlib
import numpy as np
import pandas as pd
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

log = logging.getLogger("semantic_layer")

# ──────────────────────────────────────────────────────────────
#  DATA MODELS
# ──────────────────────────────────────────────────────────────

@dataclass
class Entity:
    """A recognised 'thing' in the dataset (KRI, product, region…)."""
    name:         str          # canonical label
    entity_type:  str          # kri | metric | dimension | geographic | time | person | id
    source_col:   str          # column it was detected in
    sample_values:List[str]
    frequency:    int          # how many rows reference this entity
    importance:   float        # 0–1 score based on column role + cardinality


@dataclass
class KPISignal:
    """A detected KPI with its performance context."""
    name:         str
    col:          str
    current_val:  Optional[float]
    prev_val:     Optional[float]
    threshold:    Optional[float]
    direction:    str          # higher_is_better | lower_is_better | neutral
    status:       str          # on_track | at_risk | breaching | unknown
    trend:        str          # improving | deteriorating | stable | insufficient_data
    pct_change:   Optional[float]
    percentile:   Optional[float]  # where current_val sits in historical range


@dataclass
class Relationship:
    """A detected relationship between two columns."""
    col_a:    str
    col_b:    str
    rel_type: str   # threshold | variance | hierarchy | correlation | time_series
    strength: float # 0–1
    note:     str


@dataclass
class TemporalPattern:
    """Detected time pattern in the data."""
    time_col:   str
    measure_col:str
    trend_dir:  str       # up | down | flat | volatile
    trend_slope:float     # per period
    min_period: str
    max_period: str
    n_periods:  int
    latest_val: float
    baseline_val:float
    pct_change: float
    is_seasonal:bool


@dataclass
class AnomalySignal:
    """Something noteworthy in the current data."""
    signal_type: str   # breach | outlier | trend_reversal | missing | spike | drop
    severity:    str   # critical | high | medium | low
    description: str
    col:         str
    affected_rows:int
    value:       Any
    context:     str


@dataclass
class SemanticProfile:
    """
    Complete semantic model of a Tableau dashboard.
    Produced by SemanticLayer.analyse().
    """
    source_id:      str
    source_name:    str
    fingerprint:    str            # MD5 of column names + row count
    entities:       List[Entity]
    kpis:           List[KPISignal]
    relationships:  List[Relationship]
    temporal:       List[TemporalPattern]
    anomalies:      List[AnomalySignal]
    narrative:      str            # plain-English paragraph for LLM
    semantic_index: Optional[Any]  # fitted TfidfVectorizer (not serialised)
    index_docs:     List[str]      # documents the index was built on
    index_meta:     List[Dict]     # metadata per document
    build_ms:       int = 0

    def search(self, query: str, top_k: int = 5) -> List[Dict]:
        """
        Semantic search over the dashboard's data using TF-IDF cosine similarity.
        Returns ranked list of matching facts/cells.
        """
        if self.semantic_index is None or not self.index_docs:
            return []
        try:
            q_vec  = self.semantic_index.transform([query])
            scores = cosine_similarity(q_vec, self._doc_matrix).flatten()
            top_idx = scores.argsort()[::-1][:top_k]
            return [
                {**self.index_meta[i], "score": round(float(scores[i]), 3)}
                for i in top_idx if scores[i] > 0.05
            ]
        except Exception as e:
            log.debug(f"Semantic search failed: {e}")
            return []

    def get_llm_context(self, max_chars: int = 6000) -> str:
        """Structured context string for LLM injection."""
        parts = [
            f"=== SEMANTIC PROFILE: {self.source_name} ===",
            self.narrative,
            "",
        ]
        if self.kpis:
            parts.append("KEY PERFORMANCE INDICATORS:")
            for k in self.kpis[:8]:
                status_emoji = {"on_track":"✅","at_risk":"⚠️","breaching":"🔴","unknown":"❓"}.get(k.status,"❓")
                trend_emoji  = {"improving":"↑","deteriorating":"↓","stable":"→","insufficient_data":"·"}.get(k.trend,"·")
                val_str = f"{k.current_val:.3f}" if k.current_val is not None else "N/A"
                thr_str = f" [threshold: {k.threshold:.3f}]" if k.threshold else ""
                chg_str = f" [{k.pct_change:+.1f}% vs prev]" if k.pct_change is not None else ""
                parts.append(f"  {status_emoji} {k.name}: {val_str}{thr_str}{chg_str} {trend_emoji} {k.status} / {k.trend}")

        if self.anomalies:
            parts.append("\nNOTEWORTHY SIGNALS:")
            for a in self.anomalies[:6]:
                sev_emoji = {"critical":"🚨","high":"🔴","medium":"🟡","low":"🔵"}.get(a.severity,"·")
                parts.append(f"  {sev_emoji} [{a.signal_type}] {a.description}")

        if self.temporal:
            parts.append("\nTIME PATTERNS:")
            for t in self.temporal[:4]:
                parts.append(
                    f"  {t.measure_col} over {t.time_col}: {t.trend_dir} trend "
                    f"({t.pct_change:+.1f}% from {t.min_period} to {t.max_period}, {t.n_periods} periods)"
                )

        if self.relationships:
            parts.append("\nKEY RELATIONSHIPS:")
            for r in self.relationships[:5]:
                parts.append(f"  {r.col_a} ↔ {r.col_b}: {r.rel_type} [{r.note}]")

        if self.entities:
            by_type: Dict[str, List[str]] = {}
            for e in self.entities[:20]:
                by_type.setdefault(e.entity_type, []).append(e.name)
            for etype, names in list(by_type.items())[:4]:
                parts.append(f"  {etype.title()}s: {', '.join(names[:8])}")

        text = "\n".join(parts)
        return text[:max_chars]

    # Internal: store the sparse matrix for search
    _doc_matrix: Any = field(default=None, repr=False)


# ──────────────────────────────────────────────────────────────
#  SEMANTIC LAYER
# ──────────────────────────────────────────────────────────────

class SemanticLayer:
    """
    Analyses a DataFrame to produce a rich SemanticProfile.

    Usage:
        sl      = SemanticLayer()
        profile = sl.analyse(df, source_id="sales-q1", source_name="Sales Q1 2026")
        context = profile.get_llm_context()
        results = profile.search("which region is underperforming")
    """

    # Column name keyword maps for direction inference
    _HIGHER_BETTER = frozenset({
        "revenue","sales","profit","margin","score","rate","coverage",
        "completeness","accuracy","uptime","availability","satisfaction",
        "compliance","pass","green","target","achievement","growth"
    })
    _LOWER_BETTER  = frozenset({
        "error","failure","breach","violation","cost","expense","risk",
        "incident","ticket","lag","delay","churn","attrition","miss",
        "exception","defect","debt","issue","concern","open","outstanding",
        "overdue","red","amber","fail","outstanding","pending"
    })

    def analyse(
        self,
        df:          pd.DataFrame,
        source_id:   str  = "unknown",
        source_name: str  = "",
    ) -> SemanticProfile:
        import time
        t0 = time.time()

        if df is None or df.empty:
            return self._empty(source_id, source_name)

        df = df.copy().reset_index(drop=True)

        # ── 1. Classify columns ────────────────────────────────────────
        col_roles = self._classify_cols(df)

        # ── 2. Entity recognition ─────────────────────────────────────
        entities = self._extract_entities(df, col_roles)

        # ── 3. KPI identification ─────────────────────────────────────
        kpis = self._identify_kpis(df, col_roles)

        # ── 4. Relationship mapping ───────────────────────────────────
        relationships = self._map_relationships(df, col_roles)

        # ── 5. Temporal patterns ──────────────────────────────────────
        temporal = self._detect_temporal(df, col_roles)

        # ── 6. Anomaly detection ──────────────────────────────────────
        anomalies = self._detect_anomalies(df, col_roles, kpis)

        # ── 7. Narrative ──────────────────────────────────────────────
        narrative = self._build_narrative(
            df, source_name, col_roles, kpis, anomalies, temporal
        )

        # ── 8. TF-IDF semantic index ───────────────────────────────────
        index, docs, meta, matrix = self._build_index(df, col_roles)

        fp = hashlib.md5(f"{sorted(df.columns.tolist())}{len(df)}".encode()).hexdigest()[:12]

        profile = SemanticProfile(
            source_id      = source_id,
            source_name    = source_name or source_id,
            fingerprint    = fp,
            entities       = entities,
            kpis           = kpis,
            relationships  = relationships,
            temporal       = temporal,
            anomalies      = anomalies,
            narrative      = narrative,
            semantic_index = index,
            index_docs     = docs,
            index_meta     = meta,
            build_ms       = int((time.time() - t0) * 1000),
        )
        profile._doc_matrix = matrix

        log.info(
            f"SemanticLayer: {source_id} | {len(entities)} entities | "
            f"{len(kpis)} KPIs | {len(anomalies)} anomalies | "
            f"{len(docs)} index docs | {profile.build_ms}ms"
        )
        return profile

    # ──────────────────────────────────────────────────────────
    #  COLUMN CLASSIFICATION
    # ──────────────────────────────────────────────────────────

    def _classify_cols(self, df: pd.DataFrame) -> Dict[str, str]:
        """Returns {col_name: role} for every column."""
        roles = {}
        n = len(df)
        for col in df.columns:
            s      = df[col]
            col_lo = col.lower().strip()
            is_num = pd.api.types.is_numeric_dtype(s)
            n_uniq = s.nunique()

            if self._is_temporal(col, s):
                roles[col] = "time"
            elif self._is_geo(col_lo):
                roles[col] = "geo"
            elif self._is_threshold(col_lo) and is_num:
                roles[col] = "threshold"
            elif self._is_status(col, s):
                roles[col] = "status"
            elif self._is_id(col_lo, n_uniq, n):
                roles[col] = "id"
            elif is_num:
                roles[col] = "measure"
            elif n_uniq / max(n, 1) < 0.35 or n_uniq <= 50:
                roles[col] = "dimension"
            else:
                roles[col] = "text"
        return roles

    def _is_temporal(self, col: str, s: pd.Series) -> bool:
        if pd.api.types.is_datetime64_any_dtype(s):
            return True
        time_re = re.compile(r"(date|month|period|quarter|year|week|day|time|dt$|fy\d|q[1-4])", re.I)
        if time_re.search(col):
            return True
        if s.dtype == object:
            sample = s.dropna().astype(str).head(8)
            date_re = re.compile(
                r"^\d{4}[-/]\d{2}|^\d{2}[-/]\d{4}|"
                r"^(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[-\s]\d{2,4}|"
                r"^q[1-4][-\s]\d{4}|^fy\d{2,4}|^\d{4}$", re.I
            )
            return sum(1 for v in sample if date_re.match(str(v).strip())) >= min(3, len(sample))
        return False

    def _is_geo(self, col_lo: str) -> bool:
        geo_kw = {"country","state","province","city","region","latitude","longitude",
                  "lat","lng","lon","postal","zip","territory","continent","location"}
        return any(g in col_lo for g in geo_kw)

    def _is_threshold(self, col_lo: str) -> bool:
        return bool(re.search(r"threshold|limit|target|benchmark|sla|tolerance|redline|ceiling", col_lo))

    def _is_status(self, col: str, s: pd.Series) -> bool:
        col_lo = col.lower()
        if re.search(r"(rag|status|flag|color|colour|band|state|result|outcome|rating)", col_lo):
            return True
        rag_vals = {"red","amber","green","pass","fail","breach","compliant","ok","high","medium","low"}
        if s.dtype == object and s.nunique() <= 10:
            sample_lo = {str(v).lower() for v in s.dropna().head(10)}
            return bool(sample_lo & rag_vals)
        return False

    def _is_id(self, col_lo: str, n_uniq: int, n: int) -> bool:
        return (bool(re.search(r"^(id|pk|key|code|ref|uuid|guid)$|(_id|_key|_code)$", col_lo))
                and n_uniq > n * 0.8)

    # ──────────────────────────────────────────────────────────
    #  ENTITY RECOGNITION
    # ──────────────────────────────────────────────────────────

    def _extract_entities(self, df: pd.DataFrame, roles: Dict) -> List[Entity]:
        entities = []
        dim_cols = [c for c, r in roles.items() if r == "dimension"]

        for col in dim_cols[:8]:
            vc = df[col].value_counts()
            col_lo = col.lower()

            # Guess entity type from column name
            if any(k in col_lo for k in ["kri","kpi","indicator","metric","control","risk"]):
                etype = "kri"
            elif any(k in col_lo for k in ["country","state","city","region","territory"]):
                etype = "geographic"
            elif any(k in col_lo for k in ["product","category","segment","type","class"]):
                etype = "product"
            elif any(k in col_lo for k in ["team","owner","user","person","manager","analyst"]):
                etype = "person"
            elif any(k in col_lo for k in ["date","month","period","year","quarter"]):
                etype = "time"
            else:
                etype = "dimension"

            # Importance: combine column uniqueness ratio + coverage
            importance = min(1.0, (vc.iloc[0] / len(df)) * (1 + len(vc) / 10))

            entities.append(Entity(
                name          = col,
                entity_type   = etype,
                source_col    = col,
                sample_values = [str(v) for v in vc.head(5).index.tolist()],
                frequency     = int(vc.sum()),
                importance    = round(importance, 3),
            ))

        # Sort by importance
        entities.sort(key=lambda e: e.importance, reverse=True)
        return entities

    # ──────────────────────────────────────────────────────────
    #  KPI IDENTIFICATION
    # ──────────────────────────────────────────────────────────

    def _identify_kpis(self, df: pd.DataFrame, roles: Dict) -> List[KPISignal]:
        kpis = []
        measure_cols   = [c for c, r in roles.items() if r == "measure"]
        threshold_cols = [c for c, r in roles.items() if r == "threshold"]
        time_cols      = [c for c, r in roles.items() if r == "time"]
        status_cols    = [c for c, r in roles.items() if r == "status"]

        for mc in measure_cols[:8]:
            col    = df[mc].dropna()
            if col.empty:
                continue

            current_val = float(col.iloc[-1]) if len(col) > 0 else None
            prev_val    = float(col.iloc[-2]) if len(col) > 1 else None

            # Find matching threshold column
            thr_col = next(
                (t for t in threshold_cols
                 if t.lower()[:4] == mc.lower()[:4] or any(k in t.lower() for k in ["threshold","limit","target"])),
                threshold_cols[0] if threshold_cols else None
            )
            threshold = float(df[thr_col].dropna().iloc[-1]) if thr_col and not df[thr_col].dropna().empty else None

            # Determine direction
            mc_lo = mc.lower()
            if any(k in mc_lo for k in self._LOWER_BETTER):
                direction = "lower_is_better"
            elif any(k in mc_lo for k in self._HIGHER_BETTER):
                direction = "higher_is_better"
            else:
                direction = "neutral"

            # Status
            status = "unknown"
            if current_val is not None and threshold is not None:
                if direction == "lower_is_better":
                    status = "breaching" if current_val > threshold else "on_track"
                elif direction == "higher_is_better":
                    status = "breaching" if current_val < threshold else "on_track"
                else:
                    gap = abs(current_val - threshold) / max(abs(threshold), 1e-9)
                    status = "breaching" if gap > 0.2 else ("at_risk" if gap > 0.05 else "on_track")

            # Check status column
            if status_cols and status == "unknown":
                stat_series = df[status_cols[0]].dropna()
                if not stat_series.empty:
                    last = str(stat_series.iloc[-1]).lower()
                    if "red" in last or "fail" in last or "breach" in last:
                        status = "breaching"
                    elif "amber" in last or "warn" in last or "at risk" in last:
                        status = "at_risk"
                    elif "green" in last or "pass" in last or "ok" in last:
                        status = "on_track"

            # Trend
            pct_change = None
            trend      = "insufficient_data"
            if current_val is not None and prev_val is not None and prev_val != 0:
                pct_change = round((current_val - prev_val) / abs(prev_val) * 100, 2)
                if direction == "higher_is_better":
                    trend = "improving" if pct_change > 1 else ("deteriorating" if pct_change < -1 else "stable")
                elif direction == "lower_is_better":
                    trend = "improving" if pct_change < -1 else ("deteriorating" if pct_change > 1 else "stable")
                else:
                    trend = "stable" if abs(pct_change) <= 3 else ("improving" if pct_change > 0 else "deteriorating")

            # Time-based trend (use linear slope if time col exists)
            if time_cols and len(col) >= 4:
                try:
                    slope = np.polyfit(range(len(col)), col.values, 1)[0]
                    if abs(slope) < col.std() * 0.05:
                        trend = "stable"
                    elif (direction == "higher_is_better" and slope > 0) or (direction == "lower_is_better" and slope < 0):
                        trend = "improving"
                    elif (direction == "higher_is_better" and slope < 0) or (direction == "lower_is_better" and slope > 0):
                        trend = "deteriorating"
                except Exception:
                    pass

            # Percentile of current value in historical range
            percentile = None
            if current_val is not None and len(col) > 1:
                percentile = round(float((col <= current_val).mean() * 100), 1)

            kpis.append(KPISignal(
                name       = mc,
                col        = mc,
                current_val= current_val,
                prev_val   = prev_val,
                threshold  = threshold,
                direction  = direction,
                status     = status,
                trend      = trend,
                pct_change = pct_change,
                percentile = percentile,
            ))

        # Sort: breaching first, then at_risk, then on_track
        order = {"breaching": 0, "at_risk": 1, "on_track": 2, "unknown": 3}
        kpis.sort(key=lambda k: order.get(k.status, 3))
        return kpis

    # ──────────────────────────────────────────────────────────
    #  RELATIONSHIP MAPPING
    # ──────────────────────────────────────────────────────────

    def _map_relationships(self, df: pd.DataFrame, roles: Dict) -> List[Relationship]:
        rels = []
        measures   = [c for c, r in roles.items() if r == "measure"]
        thresholds = [c for c, r in roles.items() if r == "threshold"]
        dims       = [c for c, r in roles.items() if r == "dimension"]
        times      = [c for c, r in roles.items() if r == "time"]

        # Threshold relationships
        for mc in measures:
            for tc in thresholds:
                if df[mc].dropna().empty or df[tc].dropna().empty:
                    continue
                v_mean = float(df[mc].mean())
                t_mean = float(df[tc].mean())
                pct_gap = abs(v_mean - t_mean) / max(abs(t_mean), 1e-9) * 100
                rels.append(Relationship(
                    col_a=mc, col_b=tc, rel_type="threshold",
                    strength=min(1.0, 1 - pct_gap / 100),
                    note=f"{mc} is {pct_gap:.1f}% {'below' if v_mean < t_mean else 'above'} {tc}"
                ))

        # Correlation between measures
        if len(measures) >= 2:
            try:
                corr = df[measures[:6]].corr(numeric_only=True)
                for i in range(len(measures[:6])):
                    for j in range(i+1, len(measures[:6])):
                        c = float(corr.iloc[i, j])
                        if abs(c) > 0.6:
                            rels.append(Relationship(
                                col_a=measures[i], col_b=measures[j],
                                rel_type="correlation",
                                strength=round(abs(c), 3),
                                note=f"r={c:.2f} ({'positive' if c > 0 else 'negative'} correlation)"
                            ))
            except Exception:
                pass

        # Hierarchy detection (dimension A contains dimension B)
        for i, d0 in enumerate(dims[:5]):
            for d1 in dims[i+1:5]:
                try:
                    ratio = df.groupby(d1)[d0].nunique().max()
                    if ratio == 1 and df[d0].nunique() < df[d1].nunique():
                        rels.append(Relationship(
                            col_a=d0, col_b=d1, rel_type="hierarchy",
                            strength=0.9,
                            note=f"{d0} is a parent of {d1}"
                        ))
                except Exception:
                    pass

        # Time → measure relationship
        for tc in times[:2]:
            for mc in measures[:3]:
                rels.append(Relationship(
                    col_a=tc, col_b=mc, rel_type="time_series",
                    strength=0.8,
                    note=f"{mc} can be trended over {tc}"
                ))

        rels.sort(key=lambda r: r.strength, reverse=True)
        return rels[:15]

    # ──────────────────────────────────────────────────────────
    #  TEMPORAL PATTERN DETECTION
    # ──────────────────────────────────────────────────────────

    def _detect_temporal(self, df: pd.DataFrame, roles: Dict) -> List[TemporalPattern]:
        patterns = []
        time_cols    = [c for c, r in roles.items() if r == "time"]
        measure_cols = [c for c, r in roles.items() if r == "measure"]

        if not time_cols or not measure_cols:
            return patterns

        for tc in time_cols[:2]:
            for mc in measure_cols[:4]:
                try:
                    grp = df.groupby(tc, sort=True)[mc].mean().dropna()
                    if len(grp) < 2:
                        continue

                    vals     = grp.values.astype(float)
                    periods  = grp.index.astype(str).tolist()
                    latest   = float(vals[-1])
                    baseline = float(vals[0])
                    pct_chg  = round((latest - baseline) / max(abs(baseline), 1e-9) * 100, 2)

                    # Trend direction via linear slope
                    slope = float(np.polyfit(range(len(vals)), vals, 1)[0])
                    std   = float(np.std(vals))
                    if abs(slope) < std * 0.03:
                        trend_dir = "flat"
                    elif slope > 0:
                        trend_dir = "up"
                    elif slope < 0:
                        trend_dir = "down"
                    else:
                        trend_dir = "volatile"

                    # Simple seasonality: check if std of diffs is high relative to mean
                    diffs     = np.diff(vals)
                    is_seasonal = bool(len(diffs) >= 4 and np.std(diffs) > np.mean(np.abs(diffs)) * 0.5)

                    patterns.append(TemporalPattern(
                        time_col    = tc,
                        measure_col = mc,
                        trend_dir   = trend_dir,
                        trend_slope = round(slope, 4),
                        min_period  = periods[0],
                        max_period  = periods[-1],
                        n_periods   = len(grp),
                        latest_val  = round(latest, 4),
                        baseline_val= round(baseline, 4),
                        pct_change  = pct_chg,
                        is_seasonal = is_seasonal,
                    ))
                except Exception as e:
                    log.debug(f"Temporal pattern failed {tc}/{mc}: {e}")

        return patterns

    # ──────────────────────────────────────────────────────────
    #  ANOMALY DETECTION
    # ──────────────────────────────────────────────────────────

    def _detect_anomalies(
        self, df: pd.DataFrame, roles: Dict, kpis: List[KPISignal]
    ) -> List[AnomalySignal]:
        anomalies = []

        # 1. KPI breaches
        for kp in kpis:
            if kp.status == "breaching":
                anomalies.append(AnomalySignal(
                    signal_type  = "breach",
                    severity     = "critical",
                    description  = (
                        f"{kp.name} is breaching its threshold: "
                        f"current={self._fmt(kp.current_val)} vs threshold={self._fmt(kp.threshold)}"
                    ),
                    col          = kp.col,
                    affected_rows= int(len(df)),
                    value        = kp.current_val,
                    context      = f"Direction: {kp.direction}",
                ))
            elif kp.status == "at_risk" and kp.trend == "deteriorating":
                anomalies.append(AnomalySignal(
                    signal_type  = "breach",
                    severity     = "high",
                    description  = f"{kp.name} is at risk and deteriorating: {self._fmt(kp.current_val)}",
                    col          = kp.col,
                    affected_rows= int(len(df)),
                    value        = kp.current_val,
                    context      = f"Change: {kp.pct_change:+.1f}%" if kp.pct_change else "",
                ))

        # 2. Statistical outliers (IQR method)
        measure_cols = [c for c, r in roles.items() if r == "measure"]
        for mc in measure_cols[:5]:
            col = df[mc].dropna()
            if len(col) < 4:
                continue
            q1, q3  = col.quantile(0.25), col.quantile(0.75)
            iqr     = q3 - q1
            lo, hi  = q1 - 2.5 * iqr, q3 + 2.5 * iqr
            outlier_mask = (col < lo) | (col > hi)
            n_out   = int(outlier_mask.sum())
            if n_out > 0:
                anomalies.append(AnomalySignal(
                    signal_type  = "outlier",
                    severity     = "medium" if n_out < 3 else "high",
                    description  = f"{n_out} outlier(s) in {mc} outside [{self._fmt(lo)}, {self._fmt(hi)}]",
                    col          = mc,
                    affected_rows= n_out,
                    value        = col[outlier_mask].tolist()[:3],
                    context      = f"IQR range: Q1={self._fmt(q1)}, Q3={self._fmt(q3)}",
                ))

        # 3. Missing data
        for col in df.columns:
            null_pct = df[col].isna().mean()
            if null_pct > 0.3:
                anomalies.append(AnomalySignal(
                    signal_type  = "missing",
                    severity     = "medium" if null_pct < 0.6 else "high",
                    description  = f"{col} has {null_pct:.0%} missing values",
                    col          = col,
                    affected_rows= int(df[col].isna().sum()),
                    value        = null_pct,
                    context      = "Consider data quality investigation",
                ))

        # 4. Trend reversals (temporal anomalies)
        time_cols = [c for c, r in roles.items() if r == "time"]
        for mc in measure_cols[:3]:
            for tc in time_cols[:1]:
                try:
                    grp  = df.groupby(tc, sort=True)[mc].mean().dropna()
                    if len(grp) < 4:
                        continue
                    vals = grp.values.astype(float)
                    # Check last move vs long-term direction
                    long_slope = np.polyfit(range(len(vals)-1), vals[:-1], 1)[0]
                    last_move  = vals[-1] - vals[-2]
                    if (long_slope > 0 and last_move < -abs(long_slope) * 2) or \
                       (long_slope < 0 and last_move >  abs(long_slope) * 2):
                        anomalies.append(AnomalySignal(
                            signal_type  = "trend_reversal",
                            severity     = "medium",
                            description  = f"{mc} reversed its trend in the latest period ({grp.index[-1]})",
                            col          = mc,
                            affected_rows= 1,
                            value        = float(vals[-1]),
                            context      = f"Long-term slope: {long_slope:.4f}, last change: {last_move:.4f}",
                        ))
                except Exception:
                    pass

        # Sort by severity
        sev_order = {"critical": 0, "high": 1, "medium": 2, "low": 3}
        anomalies.sort(key=lambda a: sev_order.get(a.severity, 4))
        return anomalies[:12]

    # ──────────────────────────────────────────────────────────
    #  NARRATIVE GENERATION
    # ──────────────────────────────────────────────────────────

    def _build_narrative(
        self, df: pd.DataFrame, source_name: str,
        roles: Dict, kpis: List[KPISignal],
        anomalies: List[AnomalySignal],
        temporal: List[TemporalPattern],
    ) -> str:
        parts = []
        n_rows  = len(df)
        n_cols  = len(df.columns)
        dims    = [c for c, r in roles.items() if r == "dimension"]
        meas    = [c for c, r in roles.items() if r == "measure"]
        times   = [c for c, r in roles.items() if r == "time"]

        parts.append(
            f"{source_name} contains {n_rows:,} records across {n_cols} fields. "
        )

        if dims:
            parts.append(f"Key dimensions: {', '.join(dims[:4])}. ")
        if meas:
            parts.append(f"Measures tracked: {', '.join(meas[:4])}. ")

        # KPI summary
        breaching = [k for k in kpis if k.status == "breaching"]
        at_risk   = [k for k in kpis if k.status == "at_risk"]
        on_track  = [k for k in kpis if k.status == "on_track"]
        if breaching:
            parts.append(
                f"{len(breaching)} KPI(s) are currently breaching thresholds "
                f"({', '.join(k.name for k in breaching[:3])}). "
            )
        if at_risk:
            parts.append(f"{len(at_risk)} KPI(s) are at risk. ")
        if on_track:
            parts.append(f"{len(on_track)} KPI(s) are on track. ")

        # Temporal summary
        if temporal:
            t = temporal[0]
            direction_word = {"up": "upward", "down": "downward", "flat": "stable", "volatile": "volatile"}.get(t.trend_dir, "mixed")
            parts.append(
                f"{t.measure_col} shows a {direction_word} trend over {t.n_periods} periods "
                f"({t.pct_change:+.1f}% from {t.min_period} to {t.max_period}). "
            )

        # Anomaly summary
        critical = [a for a in anomalies if a.severity == "critical"]
        if critical:
            parts.append(
                f"CRITICAL: {'; '.join(a.description for a in critical[:2])}. "
            )

        return "".join(parts).strip()

    # ──────────────────────────────────────────────────────────
    #  TF-IDF SEMANTIC INDEX
    # ──────────────────────────────────────────────────────────

    def _build_index(
        self, df: pd.DataFrame, roles: Dict
    ) -> Tuple[Optional[TfidfVectorizer], List[str], List[Dict], Any]:
        """
        Build a TF-IDF index over every meaningful fact in the dataset.
        Each 'document' = one logical unit: a row summary, a column description,
        or a category-value fact.

        This enables semantic search: "which region is underperforming" →
        finds rows/facts about regions with low measure values.
        """
        docs  = []
        meta  = []
        dims  = [c for c, r in roles.items() if r in ("dimension", "geo")]
        meas  = [c for c, r in roles.items() if r in ("measure", "threshold")]
        stats = [c for c, r in roles.items() if r == "status"]
        times = [c for c, r in roles.items() if r == "time"]

        # 1. Column name documents (so "region" query finds REGION column)
        for col in df.columns:
            docs.append(f"{col} {col.replace('_',' ').replace('-',' ')}")
            meta.append({"type": "column", "col": col, "role": roles.get(col, "?")})

        # 2. Category × measure aggregations
        for dc in dims[:5]:
            for mc in meas[:3]:
                grp = df.groupby(dc)[mc].mean().round(4)
                for val, mean_m in grp.items():
                    text = f"{dc} {val} {mc} {self._fmt(mean_m)} average {dc.replace('_',' ')} {str(val).lower()}"
                    docs.append(text)
                    meta.append({"type": "fact", "dim": dc, "dim_val": str(val),
                                 "measure": mc, "value": float(mean_m)})

        # 3. Status distribution
        for sc in stats[:2]:
            vc = df[sc].value_counts()
            for status_val, cnt in vc.items():
                pct = round(cnt / len(df) * 100, 1)
                text = f"{sc} {status_val} {cnt} items {pct} percent status {str(status_val).lower()} count"
                docs.append(text)
                meta.append({"type": "status", "col": sc, "status": str(status_val),
                             "count": int(cnt), "pct": pct})

        # 4. Top/bottom row summaries
        for mc in meas[:3]:
            col_sorted = df.nlargest(3, mc) if not df.empty else df
            for _, row in col_sorted.iterrows():
                dim_parts = " ".join(f"{d} {row.get(d,'')}" for d in dims[:3] if d in row)
                text = f"top highest maximum {mc} {self._fmt(row[mc])} {dim_parts}"
                docs.append(text)
                meta.append({"type": "top_row", "measure": mc, "value": float(row[mc]),
                             "dims": {d: str(row.get(d,"")) for d in dims[:3] if d in row}})

        # 5. Temporal summaries
        for tc in times[:1]:
            for mc in meas[:3]:
                try:
                    grp = df.groupby(tc, sort=True)[mc].mean().dropna()
                    if len(grp) >= 2:
                        latest = grp.iloc[-1]
                        oldest = grp.iloc[0]
                        pct    = (latest - oldest) / max(abs(oldest), 1e-9) * 100
                        direction = "increasing" if pct > 0 else "decreasing"
                        text = f"trend {mc} over time {tc} {direction} {pct:+.1f} percent change period {grp.index[-1]}"
                        docs.append(text)
                        meta.append({"type": "trend", "time_col": tc, "measure": mc,
                                     "direction": direction, "pct_change": round(pct, 2)})
                except Exception:
                    pass

        if not docs:
            return None, [], [], None

        try:
            vectorizer = TfidfVectorizer(
                ngram_range=(1, 2), max_features=3000,
                sublinear_tf=True, min_df=1, stop_words="english",
            )
            matrix = vectorizer.fit_transform(docs)
            log.debug(f"TF-IDF index: {len(docs)} docs, {matrix.shape[1]} features")
            return vectorizer, docs, meta, matrix
        except Exception as e:
            log.warning(f"TF-IDF index failed: {e}")
            return None, docs, meta, None

    # ──────────────────────────────────────────────────────────
    #  HELPERS
    # ──────────────────────────────────────────────────────────

    def _fmt(self, v) -> str:
        if v is None or (isinstance(v, float) and (np.isnan(v) or np.isinf(v))):
            return "N/A"
        if isinstance(v, float):
            if 0 < abs(v) <= 1:
                return f"{v:.2%}"
            if abs(v) >= 1_000_000:
                return f"{v/1_000_000:.2f}M"
            if abs(v) >= 1_000:
                return f"{v:,.0f}"
            return f"{v:.3f}"
        return str(v)

    def _empty(self, source_id: str, source_name: str) -> SemanticProfile:
        return SemanticProfile(
            source_id="", source_name=source_name, fingerprint="empty",
            entities=[], kpis=[], relationships=[], temporal=[], anomalies=[],
            narrative="No data available.", semantic_index=None,
            index_docs=[], index_meta=[], _doc_matrix=None,
        )
