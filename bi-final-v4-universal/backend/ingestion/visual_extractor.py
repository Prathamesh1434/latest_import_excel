"""
ingestion/visual_extractor.py
─────────────────────────────
Universal Tableau Dashboard Data Extractor.

Works with ANY Tableau dashboard without prior knowledge of its structure.
Zero hardcoding for specific dashboard types.

Handles all Tableau visualization types:
  ✓ Bar / Grouped Bar / Stacked Bar
  ✓ Line / Area charts
  ✓ Pie / Donut / Ring charts
  ✓ Scatter plots / Bubble charts
  ✓ Maps (Filled Map, Symbol Map, Density Map)
  ✓ KPI Cards / Big Number displays
  ✓ KRI Scorecards with RAG / threshold columns
  ✓ Heat Maps / Highlight Tables
  ✓ Treemaps / Packed Bubbles
  ✓ Histograms (binned fields)
  ✓ Box Plots / Candlestick
  ✓ Waterfall / Gantt charts
  ✓ Dual-Axis / Combined Charts
  ✓ Crosstabs / Pivot Tables
  ✓ Tableau Measure Names + Measure Values pattern (multi-measure)
  ✓ Calculated fields, LOD expressions, Table calculations
  ✓ Date-part columns (YEAR, MONTH, QUARTER, WEEK, DAY)
  ✓ Geographic dimensions (Country, State, City, PostCode)
  ✓ Tableau Sets (boolean columns)
  ✓ Bin fields ([Field] (bin))
  ✓ Large datasets with aggregation

Pipeline:
    df = tableau_extractor.get_dataframe(target)
    snap = VisualDashboardExtractor().extract(df, source_id, source_name)
    context = snap.to_llm_context()

Packages: pandas==2.0.3  numpy==1.26.4
"""
from __future__ import annotations

import re, json, logging, hashlib
import numpy  as np
import pandas as pd
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

log = logging.getLogger("visual_extractor")

# ══════════════════════════════════════════════════════════════
#  CONSTANTS
# ══════════════════════════════════════════════════════════════

# Column roles
ROLE_TIME      = "time"
ROLE_DIM       = "dimension"
ROLE_MEASURE   = "measure"
ROLE_GEO       = "geographic"
ROLE_STATUS    = "status"
ROLE_THRESHOLD = "threshold"
ROLE_BOOL      = "boolean"
ROLE_BIN       = "bin"
ROLE_ID        = "id"
ROLE_TEXT      = "text"

# Visual types (returned in VisualType.vtype)
VT_TIME_SERIES   = "time_series"
VT_MULTI_SERIES  = "multi_series_time"
VT_BAR           = "bar_chart"
VT_PIE           = "pie_chart"
VT_DONUT         = "donut_chart"
VT_SCATTER       = "scatter_plot"
VT_BUBBLE        = "bubble_chart"
VT_HEATMAP       = "heat_map"
VT_TREEMAP       = "treemap"
VT_KPI_CARDS     = "kpi_cards"
VT_KPI_SCORE     = "kpi_scorecard"
VT_MAP           = "map"
VT_HISTOGRAM     = "histogram"
VT_BOX_PLOT      = "box_plot"
VT_WATERFALL     = "waterfall"
VT_GANTT         = "gantt"
VT_MULTI_MEASURE = "multi_measure"   # Tableau Measure Names/Values
VT_CROSSTAB      = "crosstab"
VT_TABLE         = "data_table"

# ── Regex patterns ─────────────────────────────────────────────

_DATE_RE = re.compile(
    r"(^year|^month|^quarter|^week|^day|^hour|^minute|^second)"
    r"|(^date(trunc|part|diff|add)?[\s(])"
    r"|[\s_](date|month|period|quarter|year|week|dt)$"
    r"|[\s_](date|month|period|quarter|year|week|dt)[\s_]",
    re.IGNORECASE,
)
_STATUS_RE = re.compile(
    r"^(rag|status|flag|indicator|signal|alert|health|state|colour|color|"
    r"traffic|band|tier|rating|grade|performance|result|outcome)$",
    re.IGNORECASE,
)
_THRESH_RE = re.compile(
    r"threshold|limit|target|benchmark|ceiling|floor|sla|"
    r"tolerance|redline|greenline|upper.bound|lower.bound",
    re.IGNORECASE,
)
_ID_RE = re.compile(
    r"^(id|pk|key|code|ref|num|uuid|guid|serial)$"
    r"|(_id|_key|_code|_ref|_num)$"
    r"|^(row.?number|record.?id|line.?num)",
    re.IGNORECASE,
)
_BIN_RE = re.compile(r"\(bin\)$|\bbin\b", re.IGNORECASE)
_TABLE_CALC_RE = re.compile(
    r"(running|moving|window|cumul|pct.of|percent.of|rank|index)\b",
    re.IGNORECASE,
)

# Geographic keyword sets
_GEO_EXACT = frozenset({
    "latitude", "longitude", "lat", "lng", "lon", "long",
    "latitude (generated)", "longitude (generated)", "x coordinate", "y coordinate",
})
_GEO_DIMS = frozenset({
    "country", "state", "province", "region", "city", "town", "district",
    "county", "zip", "postal", "postcode", "territory", "continent",
    "nation", "location", "address", "geographic area", "geo",
})
_MEASURE_SPECIAL = frozenset({"measure names", "measure values"})

# RAG canonical values
_RAG_VALUES = frozenset({
    "red","amber","green","grey","gray","na","n/a",
    "fail","pass","breach","compliant","ok","good","bad",
    "high","medium","low","critical","warning","1","0","true","false",
})


# ══════════════════════════════════════════════════════════════
#  DATA MODELS
# ══════════════════════════════════════════════════════════════

@dataclass
class ColProfile:
    """Profile of a single DataFrame column."""
    name:        str
    role:        str              # one of ROLE_* constants
    dtype:       str
    n_unique:    int
    null_pct:    float
    is_numeric:  bool
    is_temporal: bool
    is_boolean:  bool
    is_bin:      bool
    is_geo:      bool
    is_id:       bool
    is_measure_special: bool      # "Measure Names" or "Measure Values"
    is_table_calc: bool           # Running Total, Rank etc.
    sample:      List[Any]
    stats:       Dict[str, float] = field(default_factory=dict)


@dataclass
class VisualType:
    """Inferred visual type with confidence score."""
    vtype:       str        # one of VT_* constants
    confidence:  float      # 0.0 – 1.0
    dim_cols:    List[str]  # columns used as dimensions
    measure_cols:List[str]  # columns used as measures
    time_col:    Optional[str] = None
    geo_col:     Optional[str] = None
    size_col:    Optional[str] = None
    color_col:   Optional[str] = None
    note:        str = ""


@dataclass
class AggBlock:
    """One pre-computed aggregation block."""
    agg_id:     str
    agg_type:   str           # dist | trend | cross | corr | kpi | geo | top_n | hist
    title:      str
    data:       List[Dict[str, Any]]
    metadata:   Dict[str, Any] = field(default_factory=dict)

    def to_text(self, max_rows: int = 30) -> str:
        lines = [f"[{self.agg_type.upper()}] {self.title}"]
        for row in self.data[:max_rows]:
            lines.append("  " + "  |  ".join(f"{k}: {v}" for k, v in row.items()))
        if len(self.data) > max_rows:
            lines.append(f"  … {len(self.data)-max_rows} more rows")
        return "\n".join(lines)


@dataclass
class DashboardSnapshot:
    """
    Complete structured snapshot of any Tableau dashboard.
    Produced by VisualDashboardExtractor.extract().
    """
    view_id:        str
    source_id:      str
    source_name:    str
    total_rows:     int
    total_cols:     int
    all_columns:    List[str]
    col_profiles:   List[ColProfile]
    visual_types:   List[VisualType]    # sorted by confidence desc
    agg_blocks:     List[AggBlock]      # pre-computed aggregations
    schema_text:    str                 # LLM-ready schema description
    summary_text:   str                 # one-paragraph summary
    has_time:       bool
    has_geo:        bool
    has_rag:        bool
    has_threshold:  bool
    has_multi_measure: bool             # Measure Names / Values
    dashboard_type: str                 # best-guess type label
    extraction_ms:  int = 0

    def primary_visual(self) -> Optional[VisualType]:
        return self.visual_types[0] if self.visual_types else None

    def to_llm_context(self, max_chars: int = 10000) -> str:
        """Full LLM-ready context string — schema + aggregations."""
        parts  = [
            f"=== DASHBOARD: {self.source_name} ===",
            f"Primary visual type: {self.dashboard_type}",
            f"Rows: {self.total_rows:,}  |  Columns: {self.total_cols}",
        ]
        if self.has_time:      parts.append("✓ Time-series capable")
        if self.has_geo:       parts.append("✓ Geographic data present")
        if self.has_rag:       parts.append("✓ RAG / Status tracking present")
        if self.has_threshold: parts.append("✓ Threshold/target columns present")
        if self.has_multi_measure: parts.append("✓ Multi-measure (Measure Names/Values) pattern")
        parts.append(f"\nSummary: {self.summary_text}")
        parts.append(f"\n{self.schema_text}")
        parts.append("")

        remaining = max_chars - sum(len(p)+1 for p in parts)
        for blk in self.agg_blocks:
            txt = blk.to_text() + "\n\n"
            if remaining - len(txt) > 100:
                parts.append(txt)
                remaining -= len(txt)
            elif remaining > 200:
                parts.append(f"[{blk.agg_type.upper()}] {blk.title} — truncated\n")
                remaining -= 60

        return "\n".join(parts)

    def to_dict(self) -> Dict:
        return {
            "view_id":        self.view_id,
            "source_id":      self.source_id,
            "source_name":    self.source_name,
            "total_rows":     self.total_rows,
            "total_cols":     self.total_cols,
            "dashboard_type": self.dashboard_type,
            "has_time":       self.has_time,
            "has_geo":        self.has_geo,
            "has_rag":        self.has_rag,
            "has_threshold":  self.has_threshold,
            "visual_types":   [{"type":v.vtype,"conf":v.confidence} for v in self.visual_types[:3]],
            "agg_count":      len(self.agg_blocks),
        }


# ══════════════════════════════════════════════════════════════
#  STEP 1 — NORMALISE TABLEAU DATA
# ══════════════════════════════════════════════════════════════

class TableauDataNormaliser:
    """
    Cleans and normalises raw Tableau CSV exports.
    Handles Tableau-specific quirks:
      - Measure Names / Measure Values → wide pivot
      - Date-part column name cleanup (YEAR(Date) → Date_Year)
      - NULL representations ("Null", "null", "%null%")
      - Duplicate column names
      - Mixed type columns (Tableau sometimes exports numbers as strings)
    """

    # Tableau null sentinels
    _NULL_VALS = {"null", "NULL", "Null", "%null%", "N/A", "n/a", "-",
                  "nan", "NaN", "", " ", "(blank)", "*"}

    def normalise(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df

        df = df.copy()
        df = self._fix_columns(df)
        df = self._fix_nulls(df)
        df = self._fix_dtypes(df)
        df = self._handle_measure_names(df)
        df = df.dropna(how="all").dropna(axis=1, how="all")
        df = df.reset_index(drop=True)
        log.debug(f"Normalised: {len(df)} rows × {len(df.columns)} cols")
        return df

    def _fix_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Strip whitespace, deduplicate column names."""
        cols = [str(c).strip() for c in df.columns]
        # Deduplicate
        seen: Dict[str, int] = {}
        clean = []
        for c in cols:
            if c in seen:
                seen[c] += 1
                clean.append(f"{c}_{seen[c]}")
            else:
                seen[c] = 0
                clean.append(c)
        df.columns = clean
        return df

    def _fix_nulls(self, df: pd.DataFrame) -> pd.DataFrame:
        """Replace Tableau null representations with proper NaN."""
        for col in df.select_dtypes("object").columns:
            df[col] = df[col].astype(str).str.strip()
            df[col] = df[col].replace(list(self._NULL_VALS), np.nan)
        return df

    def _fix_dtypes(self, df: pd.DataFrame) -> pd.DataFrame:
        """Coerce string columns that are actually numeric."""
        for col in df.select_dtypes("object").columns:
            sample = df[col].dropna().head(50)
            if sample.empty:
                continue
            # Try numeric
            converted = pd.to_numeric(sample.str.replace(",","").str.replace("%",""), errors="coerce")
            if converted.notna().mean() > 0.85:
                df[col] = pd.to_numeric(df[col].astype(str).str.replace(",","").str.replace("%",""), errors="coerce")
        return df

    had_measure_names: bool = False   # set True if Measure Names/Values detected

    def _handle_measure_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Detect and pivot Tableau's Measure Names / Measure Values pattern.

        Input (long format):
            Category | Measure Names | Measure Values
            West     | Sales         | 100000
            West     | Profit        | 15000

        Output (wide format):
            Category | Sales  | Profit
            West     | 100000 | 15000
        """
        mn_col = next((c for c in df.columns if c.lower() == "measure names"), None)
        mv_col = next((c for c in df.columns if c.lower() == "measure values"), None)

        if not mn_col or not mv_col:
            return df

        self.had_measure_names = True   # remember this for downstream

        log.info("Detected Tableau Measure Names/Values — pivoting to wide format")
        other_cols = [c for c in df.columns if c not in (mn_col, mv_col)]
        try:
            if other_cols:
                wide = df.pivot_table(
                    index=other_cols, columns=mn_col, values=mv_col,
                    aggfunc="first"
                ).reset_index()
                wide.columns.name = None
                log.info(f"Pivot: {df.shape} → {wide.shape}")
                return wide
            else:
                # No dimension columns — just pivot on row index
                wide = df.pivot(columns=mn_col, values=mv_col)
                wide.columns.name = None
                return wide.reset_index(drop=True)
        except Exception as e:
            log.warning(f"Measure Names pivot failed: {e} — keeping original")
            return df


# ══════════════════════════════════════════════════════════════
#  STEP 2 — CLASSIFY COLUMNS
# ══════════════════════════════════════════════════════════════

class ColumnClassifier:
    """
    Classifies every column in a DataFrame using:
      - Column name patterns (regex + keyword)
      - Data type (numeric, datetime, boolean, object)
      - Value distribution (cardinality, null %, sample values)
    """

    def classify(self, df: pd.DataFrame) -> List[ColProfile]:
        profiles = []
        n = len(df)
        for col in df.columns:
            s         = df[col]
            is_num    = pd.api.types.is_numeric_dtype(s)
            is_bool   = pd.api.types.is_bool_dtype(s)
            n_unique  = int(s.nunique(dropna=True))
            null_pct  = round(s.isna().mean() * 100, 1)
            sample    = s.dropna().head(5).tolist()
            col_lo    = col.lower().strip()

            # Flag checks (order matters — more specific first)
            is_measure_special = col_lo in _MEASURE_SPECIAL
            is_bin             = bool(_BIN_RE.search(col))
            is_id              = bool(_ID_RE.search(col_lo)) and (n_unique == n or n_unique > n * 0.9)
            is_geo             = (col_lo in _GEO_EXACT) or any(g in col_lo for g in _GEO_DIMS)
            is_temporal        = self._is_temporal(col, s)
            is_table_calc      = bool(_TABLE_CALC_RE.search(col))

            # Determine role
            role = self._assign_role(
                col_lo, s, is_num, is_bool, is_bin, is_id, is_geo,
                is_temporal, is_measure_special, n_unique, n, sample
            )

            # Numeric stats
            stats: Dict[str, float] = {}
            if is_num and n_unique > 1:
                stats = {
                    "min":  float(s.min()),
                    "max":  float(s.max()),
                    "mean": round(float(s.mean()), 4),
                    "sum":  float(s.sum()),
                    "std":  round(float(s.std()), 4),
                }

            profiles.append(ColProfile(
                name=col, role=role, dtype=str(s.dtype),
                n_unique=n_unique, null_pct=null_pct,
                is_numeric=is_num, is_temporal=is_temporal,
                is_boolean=is_bool, is_bin=is_bin, is_geo=is_geo,
                is_id=is_id, is_measure_special=is_measure_special,
                is_table_calc=is_table_calc, sample=sample, stats=stats,
            ))
        return profiles

    def _is_temporal(self, col: str, s: pd.Series) -> bool:
        if _DATE_RE.search(col):
            return True
        if pd.api.types.is_datetime64_any_dtype(s):
            return True
        # Check string values for date-like patterns
        if s.dtype == object:
            sample = s.dropna().astype(str).head(10)
            date_re = re.compile(
                r"^\d{4}[-/]\d{2}|^\d{2}[-/]\d{2}[-/]\d{4}"
                r"|^(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[-\s]\d{2,4}"
                r"|^(q[1-4][-\s]\d{4})|^(fy\d{2,4})|^\d{4}$",
                re.IGNORECASE,
            )
            hits = sum(1 for v in sample if date_re.match(str(v)))
            return hits >= min(3, len(sample))
        return False

    def _assign_role(
        self, col_lo: str, s: pd.Series, is_num: bool, is_bool: bool,
        is_bin: bool, is_id: bool, is_geo: bool, is_temporal: bool,
        is_measure_special: bool, n_unique: int, n: int, sample: List
    ) -> str:
        if is_measure_special:   return ROLE_MEASURE   # Measure Names/Values
        if is_temporal:          return ROLE_TIME
        if is_id:                return ROLE_ID
        if is_geo:               return ROLE_GEO
        if is_bin:               return ROLE_BIN
        if is_bool:
            return ROLE_BOOL
        if n_unique <= 2 and sample:
            sample_lo = {str(v).lower() for v in sample}
            # Only boolean if values actually look like boolean/status — not category labels
            bool_like  = frozenset({"true","false","1","0","y","n","yes","no"})
            rag_like   = frozenset({"red","amber","green","grey","pass","fail",
                                    "breach","compliant","ok","good","bad","high","low"})
            if sample_lo.issubset(rag_like):
                return ROLE_STATUS
            if sample_lo.issubset(bool_like):
                return ROLE_BOOL
            # Two-value categoricals (like "A","B") fall through to dimension
        if _STATUS_RE.search(col_lo):
            sample_lo = {str(v).lower() for v in sample}
            if sample_lo.issubset(_RAG_VALUES) or n_unique <= 10:
                return ROLE_STATUS
        if _THRESH_RE.search(col_lo) and is_num:
            return ROLE_THRESHOLD
        if is_num:
            return ROLE_MEASURE
        # Cardinality-based dimension detection
        cardinality_ratio = n_unique / max(n, 1)
        avg_len = s.dropna().astype(str).str.len().mean() if not is_num else 0
        if cardinality_ratio < 0.4 or n_unique <= 100:
            return ROLE_DIM
        if avg_len and avg_len > 100:
            return ROLE_TEXT
        return ROLE_DIM


# ══════════════════════════════════════════════════════════════
#  STEP 3 — INFER VISUAL TYPES
# ══════════════════════════════════════════════════════════════

class VisualTypeInferencer:
    """
    Infers the likely Tableau visualization type(s) from the data shape.
    Returns ranked list of (VisualType, confidence) — highest first.

    Does NOT use dashboard name or external knowledge.
    Pure data-shape heuristics.
    """

    def infer(self, df: pd.DataFrame, profiles: List[ColProfile]) -> List[VisualType]:
        by_role: Dict[str, List[ColProfile]] = {}
        for p in profiles:
            by_role.setdefault(p.role, []).append(p)

        times  = by_role.get(ROLE_TIME,      [])
        dims   = by_role.get(ROLE_DIM,       [])
        meas   = by_role.get(ROLE_MEASURE,   [])
        geos   = by_role.get(ROLE_GEO,       [])
        stats  = by_role.get(ROLE_STATUS,    [])
        thresh = by_role.get(ROLE_THRESHOLD, [])
        bins   = by_role.get(ROLE_BIN,       [])
        bools  = by_role.get(ROLE_BOOL,      [])

        n_rows = len(df)
        n_time, n_dim, n_meas, n_geo = len(times), len(dims), len(meas), len(geos)

        # Check for Measure Names/Values pattern
        has_mn = any(p.is_measure_special for p in profiles)

        candidates: List[VisualType] = []

        # ── Measure Names/Values (Tableau multi-measure) ──────────────────
        if has_mn:
            candidates.append(VisualType(
                vtype=VT_MULTI_MEASURE, confidence=0.95,
                dim_cols=[p.name for p in dims],
                measure_cols=[p.name for p in meas],
                note="Tableau Measure Names / Measure Values pattern (already pivoted)",
            ))

        # ── Geographic Map ────────────────────────────────────────────────
        if n_geo >= 2:
            candidates.append(VisualType(
                vtype=VT_MAP, confidence=0.92,
                dim_cols=[p.name for p in geos],
                measure_cols=[p.name for p in meas[:2]],
                geo_col=geos[0].name if geos else None,
                note=f"Lat/Lng or named geo columns: {[p.name for p in geos]}",
            ))
        elif n_geo == 1 and n_meas >= 1:
            candidates.append(VisualType(
                vtype=VT_MAP, confidence=0.75,
                dim_cols=[geos[0].name],
                measure_cols=[meas[0].name],
                geo_col=geos[0].name,
            ))
        elif any(g in " ".join(p.name.lower() for p in dims) for g in ("country","state","city","region","province")):
            candidates.append(VisualType(
                vtype=VT_MAP, confidence=0.60,
                dim_cols=[p.name for p in dims[:2]],
                measure_cols=[p.name for p in meas[:1]],
            ))

        # ── KPI Scorecard (RAG + thresholds) ─────────────────────────────
        if (stats or thresh) and n_meas >= 1:
            conf = 0.88 if (stats and thresh) else 0.72
            candidates.append(VisualType(
                vtype=VT_KPI_SCORE, confidence=conf,
                dim_cols=[p.name for p in dims[:2]],
                measure_cols=[p.name for p in meas[:4]],
                color_col=stats[0].name if stats else None,
                note=f"Status cols: {[p.name for p in stats]} | Threshold cols: {[p.name for p in thresh]}",
            ))

        # ── KPI Cards (single/few numeric values) ────────────────────────
        if n_meas >= 1 and n_dim == 0 and n_time == 0 and n_rows <= 20:
            candidates.append(VisualType(
                vtype=VT_KPI_CARDS, confidence=0.85,
                dim_cols=[],
                measure_cols=[p.name for p in meas[:6]],
                note="Few rows, only measures — likely big-number KPI display",
            ))

        # ── Histogram (binned field) ──────────────────────────────────────
        if bins:
            candidates.append(VisualType(
                vtype=VT_HISTOGRAM, confidence=0.92,
                dim_cols=[p.name for p in bins],
                measure_cols=[p.name for p in meas[:1]],
                note=f"Bin columns detected: {[p.name for p in bins]}",
            ))

        # ── Time Series ───────────────────────────────────────────────────
        if n_time >= 1 and n_meas >= 1:
            if n_dim >= 1:
                # Multiple series (one line per dimension value)
                max_cat = max(df[p.name].nunique() for p in dims) if dims else 0
                if max_cat <= 30:
                    candidates.append(VisualType(
                        vtype=VT_MULTI_SERIES, confidence=0.87,
                        dim_cols=[p.name for p in dims[:2]],
                        measure_cols=[p.name for p in meas[:3]],
                        time_col=times[0].name,
                        color_col=dims[0].name if dims else None,
                        note=f"Time col: {times[0].name}  |  Series: {dims[0].name}",
                    ))
            candidates.append(VisualType(
                vtype=VT_TIME_SERIES, confidence=0.85,
                dim_cols=[p.name for p in dims],
                measure_cols=[p.name for p in meas[:3]],
                time_col=times[0].name,
                note=f"Time col: {times[0].name}",
            ))

        # ── Scatter / Bubble Plot ─────────────────────────────────────────
        if n_meas >= 2 and n_time == 0 and n_dim <= 3:
            if n_meas >= 3:
                candidates.append(VisualType(
                    vtype=VT_BUBBLE, confidence=0.78,
                    dim_cols=[p.name for p in dims[:2]],
                    measure_cols=[meas[0].name, meas[1].name],
                    size_col=meas[2].name,
                    color_col=dims[0].name if dims else None,
                    note=f"X={meas[0].name}  Y={meas[1].name}  Size={meas[2].name}",
                ))
            candidates.append(VisualType(
                vtype=VT_SCATTER, confidence=0.73,
                dim_cols=[p.name for p in dims[:2]],
                measure_cols=[meas[0].name, meas[1].name],
                color_col=dims[0].name if dims else None,
                note=f"X={meas[0].name}  Y={meas[1].name}",
            ))

        # ── Heat Map / Highlight Table ─────────────────────────────────────
        # Case A: two categorical dimensions
        if n_dim >= 2 and n_meas >= 1:
            d0_uniq = df[dims[0].name].nunique() if dims else 0
            d1_uniq = df[dims[1].name].nunique() if len(dims) > 1 else 0
            if 2 <= d0_uniq <= 50 and 2 <= d1_uniq <= 50:
                candidates.append(VisualType(
                    vtype=VT_HEATMAP, confidence=0.70,
                    dim_cols=[dims[0].name, dims[1].name],
                    measure_cols=[meas[0].name],
                    note=f"Row dim: {dims[0].name} ({d0_uniq})  Col dim: {dims[1].name} ({d1_uniq})",
                ))
        # Case B: time × one categorical dimension (common in highlight tables)
        if n_time >= 1 and n_dim >= 1 and n_meas >= 1:
            tc_uniq  = df[times[0].name].nunique() if times else 0
            dim_uniq = df[dims[0].name].nunique()  if dims  else 0
            if 2 <= tc_uniq <= 50 and 2 <= dim_uniq <= 50:
                candidates.append(VisualType(
                    vtype=VT_HEATMAP, confidence=0.62,
                    dim_cols=[times[0].name, dims[0].name],
                    measure_cols=[meas[0].name],
                    time_col=times[0].name,
                    note=f"Time×Dim heat map: {times[0].name} ({tc_uniq}) × {dims[0].name} ({dim_uniq})",
                ))

        # ── Treemap / Packed Bubbles (nested categories + measure) ─────────
        if n_dim >= 2 and n_meas >= 1 and n_time == 0:
            cards = sorted([df[p.name].nunique() for p in dims], reverse=True)
            if len(cards) >= 2 and cards[0] > 5:
                candidates.append(VisualType(
                    vtype=VT_TREEMAP, confidence=0.62,
                    dim_cols=[p.name for p in dims[:3]],
                    measure_cols=[meas[0].name],
                    size_col=meas[0].name,
                    note=f"Nested dims: {[p.name for p in dims[:3]]}",
                ))

        # ── Bar Chart ─────────────────────────────────────────────────────
        if n_dim >= 1 and n_meas >= 1:
            max_uniq = max(df[p.name].nunique() for p in dims) if dims else 0
            if max_uniq <= 100:
                candidates.append(VisualType(
                    vtype=VT_BAR, confidence=0.68,
                    dim_cols=[p.name for p in dims[:2]],
                    measure_cols=[p.name for p in meas[:4]],
                    color_col=dims[1].name if len(dims) > 1 else None,
                    note=f"Primary dim: {dims[0].name} ({max_uniq} values)",
                ))

        # ── Pie / Donut ────────────────────────────────────────────────────
        if n_dim >= 1 and n_meas >= 1:
            max_uniq = max(df[p.name].nunique() for p in dims) if dims else 0
            if max_uniq <= 15:
                candidates.append(VisualType(
                    vtype=VT_PIE, confidence=0.55,
                    dim_cols=[dims[0].name],
                    measure_cols=[meas[0].name],
                    note=f"Few categories ({max_uniq}) — could be pie/donut",
                ))

        # ── Box Plot ──────────────────────────────────────────────────────
        if n_meas >= 1 and n_rows > 30:
            if n_dim <= 2:
                candidates.append(VisualType(
                    vtype=VT_BOX_PLOT, confidence=0.45,
                    dim_cols=[p.name for p in dims[:1]],
                    measure_cols=[meas[0].name],
                ))

        # ── Crosstab / Pivot Table ─────────────────────────────────────────
        # Case A: dim × dim
        if n_dim >= 2 and n_meas >= 1 and dims and len(dims) >= 2:
            d0u = df[dims[0].name].nunique()
            d1u = df[dims[1].name].nunique()
            if abs(n_rows - d0u * d1u) < max(5, d0u * d1u * 0.15):
                candidates.append(VisualType(
                    vtype=VT_CROSSTAB, confidence=0.80,
                    dim_cols=[dims[0].name, dims[1].name],
                    measure_cols=[meas[0].name],
                    note=f"Row dim: {dims[0].name} ({d0u})  Col dim: {dims[1].name} ({d1u})",
                ))
        # Case B: time × dim (perfect grid)
        if n_time >= 1 and n_dim >= 1 and n_meas >= 1:
            t0u = df[times[0].name].nunique()
            d0u = df[dims[0].name].nunique()
            if 2 <= t0u <= 50 and 2 <= d0u <= 50 and abs(n_rows - t0u * d0u) < max(4, t0u * d0u * 0.15):
                candidates.append(VisualType(
                    vtype=VT_CROSSTAB, confidence=0.75,
                    dim_cols=[times[0].name, dims[0].name],
                    measure_cols=[meas[0].name],
                    time_col=times[0].name,
                    note=f"Time×Dim crosstab: {times[0].name} ({t0u}) × {dims[0].name} ({d0u})",
                ))

        # ── Waterfall ─────────────────────────────────────────────────────
        if n_meas >= 1 and n_dim == 1 and n_time == 0:
            vals = df[meas[0].name].dropna()
            if vals.lt(0).any() and vals.gt(0).any():
                candidates.append(VisualType(
                    vtype=VT_WATERFALL, confidence=0.50,
                    dim_cols=[dims[0].name] if dims else [],
                    measure_cols=[meas[0].name],
                    note="Mixed positive/negative values — could be waterfall",
                ))

        # ── Gantt ─────────────────────────────────────────────────────────
        if n_time >= 2 and n_dim >= 1:
            candidates.append(VisualType(
                vtype=VT_GANTT, confidence=0.55,
                dim_cols=[p.name for p in dims[:2]],
                measure_cols=[p.name for p in meas[:1]],
                time_col=times[0].name,
                note=f"Multiple time cols ({[p.name for p in times[:2]]}) — could be Gantt",
            ))

        # ── Generic Data Table (always available as fallback) ─────────────
        candidates.append(VisualType(
            vtype=VT_TABLE, confidence=0.25,
            dim_cols=[p.name for p in dims],
            measure_cols=[p.name for p in meas],
            note="Fallback: data table",
        ))

        # Sort by confidence descending, deduplicate by vtype
        seen_types: set = set()
        unique: List[VisualType] = []
        for vt in sorted(candidates, key=lambda v: v.confidence, reverse=True):
            if vt.vtype not in seen_types:
                seen_types.add(vt.vtype)
                unique.append(vt)

        return unique


# ══════════════════════════════════════════════════════════════
#  STEP 4 — PRE-COMPUTE AGGREGATIONS
# ══════════════════════════════════════════════════════════════

class UniversalAggregator:
    """
    Pre-computes all analytically useful aggregations from a DataFrame.
    Works for any data shape — adapts to what's available.
    """

    MAX_CAT_VALS = 50      # max distinct values in a categorical to aggregate
    MAX_ROWS_OUT = 100     # max rows per aggregation block

    def compute(self, df: pd.DataFrame, profiles: List[ColProfile]) -> List[AggBlock]:
        by_role = {r: [p for p in profiles if p.role == r] for r in
                   [ROLE_TIME,ROLE_DIM,ROLE_MEASURE,ROLE_GEO,ROLE_STATUS,ROLE_THRESHOLD,ROLE_BIN,ROLE_BOOL]}

        times  = by_role[ROLE_TIME]
        dims   = by_role[ROLE_DIM]
        meas   = by_role[ROLE_MEASURE]
        stats  = by_role[ROLE_STATUS]
        thresh = by_role[ROLE_THRESHOLD]
        bins   = by_role[ROLE_BIN]
        geos   = by_role[ROLE_GEO]

        blocks: List[AggBlock] = []

        # 1. Summary stats (always)
        blocks.append(self._summary_stats(df, meas))

        # 2. Distribution of each categorical (top N)
        for dim in dims[:6]:
            blk = self._distribution(df, dim)
            if blk:
                blocks.append(blk)

        # 3. Status / RAG distribution
        for st in stats[:3]:
            blk = self._distribution(df, st)
            if blk:
                blocks.append(blk)

        # 4. Time series aggregation
        for tc in times[:2]:
            for mc in meas[:4]:
                blk = self._time_series(df, tc, mc, dims)
                if blk:
                    blocks.append(blk)

        # 5. Top N by each measure × each dimension
        for mc in meas[:4]:
            for dc in dims[:3]:
                blk = self._top_n(df, mc, dc, n=10)
                if blk:
                    blocks.append(blk)

        # 6. Cross-tabulation (dim × dim × measure)
        if len(dims) >= 2 and meas:
            blk = self._cross_tab(df, dims[0], dims[1], meas[0])
            if blk:
                blocks.append(blk)

        # 7. Scatter correlation
        if len(meas) >= 2:
            blk = self._correlation(df, meas)
            if blk:
                blocks.append(blk)

        # 8. Threshold / breach analysis
        for mc in meas[:3]:
            for tc in thresh[:2]:
                blk = self._breach_analysis(df, mc, tc, dims, stats)
                if blk:
                    blocks.append(blk)

        # 9. Geographic aggregation
        for gc in geos[:2]:
            if meas:
                blk = self._geo_agg(df, gc, meas[0])
                if blk:
                    blocks.append(blk)

        # 10. Histogram / distribution for numeric measures
        for mc in meas[:3]:
            blk = self._histogram(df, mc)
            if blk:
                blocks.append(blk)

        # 11. Bin aggregations
        for bc in bins[:3]:
            if meas:
                blk = self._bin_agg(df, bc, meas[0])
                if blk:
                    blocks.append(blk)

        # 12. Boolean / set summaries
        for bc in by_role[ROLE_BOOL][:3]:
            blk = self._bool_summary(df, bc, meas)
            if blk:
                blocks.append(blk)

        return blocks

    # ── Block builders ─────────────────────────────────────────────────────

    def _summary_stats(self, df: pd.DataFrame, meas: List[ColProfile]) -> AggBlock:
        data = []
        for mc in meas[:10]:
            col = df[mc.name].dropna()
            if col.empty:
                continue
            data.append({
                "column": mc.name,
                "count":  int(col.count()),
                "sum":    self._fmt(float(col.sum())),
                "mean":   self._fmt(float(col.mean())),
                "min":    self._fmt(float(col.min())),
                "max":    self._fmt(float(col.max())),
                "std":    self._fmt(float(col.std())),
            })
        data.append({"total_rows": len(df), "total_cols": len(df.columns)})
        return AggBlock(
            agg_id="summary_stats", agg_type="kpi",
            title="Summary Statistics (all measures)",
            data=data,
        )

    def _distribution(self, df: pd.DataFrame, col: ColProfile) -> Optional[AggBlock]:
        s = df[col.name].dropna()
        if s.empty or col.n_unique > self.MAX_CAT_VALS:
            return None
        vc    = s.value_counts()
        total = vc.sum()
        data  = [
            {"value": str(k), "count": int(v), "pct": round(v/total*100, 1)}
            for k, v in vc.head(self.MAX_ROWS_OUT).items()
        ]
        return AggBlock(
            agg_id=f"dist_{col.name[:20]}",
            agg_type="dist",
            title=f"Distribution: {col.name} ({col.n_unique} unique values)",
            data=data,
            metadata={"total": int(total), "column": col.name},
        )

    def _time_series(
        self, df: pd.DataFrame, tc: ColProfile, mc: ColProfile,
        dims: List[ColProfile]
    ) -> Optional[AggBlock]:
        if tc.name not in df.columns or mc.name not in df.columns:
            return None
        try:
            if dims and df[dims[0].name].nunique() <= 15:
                # Multi-series: aggregate by time × first dimension
                grp = (
                    df.groupby([tc.name, dims[0].name])[mc.name]
                    .sum().reset_index()
                )
                grp.columns = ["period", "series", "value"]
                data = [{"period": str(r.period), "series": str(r.series), "value": self._fmt(r.value)}
                        for r in grp.itertuples(index=False)]
                return AggBlock(
                    agg_id=f"ts_{tc.name[:10]}_{mc.name[:10]}_{dims[0].name[:10]}",
                    agg_type="trend",
                    title=f"Trend: {mc.name} over {tc.name} by {dims[0].name}",
                    data=data[:self.MAX_ROWS_OUT],
                    metadata={"time_col": tc.name, "measure": mc.name, "series_col": dims[0].name},
                )
            else:
                grp = df.groupby(tc.name, sort=True)[mc.name].agg(["sum","mean","count"]).reset_index()
                grp.columns = ["period", "sum", "mean", "count"]
                data = [{"period": str(r.period), "sum": self._fmt(r.sum),
                         "mean": self._fmt(r.mean), "count": int(r.count)}
                        for r in grp.itertuples(index=False)]
                return AggBlock(
                    agg_id=f"ts_{tc.name[:10]}_{mc.name[:10]}",
                    agg_type="trend",
                    title=f"Trend: {mc.name} over {tc.name}",
                    data=data[:self.MAX_ROWS_OUT],
                    metadata={"time_col": tc.name, "measure": mc.name},
                )
        except Exception as e:
            log.debug(f"Time series agg failed for {tc.name}/{mc.name}: {e}")
            return None

    def _top_n(self, df: pd.DataFrame, mc: ColProfile, dc: ColProfile, n: int = 10) -> Optional[AggBlock]:
        if mc.name not in df.columns or dc.name not in df.columns:
            return None
        if df[dc.name].nunique() > self.MAX_CAT_VALS:
            return None
        try:
            grp = df.groupby(dc.name)[mc.name].sum().nlargest(n).reset_index()
            data = [{"label": str(r[dc.name]), "value": self._fmt(float(r[mc.name]))}
                    for _, r in grp.iterrows()]
            return AggBlock(
                agg_id=f"topn_{mc.name[:10]}_{dc.name[:10]}",
                agg_type="top_n",
                title=f"Top {n}: {mc.name} by {dc.name}",
                data=data,
                metadata={"measure": mc.name, "dimension": dc.name},
            )
        except Exception as e:
            log.debug(f"Top-N failed {mc.name}/{dc.name}: {e}")
            return None

    def _cross_tab(
        self, df: pd.DataFrame,
        d0: ColProfile, d1: ColProfile, mc: ColProfile
    ) -> Optional[AggBlock]:
        if d0.n_unique > 30 or d1.n_unique > 30:
            return None
        try:
            pivot = df.pivot_table(values=mc.name, index=d0.name,
                                   columns=d1.name, aggfunc="sum",
                                   fill_value=0)
            rows = []
            for idx, row in pivot.iterrows():
                entry = {d0.name: str(idx)}
                for col in pivot.columns:
                    entry[str(col)] = self._fmt(float(row[col]))
                rows.append(entry)
            return AggBlock(
                agg_id=f"xtab_{d0.name[:10]}_{d1.name[:10]}",
                agg_type="cross",
                title=f"Cross-tab: {mc.name} by {d0.name} × {d1.name}",
                data=rows[:self.MAX_ROWS_OUT],
                metadata={"row_dim": d0.name, "col_dim": d1.name, "measure": mc.name},
            )
        except Exception as e:
            log.debug(f"Cross-tab failed: {e}")
            return None

    def _correlation(self, df: pd.DataFrame, meas: List[ColProfile]) -> Optional[AggBlock]:
        if len(meas) < 2:
            return None
        try:
            cols  = [m.name for m in meas[:8] if m.name in df.columns]
            corr  = df[cols].corr(numeric_only=True).round(3)
            pairs = []
            for i in range(len(cols)):
                for j in range(i+1, len(cols)):
                    c = float(corr.iloc[i, j])
                    pairs.append({
                        "col_a": cols[i], "col_b": cols[j],
                        "correlation": round(c, 3),
                        "strength": "strong" if abs(c) > 0.7 else "moderate" if abs(c) > 0.4 else "weak",
                    })
            pairs.sort(key=lambda x: abs(x["correlation"]), reverse=True)
            return AggBlock(
                agg_id="correlation",
                agg_type="corr",
                title=f"Correlation Matrix ({len(cols)} measures)",
                data=pairs[:30],
            )
        except Exception as e:
            log.debug(f"Correlation failed: {e}")
            return None

    def _breach_analysis(
        self, df: pd.DataFrame, mc: ColProfile, tc: ColProfile,
        dims: List[ColProfile], stats: List[ColProfile]
    ) -> Optional[AggBlock]:
        if mc.name not in df.columns or tc.name not in df.columns:
            return None
        try:
            v = df[mc.name]
            t = df[tc.name]
            v_mean = v.dropna().mean()
            t_mean = t.dropna().mean()
            # Determine breach direction
            if v_mean < t_mean:
                breach_mask = v < t
            else:
                breach_mask = v > t

            df2 = df.copy()
            df2["_breach"]    = breach_mask
            df2["_gap"]       = (v - t).abs().round(4)
            breaching = df2[df2["_breach"]]

            display_cols = [mc.name, tc.name, "_gap"]
            if dims:
                display_cols = [dims[0].name] + display_cols
            if stats:
                display_cols.append(stats[0].name)

            data = breaching[display_cols].head(50).to_dict("records")
            return AggBlock(
                agg_id=f"breach_{mc.name[:10]}_{tc.name[:10]}",
                agg_type="breach",
                title=f"Threshold Breach: {mc.name} vs {tc.name} ({len(breaching)} breaches / {len(df)} total)",
                data=data,
                metadata={
                    "value_col": mc.name, "threshold_col": tc.name,
                    "breach_count": int(len(breaching)), "total": int(len(df)),
                    "breach_rate": round(len(breaching)/max(len(df),1)*100, 1),
                },
            )
        except Exception as e:
            log.debug(f"Breach analysis failed: {e}")
            return None

    def _geo_agg(self, df: pd.DataFrame, gc: ColProfile, mc: ColProfile) -> Optional[AggBlock]:
        if gc.n_unique > 500 or mc.name not in df.columns:
            return None
        try:
            grp = df.groupby(gc.name)[mc.name].agg(["sum","mean","count"]).reset_index()
            grp.columns = [gc.name, "sum", "mean", "count"]
            grp = grp.sort_values("sum", ascending=False)
            data = [{"geo": str(r[gc.name]), "sum": self._fmt(r["sum"]),
                     "mean": self._fmt(r["mean"]), "count": int(r["count"])}
                    for _, r in grp.head(self.MAX_ROWS_OUT).iterrows()]
            return AggBlock(
                agg_id=f"geo_{gc.name[:10]}_{mc.name[:10]}",
                agg_type="geo",
                title=f"Geographic: {mc.name} by {gc.name}",
                data=data,
            )
        except Exception as e:
            log.debug(f"Geo agg failed: {e}")
            return None

    def _histogram(self, df: pd.DataFrame, mc: ColProfile) -> Optional[AggBlock]:
        if mc.name not in df.columns or mc.n_unique < 5:
            return None
        try:
            col = df[mc.name].dropna()
            counts, edges = np.histogram(col, bins=min(20, mc.n_unique))
            data = [
                {"bin_start": round(float(edges[i]), 3),
                 "bin_end":   round(float(edges[i+1]), 3),
                 "count":     int(counts[i])}
                for i in range(len(counts))
            ]
            return AggBlock(
                agg_id=f"hist_{mc.name[:20]}",
                agg_type="hist",
                title=f"Distribution: {mc.name} (histogram)",
                data=data,
                metadata={"min": float(col.min()), "max": float(col.max()),
                          "mean": round(float(col.mean()), 3)},
            )
        except Exception as e:
            log.debug(f"Histogram failed {mc.name}: {e}")
            return None

    def _bin_agg(self, df: pd.DataFrame, bc: ColProfile, mc: ColProfile) -> Optional[AggBlock]:
        try:
            grp = df.groupby(bc.name)[mc.name].sum().reset_index()
            grp.columns = ["bin", "value"]
            data = [{"bin": str(r.bin), "value": self._fmt(float(r.value))} for r in grp.itertuples(index=False)]
            return AggBlock(
                agg_id=f"bin_{bc.name[:20]}",
                agg_type="hist",
                title=f"Binned: {mc.name} by {bc.name}",
                data=data[:50],
            )
        except Exception as e:
            log.debug(f"Bin agg failed: {e}")
            return None

    def _bool_summary(self, df: pd.DataFrame, bc: ColProfile, meas: List[ColProfile]) -> Optional[AggBlock]:
        try:
            vc = df[bc.name].value_counts()
            data = [{"value": str(k), "count": int(v)} for k, v in vc.items()]
            if meas:
                grp = df.groupby(bc.name)[meas[0].name].mean()
                for item in data:
                    v = grp.get(item["value"], np.nan)
                    item[f"avg_{meas[0].name}"] = self._fmt(float(v)) if not np.isnan(v) else "N/A"
            return AggBlock(
                agg_id=f"bool_{bc.name[:20]}",
                agg_type="dist",
                title=f"Boolean/Set: {bc.name}",
                data=data,
            )
        except Exception as e:
            log.debug(f"Bool summary failed: {e}")
            return None

    @staticmethod
    def _fmt(v: float) -> str:
        if np.isnan(v) or np.isinf(v):
            return "N/A"
        if 0 < abs(v) < 1:
            return f"{v:.2%}"
        if abs(v) >= 1_000_000:
            return f"{v/1_000_000:.2f}M"
        if abs(v) >= 1_000:
            return f"{v:,.0f}"
        return f"{v:.3f}"


# ══════════════════════════════════════════════════════════════
#  STEP 5 — BUILD LLM TEXT
# ══════════════════════════════════════════════════════════════

class LLMContextBuilder:
    """Builds LLM-ready text from profiles and visual types."""

    _VT_LABELS = {
        VT_TIME_SERIES:   "Time Series Chart",
        VT_MULTI_SERIES:  "Multi-Series Time Chart",
        VT_BAR:           "Bar Chart",
        VT_PIE:           "Pie Chart",
        VT_DONUT:         "Donut Chart",
        VT_SCATTER:       "Scatter Plot",
        VT_BUBBLE:        "Bubble Chart",
        VT_HEATMAP:       "Heat Map",
        VT_TREEMAP:       "Treemap",
        VT_KPI_CARDS:     "KPI Card Display",
        VT_KPI_SCORE:     "KPI Scorecard (RAG/Threshold)",
        VT_MAP:           "Geographic Map",
        VT_HISTOGRAM:     "Histogram",
        VT_BOX_PLOT:      "Box Plot",
        VT_WATERFALL:     "Waterfall Chart",
        VT_GANTT:         "Gantt Chart",
        VT_MULTI_MEASURE: "Multi-Measure Chart (Measure Names/Values)",
        VT_CROSSTAB:      "Cross-tab / Pivot Table",
        VT_TABLE:         "Data Table",
    }

    def build_schema(self, profiles: List[ColProfile]) -> str:
        lines = ["=== DATA SCHEMA ==="]
        for p in profiles:
            extras = []
            if p.is_temporal:       extras.append("time")
            if p.is_geo:            extras.append("geographic")
            if p.is_bin:            extras.append("bin")
            if p.is_table_calc:     extras.append("table-calc")
            if p.is_measure_special:extras.append("measure-names-values")
            extra_s = f" [{', '.join(extras)}]" if extras else ""
            stats_s = ""
            if p.stats:
                s = p.stats
                stats_s = f" | range: {s.get('min','?')}–{s.get('max','?')} | mean: {s.get('mean','?')}"
            lines.append(
                f"  {p.name} [{p.role}]{extra_s}: {p.dtype}"
                f" | {p.n_unique} unique values{stats_s}"
                f" | sample: {p.sample[:3]}"
            )
        return "\n".join(lines)

    def build_summary(
        self, source_name: str, profiles: List[ColProfile],
        visual_types: List[VisualType], df: pd.DataFrame
    ) -> str:
        n_rows, n_cols = len(df), len(df.columns)
        top_vt = visual_types[0] if visual_types else None

        dims  = [p for p in profiles if p.role == ROLE_DIM]
        meas  = [p for p in profiles if p.role == ROLE_MEASURE]
        times = [p for p in profiles if p.role == ROLE_TIME]
        geos  = [p for p in profiles if p.role == ROLE_GEO]

        parts = [
            f"{source_name}: {n_rows:,} rows × {n_cols} columns.",
            f"Primary visualization: {self._VT_LABELS.get(top_vt.vtype, top_vt.vtype) if top_vt else 'Unknown'}"
            + (f" (confidence: {top_vt.confidence:.0%})" if top_vt else ""),
        ]
        if dims:
            parts.append(f"Dimensions: {', '.join(p.name for p in dims[:6])}.")
        if meas:
            parts.append(f"Measures: {', '.join(p.name for p in meas[:6])}.")
        if times:
            parts.append(f"Time fields: {', '.join(p.name for p in times)}.")
        if geos:
            parts.append(f"Geographic fields: {', '.join(p.name for p in geos)}.")
        if len(visual_types) > 1:
            alt = visual_types[1]
            parts.append(
                f"Alternative interpretation: {self._VT_LABELS.get(alt.vtype, alt.vtype)} "
                f"({alt.confidence:.0%} confidence)."
            )
        return " ".join(parts)


# ══════════════════════════════════════════════════════════════
#  MAIN CLASS
# ══════════════════════════════════════════════════════════════

class VisualDashboardExtractor:
    """
    Universal entry point for extracting structured analytics
    from any Tableau dashboard DataFrame.

    Usage:
        df       = tableau_extractor.get_dataframe(target)
        snap     = VisualDashboardExtractor().extract(df, "my-dash", "Sales Q1")
        context  = snap.to_llm_context()
        # → inject context into Gemini system prompt

    Works for every Tableau visual type. No prior knowledge of
    dashboard structure required.
    """

    def __init__(self):
        self._normaliser  = TableauDataNormaliser()
        self._classifier  = ColumnClassifier()
        self._inferencer  = VisualTypeInferencer()
        self._aggregator  = UniversalAggregator()
        self._ctx_builder = LLMContextBuilder()

    def extract(
        self,
        df:          pd.DataFrame,
        source_id:   str  = "unknown",
        source_name: str  = "",
        view_id:     str  = "",
    ) -> DashboardSnapshot:
        """
        Full pipeline: DataFrame → DashboardSnapshot.
        Handles any Tableau visual type automatically.
        """
        import time
        t0 = time.time()

        if df is None or df.empty:
            log.warning(f"Empty DataFrame passed for {source_id}")
            return self._empty_snapshot(source_id, source_name, view_id)

        # Step 1: Normalise (fix Tableau quirks); keep reference to check flags
        normaliser = self._normaliser
        df = normaliser.normalise(df)

        if df.empty:
            return self._empty_snapshot(source_id, source_name, view_id)

        # Step 2: Classify columns
        profiles = self._classifier.classify(df)

        # Step 3: Infer visual types
        visual_types = self._inferencer.infer(df, profiles)

        # Step 4: Pre-compute aggregations
        agg_blocks = self._aggregator.compute(df, profiles)

        # Step 5: Build LLM text
        schema_text  = self._ctx_builder.build_schema(profiles)
        summary_text = self._ctx_builder.build_summary(
            source_name or source_id, profiles, visual_types, df
        )

        # Step 6: Compute flags
        has_time      = any(p.role == ROLE_TIME      for p in profiles)
        has_geo       = any(p.role == ROLE_GEO       for p in profiles)
        has_rag       = any(p.role == ROLE_STATUS     for p in profiles)
        has_threshold = any(p.role == ROLE_THRESHOLD  for p in profiles)
        # has_multi_measure: either Measure Names pattern OR still present after partial pivot
        has_multi_m   = normaliser.had_measure_names or any(p.is_measure_special for p in profiles)

        primary_vt  = visual_types[0].vtype if visual_types else VT_TABLE
        dash_label  = self._ctx_builder._VT_LABELS.get(primary_vt, primary_vt)

        snap = DashboardSnapshot(
            view_id         = view_id,
            source_id       = source_id,
            source_name     = source_name or source_id,
            total_rows      = len(df),
            total_cols      = len(df.columns),
            all_columns     = df.columns.tolist(),
            col_profiles    = profiles,
            visual_types    = visual_types,
            agg_blocks      = agg_blocks,
            schema_text     = schema_text,
            summary_text    = summary_text,
            has_time        = has_time,
            has_geo         = has_geo,
            has_rag         = has_rag,
            has_threshold   = has_threshold,
            has_multi_measure = has_multi_m,
            dashboard_type  = dash_label,
            extraction_ms   = int((time.time()-t0)*1000),
        )

        log.info(
            f"Extracted: {source_id} | {dash_label} | "
            f"{len(df)} rows × {len(df.columns)} cols | "
            f"top visual: {primary_vt} ({visual_types[0].confidence:.0%}) | "
            f"{len(agg_blocks)} agg blocks | {snap.extraction_ms}ms"
        )
        return snap

    def _empty_snapshot(self, source_id: str, source_name: str, view_id: str) -> DashboardSnapshot:
        return DashboardSnapshot(
            view_id=view_id, source_id=source_id,
            source_name=source_name or source_id,
            total_rows=0, total_cols=0, all_columns=[],
            col_profiles=[], visual_types=[], agg_blocks=[],
            schema_text="No data.", summary_text="Empty dataset — check view_id and permissions.",
            has_time=False, has_geo=False, has_rag=False,
            has_threshold=False, has_multi_measure=False,
            dashboard_type="empty",
        )
