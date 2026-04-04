"""
ingestion/dynamic_pipeline.py

Fully dynamic pipeline for ANY Tableau dashboard.
Zero hardcoding. Zero manual configuration.

One call does everything:
  1. Connect to Tableau (any dashboard)
  2. Extract data (CSV → DataFrame)
  3. Analyse schema automatically
  4. Generate relevant questions
  5. Build LLM-ready context
  6. Cache in Oracle

The DashboardSession object is what the chatbot uses
for every subsequent question.
"""

from __future__ import annotations

import io
import time
import uuid
import json
import logging
import pandas as pd
from dataclasses import dataclass, field
from typing import Optional, Dict, List, Any

from backend.ingestion.tableau_extractor  import TableauConnection, TableauExtractor, ViewTarget
from backend.ingestion.data_transformer   import DataTransformer, TransformedDataset
from backend.ingestion.schema_analyser    import SchemaAnalyser, SchemaProfile
from backend.ingestion.question_generator import DynamicQuestionGenerator
from backend.ingestion.universal_answerer import UniversalAnswerer, QueryAnswer
from backend.ingestion.context_store      import ContextStore

log = logging.getLogger("dynamic_pipeline")


# ─────────────────────────────────────────────────────────────
# DASHBOARD SESSION — everything the chatbot needs
# ─────────────────────────────────────────────────────────────

@dataclass
class DashboardSession:
    """
    Complete session for one connected Tableau dashboard.
    Created once, used for all chatbot interactions.
    """
    session_id:     str
    source_id:      str
    source_name:    str
    view_id:        str
    dashboard_type: str          # kri | scorecard | compliance | sales | generic
    created_at:     float = field(default_factory=time.time)

    # Data
    df:             Optional[pd.DataFrame] = None
    dataset:        Optional[TransformedDataset] = None
    profile:        Optional[SchemaProfile] = None

    # Generated content
    suggested_questions: List[str] = field(default_factory=list)
    schema_summary:      str       = ""
    llm_context:         str       = ""    # pre-built for Gemini

    # Stats
    total_rows:     int  = 0
    total_cols:     int  = 0
    extraction_ms:  int  = 0
    ready:          bool = False

    def answer_directly(self, question: str) -> Optional[QueryAnswer]:
        """Try to answer without AI. Returns None if escalation needed."""
        if self.df is None or self.profile is None:
            return None
        try:
            answerer = UniversalAnswerer(self.df, self.profile)
            result   = answerer.answer(question)
            return result if result.answered else None
        except Exception as e:
            log.warning(f"Direct answer failed: {e}")
            return None

    def to_dict(self) -> Dict:
        return {
            "session_id":     self.session_id,
            "source_id":      self.source_id,
            "source_name":    self.source_name,
            "view_id":        self.view_id,
            "dashboard_type": self.dashboard_type,
            "total_rows":     self.total_rows,
            "total_cols":     self.total_cols,
            "ready":          self.ready,
            "extraction_ms":  self.extraction_ms,
            "schema_summary": self.schema_summary[:500],
            "suggested_questions": self.suggested_questions,
            "capabilities": {
                "has_time_series": self.profile.has_time_series if self.profile else False,
                "has_rag_status":  self.profile.has_rag_status  if self.profile else False,
                "has_thresholds":  self.profile.has_thresholds  if self.profile else False,
                "has_variance":    self.profile.has_variance     if self.profile else False,
                "kpi_count":       len(self.profile.kpi_patterns) if self.profile else 0,
                "dimensions":      self.profile.dimensions if self.profile else [],
                "measures":        self.profile.measures   if self.profile else [],
            }
        }


# ─────────────────────────────────────────────────────────────
# DYNAMIC PIPELINE
# ─────────────────────────────────────────────────────────────

# In-memory session registry (per process)
# For production: persist to Oracle using BI_DASHBOARD_SESSION table
_SESSIONS: Dict[str, DashboardSession] = {}


class DynamicPipeline:
    """
    Fully dynamic pipeline. Connects to ANY Tableau dashboard
    and makes it immediately queryable.

    Usage:
        pipeline = DynamicPipeline(conn)
        session  = pipeline.connect(view_id="abc-123", source_name="My Dashboard")

        # Chatbot interaction
        answer = session.answer_directly("Which items are red?")
        if answer:
            return answer          # instant, no AI
        else:
            context = session.llm_context
            return gemini.chat(context, question)   # AI fallback
    """

    def __init__(
        self,
        conn:         TableauConnection,
        oracle_pool   = None,
        retries:      int = 2,
        cache_ttl:    int = 3600,
    ):
        self.extractor   = TableauExtractor(conn, retries=retries)
        self.transformer = DataTransformer(rows_per_chunk=50)
        self.analyser    = SchemaAnalyser()
        self.generator   = DynamicQuestionGenerator()
        self.store       = ContextStore(oracle_pool, ttl_seconds=cache_ttl)

    @classmethod
    def from_env(cls, oracle_pool=None) -> "DynamicPipeline":
        from backend.config import (TABLEAU_SERVER, TABLEAU_USERNAME, TABLEAU_PASSWORD,
                                     TABLEAU_SITE, TABLEAU_API_VERSION, TABLEAU_SSL_CERT)
        conn = TableauConnection(
            server_url    = TABLEAU_SERVER,
            username      = TABLEAU_USERNAME,
            password      = TABLEAU_PASSWORD,
            site_id       = TABLEAU_SITE,
            api_version   = TABLEAU_API_VERSION,
            ssl_cert_path = TABLEAU_SSL_CERT,
        )
        return cls(conn, oracle_pool=oracle_pool)

    # ── Main connect method ───────────────────────────────────────────────

    def connect(
        self,
        view_id:      str,
        source_id:    str           = "",
        source_name:  str           = "",
        filters:      Optional[Dict]= None,
        force_refresh: bool         = False,
        max_questions: int          = 8,
    ) -> DashboardSession:
        """
        Connect to any Tableau dashboard and build a queryable session.

        Args:
            view_id:      Tableau View ID (from URL or list_views())
            source_id:    Optional internal ID (auto-generated if blank)
            source_name:  Optional human name (auto-detected if blank)
            filters:      Optional Tableau action filters
            force_refresh: Re-extract even if cached
            max_questions: How many suggestion chips to generate

        Returns:
            DashboardSession — ready for chatbot queries
        """
        t0        = time.time()
        source_id = source_id or self._slug(view_id)
        session   = DashboardSession(
            session_id  = str(uuid.uuid4()),
            source_id   = source_id,
            source_name = source_name or source_id,
            view_id     = view_id,
            dashboard_type = "generic",
        )

        # Step 1 — Check cache
        if not force_refresh:
            cached_dataset = self.store.load(source_id)
            if cached_dataset:
                df = self._dataset_to_df(cached_dataset)
                if df is not None:
                    return self._finalise(session, df, cached_dataset, t0, max_questions)

        # Step 2 — Extract from Tableau
        target = ViewTarget(view_id=view_id, scorecard_id=source_id, view_name=source_name)
        try:
            df = self.extractor.get_dataframe(target, filters=filters)
            log.info(f"Extracted {len(df)} rows from Tableau: {view_id}")
        except Exception as e:
            log.error(f"Extraction failed: {e}")
            session.ready = False
            session.schema_summary = f"Extraction failed: {e}"
            return session

        if df.empty:
            log.warning(f"Empty DataFrame for {view_id}")
            session.ready = False
            session.schema_summary = "Dashboard returned no data."
            return session

        # Step 3 — Auto-detect source name from data if not provided
        if not source_name:
            source_name = self._detect_name(df, view_id)
            session.source_name = source_name

        # Step 4 — Transform and cache
        dataset = self.transformer.transform(df, source_id, source_name)
        self.store.save(dataset)

        return self._finalise(session, df, dataset, t0, max_questions)

    def _finalise(
        self,
        session:      DashboardSession,
        df:           pd.DataFrame,
        dataset:      TransformedDataset,
        t0:           float,
        max_questions: int,
    ) -> DashboardSession:
        """Complete session setup after data is ready."""

        # Analyse schema
        profile = self.analyser.analyse(df, session.source_id, session.source_name)

        # Generate questions
        questions = self.generator.generate(profile, max_q=max_questions)

        # Build LLM context (used when AI is needed)
        llm_ctx = (
            f"=== DASHBOARD: {session.source_name} ===\n"
            f"Type: {profile.dashboard_type} | Rows: {len(df)} | Cols: {len(df.columns)}\n\n"
            f"{profile.schema_summary}\n\n"
            f"{dataset.get_context_for_llm(max_tokens=5000)}"
        )

        # Populate session
        session.df                  = df
        session.dataset             = dataset
        session.profile             = profile
        session.dashboard_type      = profile.dashboard_type
        session.source_name         = session.source_name
        session.total_rows          = len(df)
        session.total_cols          = len(df.columns)
        session.suggested_questions = questions
        session.schema_summary      = profile.schema_summary
        session.llm_context         = llm_ctx
        session.extraction_ms       = int((time.time() - t0) * 1000)
        session.ready               = True

        # Register in session store
        _SESSIONS[session.session_id] = session
        _SESSIONS[session.source_id]  = session   # also by source_id

        log.info(
            f"Session ready: {session.source_id} | {session.dashboard_type} | "
            f"{len(df)} rows | {len(questions)} questions | {session.extraction_ms}ms"
        )
        return session

    # ── Session retrieval ──────────────────────────────────────────────────

    def get_session(self, session_or_source_id: str) -> Optional[DashboardSession]:
        return _SESSIONS.get(session_or_source_id)

    def list_sessions(self) -> List[Dict]:
        seen = set()
        result = []
        for k, s in _SESSIONS.items():
            if s.session_id not in seen:
                seen.add(s.session_id)
                result.append(s.to_dict())
        return result

    def disconnect(self, source_id: str):
        self.store.invalidate(source_id)
        _SESSIONS.pop(source_id, None)
        # Remove session_id entry too
        for k, v in list(_SESSIONS.items()):
            if v.source_id == source_id:
                del _SESSIONS[k]

    # ── Auto-discover workbooks ────────────────────────────────────────────

    def discover_workbooks(self) -> List[Dict]:
        """List all available workbooks on the Tableau site."""
        return self.extractor.list_workbooks()

    def discover_views(self, workbook_id: str) -> List[Dict]:
        """List all views in a workbook."""
        return self.extractor.list_views(workbook_id)

    # ── Helpers ───────────────────────────────────────────────────────────

    def _slug(self, view_id: str) -> str:
        return "dash-" + view_id.replace("-","")[:12]

    def _detect_name(self, df: pd.DataFrame, view_id: str) -> str:
        """Guess dashboard name from column names."""
        cols = " ".join(df.columns.tolist()).lower()
        if any(k in cols for k in ["kri","risk indicator"]):
            return "KRI Dashboard"
        if any(k in cols for k in ["control","compliance","breach"]):
            return "Compliance Dashboard"
        if any(k in cols for k in ["revenue","sales","pipeline"]):
            return "Sales Dashboard"
        if any(k in cols for k in ["scorecard","kpi","performance"]):
            return "Scorecard Dashboard"
        return f"Dashboard ({view_id[:8]})"

    def _dataset_to_df(self, dataset: TransformedDataset) -> Optional[pd.DataFrame]:
        """Reconstruct DataFrame from cached row chunks."""
        row_chunks = [c.content for c in dataset.chunks if c.chunk_type == "rows"]
        if not row_chunks:
            return None
        try:
            lines  = []
            header = None
            for chunk in row_chunks:
                chunk_lines = chunk.split("\n")
                # Skip the "=== DATA ROWS N-M ===" header line
                data_lines = [l for l in chunk_lines if not l.startswith("=")]
                if not header and data_lines:
                    header = data_lines[0]
                    lines.extend(data_lines)
                else:
                    lines.extend(data_lines[1:])   # skip repeated header
            csv_str = "\n".join(lines)
            return pd.read_csv(io.StringIO(csv_str))
        except Exception as e:
            log.warning(f"DataFrame reconstruction failed: {e}")
            return None
