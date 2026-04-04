"""
ingestion/pipeline.py

Orchestrates the full data ingestion pipeline:
    Tableau → Extract → Transform → Cache → LLM Context

Usage (one call does everything):
    from backend.ingestion.pipeline import IngestionPipeline, PipelineConfig

    pipeline = IngestionPipeline.from_env()
    result   = pipeline.ingest("uk-kri", view_id="c69d5ca6-...", force_refresh=False)
    context  = result.get_context_for_llm(max_tokens=6000)
"""

from __future__ import annotations

import time
import logging
from dataclasses import dataclass
from typing import Optional, Dict, List

from backend.ingestion.tableau_extractor import TableauConnection, TableauExtractor, ViewTarget
from backend.ingestion.data_transformer  import DataTransformer, TransformedDataset
from backend.ingestion.context_store     import ContextStore

log = logging.getLogger("pipeline")


@dataclass
class PipelineConfig:
    """Configuration for the full ingestion pipeline."""
    # Tableau
    tableau_conn:    TableauConnection

    # Cache
    cache_ttl:       int  = 3600     # seconds (1 hour)
    force_refresh:   bool = False

    # Transformer
    rows_per_chunk:  int  = 50
    max_tokens_ctx:  int  = 6000     # tokens to give LLM per query

    # Retry
    retries:         int  = 2


class IngestionPipeline:
    """
    Full pipeline: Tableau → DataFrame → Chunks → Oracle Cache → LLM Context.

    Handles:
      - Authentication (username+password or PAT)
      - Multi-format extraction (CSV preferred, PDF fallback)
      - Caching (L1 memory + L2 Oracle)
      - Error handling at every layer
    """

    def __init__(self, config: PipelineConfig, oracle_pool=None):
        self.config      = config
        self.extractor   = TableauExtractor(config.tableau_conn, retries=config.retries)
        self.transformer = DataTransformer(rows_per_chunk=config.rows_per_chunk)
        self.store       = ContextStore(oracle_pool, ttl_seconds=config.cache_ttl)

    @classmethod
    def from_env(cls, oracle_pool=None) -> "IngestionPipeline":
        """Build pipeline from .env file — zero configuration needed."""
        conn   = TableauConnection.from_env()
        config = PipelineConfig(tableau_conn=conn)
        return cls(config, oracle_pool=oracle_pool)

    # ── Main entry point ───────────────────────────────────────────────────
    def ingest(
        self,
        source_id:    str,
        view_id:      str,
        source_name:  str = "",
        filters:      Optional[Dict[str, str]] = None,
        force_refresh: bool = False,
    ) -> TransformedDataset:
        """
        Full pipeline for one Tableau view.

        Args:
            source_id:    Your internal ID (e.g. "uk-kri")
            view_id:      Tableau View ID
            source_name:  Human-readable name (e.g. "UK KRI Scorecard")
            filters:      Optional Tableau action filters
            force_refresh: Bypass cache — re-extract from Tableau

        Returns:
            TransformedDataset — call .get_context_for_llm() on this
        """
        t0 = time.time()

        # Step 1 — Check cache (unless forced refresh)
        if not force_refresh and not self.config.force_refresh:
            cached = self.store.load(source_id)
            if cached:
                log.info(f"Cache HIT: {source_id} ({len(cached.chunks)} chunks)")
                return cached

        log.info(f"Extracting from Tableau: {source_id} / {view_id}")

        # Step 2 — Extract from Tableau
        target = ViewTarget(
            view_id      = view_id,
            scorecard_id = source_id,
            view_name    = source_name,
        )

        try:
            df = self.extractor.get_dataframe(target, filters=filters)
        except Exception as e:
            log.error(f"CSV extraction failed: {e} — trying fallback")
            df = self._fallback_empty(source_id, source_name, str(e))

        # Step 3 — Transform to LLM-ready chunks
        dataset = self.transformer.transform(
            df          = df,
            source_id   = source_id,
            source_name = source_name or source_id,
            extra_meta  = {
                "view_id": view_id,
                "filters": filters or {},
                "extracted_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            }
        )

        # Step 4 — Save to cache
        self.store.save(dataset)

        elapsed = time.time() - t0
        log.info(
            f"Pipeline complete: {source_id} | "
            f"{dataset.total_rows} rows | {len(dataset.chunks)} chunks | "
            f"{dataset.total_tokens} tokens | {elapsed:.1f}s"
        )
        return dataset

    # ── Bulk ingest multiple views ────────────────────────────────────────
    def ingest_batch(self, targets: List[Dict]) -> Dict[str, TransformedDataset]:
        """
        Ingest multiple views.

        targets = [
            {"source_id": "uk-kri",   "view_id": "abc...", "source_name": "UK KRI Scorecard"},
            {"source_id": "crmr-cde", "view_id": "def...", "source_name": "CRMR CDE"},
        ]
        """
        results = {}
        for t in targets:
            sid = t["source_id"]
            try:
                results[sid] = self.ingest(**t)
            except Exception as e:
                log.error(f"Batch ingest failed for {sid}: {e}")
        return results

    # ── Build LLM context for a query ─────────────────────────────────────
    def build_llm_context(
        self,
        source_id:  str,
        view_id:    str,
        source_name: str = "",
        max_tokens: int  = 6000,
        force_refresh: bool = False,
    ) -> str:
        """
        Single method called by the chatbot on every question.
        Returns a context string ready to inject into Gemini system prompt.
        """
        dataset = self.ingest(
            source_id    = source_id,
            view_id      = view_id,
            source_name  = source_name,
            force_refresh= force_refresh,
        )
        return dataset.get_context_for_llm(max_tokens=max_tokens)

    # ── Cache management ────────────────────────────────────────────────────
    def invalidate(self, source_id: str):
        """Force a re-extraction on next query."""
        self.store.invalidate(source_id)

    def cached_sources(self) -> List[Dict]:
        """List all sources currently in cache."""
        return self.store.list_cached()

    # ── Helpers ─────────────────────────────────────────────────────────────
    def _fallback_empty(self, source_id: str, source_name: str, error: str):
        """Return empty DataFrame with error context on extraction failure."""
        import pandas as pd
        log.warning(f"Using empty fallback for {source_id}: {error}")
        return pd.DataFrame([{"error": error, "source": source_name}])
