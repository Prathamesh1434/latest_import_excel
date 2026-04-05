"""
ingestion/data_transformer.py

Transforms raw Tableau DataFrames into LLM-ready context.

Strategy used: Chunk-based context injection (no vector DB needed).
Each chunk = one logical unit the LLM can reason about.

Packages: pandas==2.0.3  (tiktoken removed — offline token counting)
"""

from __future__ import annotations

import json
import hashlib
import logging
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional
import pandas as pd

log = logging.getLogger("data_transformer")

# Offline token counting — no tiktoken, no network calls (Copilot fix #3)
# 1 token ≈ 4 characters. Safe for corporate networks with SSL inspection.
def _count_tokens(text: str) -> int:
    return len(text) // 4


# ─────────────────────────────────────────────────────────────
# DATA MODELS
# ─────────────────────────────────────────────────────────────

@dataclass
class DataChunk:
    """
    A single unit of context ready for LLM injection.
    """
    chunk_id:    str               # hash of content
    source_id:   str               # view_id or scorecard_id
    chunk_type:  str               # "summary" | "rows" | "stats" | "schema"
    content:     str               # text the LLM reads
    token_count: int               # pre-counted tokens
    metadata:    Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict:
        return {
            "chunk_id":   self.chunk_id,
            "source_id":  self.source_id,
            "chunk_type": self.chunk_type,
            "content":    self.content,
            "tokens":     self.token_count,
            "metadata":   self.metadata,
        }


@dataclass
class TransformedDataset:
    """
    Complete transformed dataset for one Tableau view.
    Contains all chunks ready for LLM consumption.
    """
    source_id:    str
    source_name:  str
    total_rows:   int
    total_cols:   int
    columns:      List[str]
    chunks:       List[DataChunk]
    summary_text: str              # one-paragraph summary for quick context

    @property
    def total_tokens(self) -> int:
        return sum(c.token_count for c in self.chunks)

    def get_context_for_llm(self, max_tokens: int = 6000) -> str:
        """
        Build a single context string for LLM injection.
        Prioritises: summary → stats → most recent rows.
        Respects token budget.
        """
        parts   = []
        budget  = max_tokens
        priority_order = ["summary", "stats", "schema", "rows"]

        for chunk_type in priority_order:
            for chunk in self.chunks:
                if chunk.chunk_type != chunk_type:
                    continue
                if chunk.token_count <= budget:
                    parts.append(chunk.content)
                    budget -= chunk.token_count
                elif chunk_type == "rows":
                    # Truncate rows to fit budget
                    lines = chunk.content.split("\n")
                    fitted = []
                    for line in lines:
                        toks = _count_tokens(line + "\n")
                        if toks <= budget:
                            fitted.append(line)
                            budget -= toks
                        else:
                            break
                    if fitted:
                        parts.append("\n".join(fitted) + "\n[...truncated to fit token budget]")
                    break

        return "\n\n".join(parts)


# ─────────────────────────────────────────────────────────────
# TRANSFORMER
# ─────────────────────────────────────────────────────────────

class DataTransformer:
    """
    Converts a raw pandas DataFrame (from TableauExtractor)
    into a TransformedDataset ready for LLM querying.

    Usage:
        transformer = DataTransformer()
        dataset     = transformer.transform(df, source_id="uk-kri", source_name="UK KRI Scorecard")
        context     = dataset.get_context_for_llm(max_tokens=6000)
        # Inject context into Gemini system prompt
    """

    def __init__(self, rows_per_chunk: int = 50):
        self.rows_per_chunk = rows_per_chunk

    def transform(
        self,
        df:          pd.DataFrame,
        source_id:   str,
        source_name: str,
        extra_meta:  Optional[Dict] = None,
    ) -> TransformedDataset:
        """
        Main transformation entry point.
        Produces: schema chunk, stats chunk, summary chunk, row chunks.
        """
        if df.empty:
            log.warning(f"Empty DataFrame for {source_id}")
            return self._empty_dataset(source_id, source_name)

        df = self._clean(df)
        chunks = []

        # 1. Schema chunk
        chunks.append(self._make_schema_chunk(df, source_id))

        # 2. Statistical summary chunk
        chunks.append(self._make_stats_chunk(df, source_id))

        # 3. Data rows chunks (batched)
        chunks.extend(self._make_row_chunks(df, source_id))

        # 4. Summary text (plain language)
        summary = self._make_summary(df, source_id, source_name)
        chunks.insert(0, DataChunk(
            chunk_id    = self._hash(summary),
            source_id   = source_id,
            chunk_type  = "summary",
            content     = summary,
            token_count = _count_tokens(summary),
            metadata    = extra_meta or {},
        ))

        return TransformedDataset(
            source_id    = source_id,
            source_name  = source_name,
            total_rows   = len(df),
            total_cols   = len(df.columns),
            columns      = df.columns.tolist(),
            chunks       = chunks,
            summary_text = summary,
        )

    # ── Internal helpers ──────────────────────────────────────────────────

    def _clean(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and normalise DataFrame."""
        # Strip whitespace from string columns
        for col in df.select_dtypes("object").columns:
            df[col] = df[col].astype(str).str.strip()
            df[col] = df[col].replace({"nan": None, "None": None, "": None})

        # Remove fully empty rows/cols
        df = df.dropna(how="all").dropna(axis=1, how="all")

        # Normalise column names
        df.columns = [c.strip().replace("  ", " ") for c in df.columns]

        return df.reset_index(drop=True)

    def _make_schema_chunk(self, df: pd.DataFrame, source_id: str) -> DataChunk:
        """Describes the data structure."""
        lines = ["=== DATA SCHEMA ==="]
        for col in df.columns:
            dtype   = df[col].dtype
            n_unique = df[col].nunique()
            sample  = df[col].dropna().head(3).tolist()
            lines.append(f"  {col}: {dtype} | {n_unique} unique values | Sample: {sample}")
        lines.append(f"Total: {len(df)} rows × {len(df.columns)} columns")
        text = "\n".join(lines)
        return DataChunk(
            chunk_id    = self._hash(text),
            source_id   = source_id,
            chunk_type  = "schema",
            content     = text,
            token_count = _count_tokens(text),
        )

    def _make_stats_chunk(self, df: pd.DataFrame, source_id: str) -> DataChunk:
        """Statistical summary for numeric columns."""
        lines = ["=== STATISTICAL SUMMARY ==="]

        num_cols = df.select_dtypes(include=["number"]).columns.tolist()
        if num_cols:
            stats = df[num_cols].describe().round(3)
            lines.append(stats.to_string())

        # Value counts for categorical columns (top 5)
        cat_cols = df.select_dtypes(include=["object"]).columns.tolist()
        for col in cat_cols[:5]:
            vc = df[col].value_counts().head(5)
            lines.append(f"\nTop values in '{col}':")
            for val, cnt in vc.items():
                pct = round(cnt / len(df) * 100, 1)
                lines.append(f"  {val}: {cnt} ({pct}%)")

        text = "\n".join(lines)
        return DataChunk(
            chunk_id    = self._hash(text),
            source_id   = source_id,
            chunk_type  = "stats",
            content     = text,
            token_count = _count_tokens(text),
        )

    def _make_row_chunks(self, df: pd.DataFrame, source_id: str) -> List[DataChunk]:
        """Batch rows into chunks for LLM consumption."""
        chunks = []
        total  = len(df)

        for start in range(0, total, self.rows_per_chunk):
            end   = min(start + self.rows_per_chunk, total)
            batch = df.iloc[start:end]
            text  = f"=== DATA ROWS {start+1}–{end} of {total} ===\n"
            text += batch.to_csv(index=False)
            chunks.append(DataChunk(
                chunk_id    = self._hash(text),
                source_id   = source_id,
                chunk_type  = "rows",
                content     = text,
                token_count = _count_tokens(text),
                metadata    = {"row_start": start, "row_end": end, "total": total},
            ))

        return chunks

    def _make_summary(self, df: pd.DataFrame, source_id: str, source_name: str) -> str:
        """Generate a plain-language summary."""
        num_cols = df.select_dtypes("number").columns.tolist()
        cat_cols = df.select_dtypes("object").columns.tolist()

        lines = [
            f"=== LIVE DATA SUMMARY: {source_name} ===",
            f"Source: {source_id} | Rows: {len(df)} | Columns: {len(df.columns)}",
            f"Numeric fields: {', '.join(num_cols) if num_cols else 'none'}",
            f"Categorical fields: {', '.join(cat_cols) if cat_cols else 'none'}",
        ]

        # Detect % columns and show range
        for col in num_cols:
            if any(k in col.lower() for k in ["rate", "pct", "percent", "ratio", "%"]):
                mn, mx = df[col].min(), df[col].max()
                lines.append(f"  {col}: range {mn:.2%} – {mx:.2%}")

        # Most recent row if there's a date column
        date_cols = [c for c in df.columns if any(k in c.lower() for k in ["date", "month", "period"])]
        if date_cols:
            lines.append(f"Date/period columns detected: {date_cols}")

        return "\n".join(lines)

    def _empty_dataset(self, source_id: str, source_name: str) -> TransformedDataset:
        msg = f"No data available from {source_name} ({source_id})"
        chunk = DataChunk(
            chunk_id    = self._hash(msg),
            source_id   = source_id,
            chunk_type  = "summary",
            content     = msg,
            token_count = _count_tokens(msg),
        )
        return TransformedDataset(
            source_id    = source_id,
            source_name  = source_name,
            total_rows   = 0,
            total_cols   = 0,
            columns      = [],
            chunks       = [chunk],
            summary_text = msg,
        )

    @staticmethod
    def _hash(text: str) -> str:
        return hashlib.md5(text.encode()).hexdigest()[:12]
