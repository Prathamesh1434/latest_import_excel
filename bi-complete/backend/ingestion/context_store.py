"""
ingestion/context_store.py

Oracle-backed context cache.
Stores extracted + transformed Tableau data so the chatbot
doesn't re-fetch from Tableau on every question.

Cache strategy:
  - Store transformed chunks in Oracle (with TTL)
  - In-memory dict as L1 cache (per process lifetime)
  - Oracle as L2 persistent cache (survives restarts)

Package: oracledb==3.4.0
"""

from __future__ import annotations

import json
import time
import logging
from typing import Optional, Dict, List
from datetime import datetime

from backend.ingestion.data_transformer import TransformedDataset, DataChunk

log = logging.getLogger("context_store")

# ── In-memory L1 cache ─────────────────────────────────────────────────────
_L1: Dict[str, TransformedDataset] = {}
_L1_TS: Dict[str, float]           = {}
L1_TTL = 300   # 5 minutes


def _l1_get(source_id: str) -> Optional[TransformedDataset]:
    if source_id in _L1:
        age = time.time() - _L1_TS[source_id]
        if age < L1_TTL:
            log.debug(f"L1 cache HIT: {source_id} ({age:.0f}s old)")
            return _L1[source_id]
        del _L1[source_id]
        del _L1_TS[source_id]
    return None


def _l1_set(source_id: str, dataset: TransformedDataset):
    _L1[source_id]    = dataset
    _L1_TS[source_id] = time.time()
    log.debug(f"L1 cache SET: {source_id}")


# ── Oracle SQL ─────────────────────────────────────────────────────────────
CREATE_SQL = """
CREATE TABLE BI_DATA_CONTEXT (
    ID              NUMBER          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    SOURCE_ID       VARCHAR2(200)   NOT NULL,
    SOURCE_NAME     VARCHAR2(500),
    CHUNK_ID        VARCHAR2(50)    NOT NULL,
    CHUNK_TYPE      VARCHAR2(50)    NOT NULL,
    CONTENT         CLOB            NOT NULL,
    TOKEN_COUNT     NUMBER,
    METADATA        CLOB,
    TOTAL_ROWS      NUMBER,
    TOTAL_COLS      NUMBER,
    EXTRACTED_DT    TIMESTAMP       DEFAULT SYSTIMESTAMP,
    EXPIRES_DT      TIMESTAMP,
    CONSTRAINT UQ_CONTEXT_CHUNK UNIQUE (SOURCE_ID, CHUNK_ID)
)
"""

UPSERT_SQL = """
MERGE INTO BI_DATA_CONTEXT t
USING (SELECT :1 AS SOURCE_ID, :2 AS CHUNK_ID FROM DUAL) s
ON (t.SOURCE_ID = s.SOURCE_ID AND t.CHUNK_ID = s.CHUNK_ID)
WHEN MATCHED THEN UPDATE SET
    SOURCE_NAME  = :3,
    CHUNK_TYPE   = :4,
    CONTENT      = :5,
    TOKEN_COUNT  = :6,
    METADATA     = :7,
    TOTAL_ROWS   = :8,
    TOTAL_COLS   = :9,
    EXTRACTED_DT = SYSTIMESTAMP,
    EXPIRES_DT   = SYSTIMESTAMP + INTERVAL ':10' SECOND
WHEN NOT MATCHED THEN INSERT (
    SOURCE_ID, CHUNK_ID, SOURCE_NAME, CHUNK_TYPE, CONTENT,
    TOKEN_COUNT, METADATA, TOTAL_ROWS, TOTAL_COLS, EXPIRES_DT
) VALUES (
    :1, :2, :3, :4, :5, :6, :7, :8, :9,
    SYSTIMESTAMP + INTERVAL ':10' SECOND
)
"""

SELECT_SQL = """
SELECT CHUNK_TYPE, CONTENT, TOKEN_COUNT, METADATA, SOURCE_NAME, TOTAL_ROWS, TOTAL_COLS
FROM   BI_DATA_CONTEXT
WHERE  SOURCE_ID = :1
AND    (EXPIRES_DT IS NULL OR EXPIRES_DT > SYSTIMESTAMP)
ORDER  BY CHUNK_TYPE, ID
"""


class ContextStore:
    """
    Two-level cache for transformed Tableau data.

    Usage:
        store = ContextStore(pool)   # pass Oracle pool from oracle_service
        store.save(dataset, ttl_seconds=3600)
        dataset = store.load("uk-kri")
    """

    def __init__(self, oracle_pool=None, ttl_seconds: int = 3600):
        self._pool = oracle_pool
        self.ttl   = ttl_seconds

    def save(self, dataset: TransformedDataset) -> bool:
        """Save dataset to L1 + Oracle L2 cache."""
        _l1_set(dataset.source_id, dataset)

        if not self._pool:
            log.debug("Oracle pool not available — L1 cache only")
            return True

        try:
            with self._pool.acquire() as conn:
                for chunk in dataset.chunks:
                    conn.execute(
                        UPSERT_SQL.replace("':10'", str(self.ttl)),
                        [
                            dataset.source_id,
                            chunk.chunk_id,
                            dataset.source_name,
                            chunk.chunk_type,
                            chunk.content,
                            chunk.token_count,
                            json.dumps(chunk.metadata),
                            dataset.total_rows,
                            dataset.total_cols,
                        ]
                    )
                conn.commit()
            log.info(f"Saved {len(dataset.chunks)} chunks for {dataset.source_id}")
            return True
        except Exception as e:
            log.error(f"Oracle save failed: {e}")
            return False

    def load(self, source_id: str) -> Optional[TransformedDataset]:
        """Load dataset from L1, then Oracle L2."""
        # L1
        cached = _l1_get(source_id)
        if cached:
            return cached

        # L2 — Oracle
        if not self._pool:
            return None

        try:
            with self._pool.acquire() as conn:
                rows = conn.fetchall(SELECT_SQL, [source_id])

            if not rows:
                return None

            chunks, source_name, total_rows, total_cols = [], rows[0][4], rows[0][5] or 0, rows[0][6] or 0
            for r in rows:
                chunk_type, content, token_count, meta_raw = r[0], r[1], r[2], r[3]
                meta = json.loads(meta_raw) if meta_raw else {}
                chunks.append(DataChunk(
                    chunk_id    = f"{source_id}-{chunk_type}-{len(chunks)}",
                    source_id   = source_id,
                    chunk_type  = chunk_type,
                    content     = content,
                    token_count = token_count or 0,
                    metadata    = meta,
                ))

            summary = next((c.content for c in chunks if c.chunk_type == "summary"), "")
            dataset = TransformedDataset(
                source_id    = source_id,
                source_name  = source_name or source_id,
                total_rows   = total_rows,
                total_cols   = total_cols,
                columns      = [],
                chunks       = chunks,
                summary_text = summary,
            )
            _l1_set(source_id, dataset)
            log.info(f"Loaded {len(chunks)} chunks from Oracle for {source_id}")
            return dataset

        except Exception as e:
            log.error(f"Oracle load failed: {e}")
            return None

    def invalidate(self, source_id: str) -> None:
        """Force cache invalidation — triggers re-extraction on next request."""
        if source_id in _L1:
            del _L1[source_id]
            del _L1_TS[source_id]

        if not self._pool:
            return
        try:
            with self._pool.acquire() as conn:
                conn.execute(
                    "DELETE FROM BI_DATA_CONTEXT WHERE SOURCE_ID = :1",
                    [source_id]
                )
                conn.commit()
            log.info(f"Cache invalidated: {source_id}")
        except Exception as e:
            log.error(f"Oracle delete failed: {e}")

    def list_cached(self) -> List[Dict]:
        """Return all currently cached sources."""
        if not self._pool:
            return [{"source_id": k, "cached": "L1"} for k in _L1]
        try:
            with self._pool.acquire() as conn:
                rows = conn.fetchall(
                    """SELECT SOURCE_ID, SOURCE_NAME, COUNT(*) AS chunks,
                              MAX(EXTRACTED_DT) AS last_updated, MIN(EXPIRES_DT) AS expires
                       FROM   BI_DATA_CONTEXT
                       WHERE  EXPIRES_DT > SYSTIMESTAMP
                       GROUP  BY SOURCE_ID, SOURCE_NAME""",
                    []
                )
            return [
                {
                    "source_id":    r[0],
                    "source_name":  r[1],
                    "chunks":       r[2],
                    "last_updated": str(r[3]),
                    "expires":      str(r[4]),
                }
                for r in rows
            ]
        except Exception as e:
            log.error(f"list_cached failed: {e}")
            return []
