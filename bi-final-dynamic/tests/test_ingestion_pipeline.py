"""
tests/test_ingestion_pipeline.py

Validates the full ingestion pipeline end-to-end.
Run with real or mocked Tableau credentials.

Usage:
    conda activate prath
    cd bi-final
    python tests/test_ingestion_pipeline.py           # real Tableau
    python tests/test_ingestion_pipeline.py --mock    # mock data (no Tableau)
"""

import sys
import os
import argparse
import pandas as pd
import logging

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
logging.basicConfig(level=logging.INFO, format="%(name)s: %(message)s")

from backend.ingestion.tableau_extractor import TableauConnection, TableauExtractor, ViewTarget
from backend.ingestion.data_transformer  import DataTransformer
from backend.ingestion.context_store     import ContextStore
from backend.ingestion.pipeline          import IngestionPipeline, PipelineConfig

# ── Mock DataFrame (mirrors UK KRI Scorecard structure) ───────────────────
MOCK_DF = pd.DataFrame([
    {"KRI_ID":"UK-K40","KRI_NAME":"% high-risk DCs with measures","CURRENT_VALUE":0.95,"PREVIOUS_VALUE":0.88,"RED_THRESHOLD":0.80,"RAG_STATUS":"Red","MONTH":"Feb","YEAR":2026},
    {"KRI_ID":"UK-K41","KRI_NAME":"% DCs meeting SLA phase dates", "CURRENT_VALUE":0.08,"PREVIOUS_VALUE":0.19,"RED_THRESHOLD":0.70,"RAG_STATUS":"Red","MONTH":"Feb","YEAR":2026},
    {"KRI_ID":"UK-K42","KRI_NAME":"% open DCs inside timelines",   "CURRENT_VALUE":0.89,"PREVIOUS_VALUE":0.85,"RED_THRESHOLD":0.65,"RAG_STATUS":"Red","MONTH":"Feb","YEAR":2026},
    {"KRI_ID":"UK-K43","KRI_NAME":"# high severity DQ issues",     "CURRENT_VALUE":2.47,"PREVIOUS_VALUE":2.47,"RED_THRESHOLD":1.00,"RAG_STATUS":"Red","MONTH":"Feb","YEAR":2026},
    {"KRI_ID":"UK-K40","KRI_NAME":"% high-risk DCs with measures","CURRENT_VALUE":0.94,"PREVIOUS_VALUE":0.93,"RED_THRESHOLD":0.80,"RAG_STATUS":"Red","MONTH":"Jan","YEAR":2026},
    {"KRI_ID":"UK-K41","KRI_NAME":"% DCs meeting SLA phase dates", "CURRENT_VALUE":0.19,"PREVIOUS_VALUE":0.00,"RED_THRESHOLD":0.70,"RAG_STATUS":"Red","MONTH":"Jan","YEAR":2026},
])


def test_transformer(df: pd.DataFrame):
    print("\n" + "="*60)
    print("TEST 1: Data Transformer")
    print("="*60)

    transformer = DataTransformer(rows_per_chunk=3)
    dataset     = transformer.transform(df, source_id="uk-kri", source_name="UK KRI Scorecard")

    print(f"✅ Rows:         {dataset.total_rows}")
    print(f"✅ Columns:      {dataset.total_cols}")
    print(f"✅ Chunks:       {len(dataset.chunks)}")
    print(f"✅ Total tokens: {dataset.total_tokens}")
    print(f"\nChunk types: {[c.chunk_type for c in dataset.chunks]}")
    print(f"\nSummary:\n{dataset.summary_text}")

    ctx = dataset.get_context_for_llm(max_tokens=2000)
    print(f"\nLLM Context ({len(ctx)} chars):\n{ctx[:500]}...")

    assert dataset.total_rows   == len(df)
    assert dataset.total_tokens  > 0
    assert any(c.chunk_type == "summary" for c in dataset.chunks)
    assert any(c.chunk_type == "stats"   for c in dataset.chunks)
    assert any(c.chunk_type == "rows"    for c in dataset.chunks)
    print("\n✅ Transformer test PASSED")
    return dataset


def test_context_store(dataset):
    print("\n" + "="*60)
    print("TEST 2: Context Store (L1 cache only)")
    print("="*60)

    store = ContextStore(oracle_pool=None)   # no Oracle in test
    saved = store.save(dataset)
    print(f"✅ Save returned: {saved}")

    loaded = store.load("uk-kri")
    assert loaded is not None
    assert loaded.source_id == "uk-kri"
    assert len(loaded.chunks) == len(dataset.chunks)
    print(f"✅ Loaded {len(loaded.chunks)} chunks from L1 cache")

    store.invalidate("uk-kri")
    after = store.load("uk-kri")
    assert after is None
    print("✅ Cache invalidation works")

    print("\n✅ Context Store test PASSED")


def test_pipeline_mock():
    print("\n" + "="*60)
    print("TEST 3: Pipeline (mock data)")
    print("="*60)

    from unittest.mock import patch, MagicMock

    mock_conn = TableauConnection(
        server_url="https://mock.tableau.server",
        username="mock_user",
        password="mock_pass",
        site_id="MOCK_SITE",
    )
    config   = PipelineConfig(tableau_conn=mock_conn)
    pipeline = IngestionPipeline(config, oracle_pool=None)

    # Patch the extractor to return our mock DataFrame
    with patch.object(pipeline.extractor, "get_dataframe", return_value=MOCK_DF):
        dataset = pipeline.ingest(
            source_id   = "uk-kri",
            view_id     = "mock-view-id",
            source_name = "UK KRI Scorecard (Mock)",
        )

    print(f"✅ Rows: {dataset.total_rows}")
    print(f"✅ Chunks: {len(dataset.chunks)}")

    ctx = pipeline.build_llm_context(
        source_id   = "uk-kri",
        view_id     = "mock-view-id",
        max_tokens  = 3000,
    )
    print(f"✅ Context built: {len(ctx)} chars")
    assert "UK-K41" in ctx or "KRI" in ctx.upper()
    print("\n✅ Pipeline mock test PASSED")


def test_extractor_live(view_id: str):
    print("\n" + "="*60)
    print("TEST 4: Live Tableau Extraction")
    print("="*60)

    conn = TableauConnection.from_env()
    if not conn.server_url:
        print("⚠️  TABLEAU_SERVER not set — skipping live test")
        return

    extractor = TableauExtractor(conn)
    target    = ViewTarget(view_id=view_id, scorecard_id="test")

    print(f"Connecting to: {conn.server_url}")
    print(f"View ID: {view_id}")

    try:
        df = extractor.get_dataframe(target)
        print(f"✅ Extracted {len(df)} rows × {len(df.columns)} cols")
        print(f"Columns: {df.columns.tolist()}")
        print(df.head(3).to_string())
    except Exception as e:
        print(f"❌ Extraction failed: {e}")
        raise


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mock",    action="store_true", help="Use mock data (no Tableau)")
    parser.add_argument("--view-id", default="",          help="Tableau View ID for live test")
    args = parser.parse_args()

    # Always run these (no Tableau needed)
    dataset = test_transformer(MOCK_DF)
    test_context_store(dataset)
    test_pipeline_mock()

    # Live test only if requested
    if not args.mock and args.view_id:
        test_extractor_live(args.view_id)
    elif not args.mock:
        print("\n💡 To run live test: python tests/test_ingestion_pipeline.py --view-id YOUR_VIEW_ID")

    print("\n" + "="*60)
    print("ALL TESTS PASSED ✅")
    print("="*60)


if __name__ == "__main__":
    main()
