"""
tests/test_dynamic_system.py
Full tests for Schema Analyser, Question Generator, Universal Answerer.
All pure-pandas, no Tableau, no AI needed.
Run:
    conda activate prath && cd bi-final
    python tests/test_dynamic_system.py
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pandas as pd
import numpy as np
from backend.ingestion.schema_analyser    import SchemaAnalyser
from backend.ingestion.question_generator import DynamicQuestionGenerator
from backend.ingestion.universal_answerer import UniversalAnswerer

PASS = "✅"; FAIL = "❌"
_failed = []

def ok(name):    print(f"  {PASS} {name}")
def err(name, e):
    print(f"  {FAIL} {name}: {e}")
    _failed.append(name)
def section(title):
    print(f"\n{'='*60}\n{title}\n{'='*60}")

# ─── TEST DATA ────────────────────────────────────────────────────────────

def make_kri_df():
    return pd.DataFrame([
        {"KRI_ID":"UK-K40","KRI_NAME":"% high-risk DCs","CURRENT_VALUE":0.95,"PREVIOUS_VALUE":0.88,"RED_THRESHOLD":0.80,"RAG_STATUS":"Red","REGION":"UK","REPORT_MONTH":"Feb-26"},
        {"KRI_ID":"UK-K41","KRI_NAME":"% DCs meeting SLA","CURRENT_VALUE":0.08,"PREVIOUS_VALUE":0.19,"RED_THRESHOLD":0.70,"RAG_STATUS":"Red","REGION":"UK","REPORT_MONTH":"Feb-26"},
        {"KRI_ID":"UK-K42","KRI_NAME":"% open DCs","CURRENT_VALUE":0.89,"PREVIOUS_VALUE":0.85,"RED_THRESHOLD":0.65,"RAG_STATUS":"Red","REGION":"UK","REPORT_MONTH":"Feb-26"},
        {"KRI_ID":"UK-K43","KRI_NAME":"# DQ issues","CURRENT_VALUE":2.47,"PREVIOUS_VALUE":2.47,"RED_THRESHOLD":1.00,"RAG_STATUS":"Red","REGION":"UK","REPORT_MONTH":"Feb-26"},
        {"KRI_ID":"UK-K40","KRI_NAME":"% high-risk DCs","CURRENT_VALUE":0.94,"PREVIOUS_VALUE":0.93,"RED_THRESHOLD":0.80,"RAG_STATUS":"Red","REGION":"UK","REPORT_MONTH":"Jan-26"},
        {"KRI_ID":"UK-K41","KRI_NAME":"% DCs meeting SLA","CURRENT_VALUE":0.19,"PREVIOUS_VALUE":0.00,"RED_THRESHOLD":0.70,"RAG_STATUS":"Red","REGION":"UK","REPORT_MONTH":"Jan-26"},
        {"KRI_ID":"SGP-K1","KRI_NAME":"SGP DQ Score","CURRENT_VALUE":0.94,"PREVIOUS_VALUE":0.93,"RED_THRESHOLD":0.90,"RAG_STATUS":"Green","REGION":"SGP","REPORT_MONTH":"Feb-26"},
        {"KRI_ID":"SGP-K1","KRI_NAME":"SGP DQ Score","CURRENT_VALUE":0.92,"PREVIOUS_VALUE":0.91,"RED_THRESHOLD":0.90,"RAG_STATUS":"Amber","REGION":"SGP","REPORT_MONTH":"Jan-26"},
    ])

def make_sales_df():
    return pd.DataFrame([
        {"REGION":"North","PRODUCT":"A","REVENUE":120000,"QUOTA":100000,"CLOSED":"Yes","QUARTER":"Q1"},
        {"REGION":"South","PRODUCT":"B","REVENUE":85000, "QUOTA":100000,"CLOSED":"No", "QUARTER":"Q1"},
        {"REGION":"East", "PRODUCT":"A","REVENUE":150000,"QUOTA":120000,"CLOSED":"Yes","QUARTER":"Q2"},
        {"REGION":"West", "PRODUCT":"C","REVENUE":60000, "QUOTA":90000, "CLOSED":"No", "QUARTER":"Q2"},
        {"REGION":"North","PRODUCT":"B","REVENUE":110000,"QUOTA":100000,"CLOSED":"Yes","QUARTER":"Q2"},
        {"REGION":"South","PRODUCT":"A","REVENUE":95000, "QUOTA":100000,"CLOSED":"Yes","QUARTER":"Q1"},
    ])

def make_cde_df():
    return pd.DataFrame([
        {"CDE_NAME":"Security Id","FAIL_COUNT":8,"DIMENSION":"Completeness","STATUS":"Fail Red","REPORT_DATE":"Feb-26"},
        {"CDE_NAME":"Currency Code","FAIL_COUNT":4,"DIMENSION":"Completeness","STATUS":"Fail Red","REPORT_DATE":"Feb-26"},
        {"CDE_NAME":"Product Class","FAIL_COUNT":4,"DIMENSION":"Completeness","STATUS":"Fail Amber","REPORT_DATE":"Feb-26"},
        {"CDE_NAME":"Market Value","FAIL_COUNT":1,"DIMENSION":"Accuracy","STATUS":"Fail Amber","REPORT_DATE":"Feb-26"},
        {"CDE_NAME":"Notional","FAIL_COUNT":0,"DIMENSION":"Completeness","STATUS":"Pass","REPORT_DATE":"Feb-26"},
        {"CDE_NAME":"Risk Sensitivity","FAIL_COUNT":0,"DIMENSION":"Accuracy","STATUS":"Pass","REPORT_DATE":"Feb-26"},
    ])


# ─── SCHEMA ANALYSER TESTS ────────────────────────────────────────────────
section("1. Schema Analyser — KRI Dashboard")
try:
    analyser = SchemaAnalyser()
    df       = make_kri_df()
    profile  = analyser.analyse(df, source_id="uk-kri", source_name="UK KRI Scorecard")

    try:
        assert profile.dashboard_type == "kri", f"Expected kri, got {profile.dashboard_type}"
        ok("Dashboard type inferred as 'kri'")
    except AssertionError as e: err("Dashboard type", e)

    try:
        assert "KRI_ID" in profile.dimensions or "KRI_NAME" in profile.dimensions
        ok(f"Dimensions detected: {profile.dimensions}")
    except AssertionError as e: err("Dimensions", e)

    try:
        assert len(profile.measures) >= 2
        ok(f"Measures detected: {profile.measures}")
    except AssertionError as e: err("Measures", e)

    try:
        assert profile.has_time_series and len(profile.time_cols) > 0
        ok(f"Time series detected: {profile.time_cols}")
    except AssertionError as e: err("Time series", e)

    try:
        assert profile.has_rag_status and len(profile.status_cols) > 0
        ok(f"RAG status detected: {profile.status_cols}")
    except AssertionError as e: err("RAG status", e)

    try:
        assert profile.has_thresholds
        ok("Thresholds detected")
    except AssertionError as e: err("Thresholds", e)

    try:
        assert profile.has_variance
        ok("Variance columns detected")
    except AssertionError as e: err("Variance", e)

    try:
        assert len(profile.kpi_patterns) > 0
        breaching = [k for k in profile.kpi_patterns if k.breach_detected]
        ok(f"KPI patterns detected: {len(profile.kpi_patterns)} total, {len(breaching)} breaching")
    except AssertionError as e: err("KPI patterns", e)

    try:
        assert len(profile.schema_summary) > 100
        ok(f"Schema summary generated ({len(profile.schema_summary)} chars)")
    except AssertionError as e: err("Schema summary", e)

except Exception as e:
    err("Schema Analyser (fatal)", e)

section("2. Schema Analyser — Sales Dashboard")
try:
    df      = make_sales_df()
    profile = analyser.analyse(df, source_id="sales", source_name="Sales Q1/Q2")
    try:
        assert profile.dashboard_type == "sales", f"Expected sales, got {profile.dashboard_type}"
        ok(f"Sales dashboard type: {profile.dashboard_type}")
    except AssertionError as e: err("Sales type", e)
    try:
        assert "REVENUE" in profile.measures or "QUOTA" in profile.measures
        ok(f"Sales measures: {profile.measures}")
    except AssertionError as e: err("Sales measures", e)
except Exception as e:
    err("Sales analyser (fatal)", e)

section("3. Schema Analyser — CDE Compliance")
try:
    df      = make_cde_df()
    profile = analyser.analyse(df, source_id="cde", source_name="CRMR CDE")
    try:
        assert profile.dashboard_type in ("compliance","kri","scorecard","generic")
        ok(f"CDE dashboard type: {profile.dashboard_type}")
    except AssertionError as e: err("CDE type", e)
    try:
        assert len(profile.dimensions) > 0
        ok(f"CDE dimensions: {profile.dimensions}")
    except AssertionError as e: err("CDE dims", e)
except Exception as e:
    err("CDE analyser (fatal)", e)


# ─── QUESTION GENERATOR TESTS ─────────────────────────────────────────────
section("4. Question Generator — KRI")
try:
    gen     = DynamicQuestionGenerator()
    df      = make_kri_df()
    profile = analyser.analyse(df, source_id="uk-kri")
    qs      = gen.generate(profile, max_q=8)

    try:
        assert len(qs) > 0 and len(qs) <= 8
        ok(f"Generated {len(qs)} questions")
    except AssertionError as e: err("Question count", e)

    try:
        # All questions should be non-empty strings
        assert all(isinstance(q, str) and len(q) > 10 for q in qs)
        ok("All questions are valid strings")
    except AssertionError as e: err("Question validity", e)

    try:
        qs_text = " ".join(qs).lower()
        has_breach = any(w in qs_text for w in ["breach","threshold","red","fail"])
        assert has_breach, "No breach-related questions generated for KRI dashboard"
        ok("Breach questions generated for KRI dashboard")
    except AssertionError as e: err("Breach questions", e)

    print("  Sample questions:")
    for q in qs[:4]:
        print(f"    → {q}")

except Exception as e:
    err("Question Generator (fatal)", e)

section("5. Question Generator — Sales")
try:
    df      = make_sales_df()
    profile = analyser.analyse(df, source_id="sales")
    qs      = gen.generate(profile, max_q=8)
    qs_text = " ".join(qs).lower()
    try:
        assert any(w in qs_text for w in ["region","rank","highest","lowest","compare","average"])
        ok(f"Sales-relevant questions: {len(qs)}")
    except AssertionError as e: err("Sales questions", e)
    for q in qs[:3]:
        print(f"    → {q}")
except Exception as e:
    err("Sales questions (fatal)", e)

section("6. Question Generator — Chip format")
try:
    df      = make_kri_df()
    profile = analyser.analyse(df, source_id="uk-kri")
    chips   = gen.generate_as_chips(profile, max_q=5)
    try:
        assert all("text" in c and "source_id" in c for c in chips)
        ok(f"Chip format correct: {len(chips)} chips")
    except AssertionError as e: err("Chip format", e)
except Exception as e:
    err("Chip format (fatal)", e)


# ─── UNIVERSAL ANSWERER TESTS ─────────────────────────────────────────────
section("7. Universal Answerer — Breach Detection")
try:
    df      = make_kri_df()
    profile = analyser.analyse(df, source_id="uk-kri")
    ans     = UniversalAnswerer(df, profile)

    for q in ["Which KRIs are breaching?", "Show all failed KRIs", "which are in breach"]:
        result = ans.answer(q)
        try:
            assert result.confidence > 0.5
            assert len(result.reply) > 0
            ok(f"Breach Q: '{q[:40]}' → conf={result.confidence}")
        except AssertionError as e:
            err(f"Breach Q: '{q[:40]}'", e)
except Exception as e:
    err("Breach detection (fatal)", e)

section("8. Universal Answerer — RAG Filter")
try:
    result = ans.answer("Show all Red status items")
    try:
        assert result.confidence >= 0.8
        assert result.row_count > 0
        ok(f"RAG filter: {result.row_count} rows, conf={result.confidence}")
    except AssertionError as e: err("RAG filter", e)
except Exception as e:
    err("RAG filter (fatal)", e)

section("9. Universal Answerer — Trend")
try:
    result = ans.answer("Show the trend of CURRENT_VALUE over REPORT_MONTH")
    try:
        assert result.chart is not None
        assert result.chart["chart_type"] == "line"
        ok(f"Trend: chart_type=line, labels={result.chart.get('labels')[:3]}")
    except AssertionError as e: err("Trend chart", e)
except Exception as e:
    err("Trend (fatal)", e)

section("10. Universal Answerer — Highest/Lowest")
try:
    result = ans.answer("Which KRI has the highest value?")
    try:
        assert result.confidence >= 0.8
        ok(f"Highest: {result.reply[:60]}")
    except AssertionError as e: err("Highest", e)

    result2 = ans.answer("Which KRI is performing worst?")
    try:
        assert result2.confidence >= 0.8
        ok(f"Lowest: {result2.reply[:60]}")
    except AssertionError as e: err("Lowest", e)
except Exception as e:
    err("Highest/lowest (fatal)", e)

section("11. Universal Answerer — Summary + KPI Cards")
try:
    result = ans.answer("Give me a summary of this dashboard")
    try:
        assert result.chart is not None
        assert result.chart["chart_type"] == "kpi"
        assert len(result.chart["kpis"]) > 0
        ok(f"Summary: kpi chart with {len(result.chart['kpis'])} cards")
    except AssertionError as e: err("Summary KPI", e)
except Exception as e:
    err("Summary (fatal)", e)

section("12. Universal Answerer — Distribution")
try:
    result = ans.answer("Show RAG_STATUS distribution")
    try:
        assert result.chart is not None
        assert result.chart["chart_type"] in ("doughnut","pie","bar")
        ok(f"Distribution: {result.chart['chart_type']} — {result.reply[:60]}")
    except AssertionError as e: err("Distribution chart", e)
except Exception as e:
    err("Distribution (fatal)", e)

section("13. Universal Answerer — Variance")
try:
    result = ans.answer("What changed vs previous period?")
    try:
        assert result.confidence >= 0.5
        ok(f"Variance: {result.reply[:70]}")
    except AssertionError as e: err("Variance", e)
except Exception as e:
    err("Variance (fatal)", e)

section("14. Universal Answerer — Sales DataFrame")
try:
    df2      = make_sales_df()
    profile2 = analyser.analyse(df2, source_id="sales")
    ans2     = UniversalAnswerer(df2, profile2)

    for q, expected_chart in [
        ("Which region has highest revenue?",          ["bar","table"]),
        ("Show revenue breakdown",                     ["bar","doughnut","pie"]),
        ("Average revenue by region",                  ["bar","kpi"]),
        ("Total revenue",                              ["kpi","table"]),
    ]:
        result = ans2.answer(q)
        try:
            assert result.confidence > 0.3
            if result.chart:
                assert result.chart["chart_type"] in expected_chart + ["table","kpi","bar","line","pie","doughnut"]
            ok(f"Sales Q: '{q[:40]}' → {result.chart['chart_type'] if result.chart else 'no chart'}")
        except AssertionError as e:
            err(f"Sales Q: '{q[:40]}'", e)
except Exception as e:
    err("Sales answerer (fatal)", e)

section("15. Answer.to_dict() serialisation")
try:
    df      = make_kri_df()
    profile = analyser.analyse(df, source_id="uk-kri")
    ans     = UniversalAnswerer(df, profile)
    result  = ans.answer("How many KRIs are there?")
    d       = result.to_dict()
    try:
        assert "reply" in d and "chart" in d and "confidence" in d
        ok("Answer serialises to dict correctly")
    except AssertionError as e: err("Dict serialisation", e)
    import json
    try:
        json.dumps(d)
        ok("Answer is JSON-serialisable")
    except Exception as e2:
        err("JSON serialisation", e2)
except Exception as e:
    err("Serialisation (fatal)", e)


# ─── FINAL REPORT ─────────────────────────────────────────────────────────
print("\n" + "="*60)
if _failed:
    print(f"RESULT: {FAIL} {len(_failed)} test(s) FAILED:")
    for f in _failed:
        print(f"  - {f}")
    sys.exit(1)
else:
    print(f"RESULT: {PASS} ALL TESTS PASSED")
    print("="*60)
