"""
intelligence/query_engine.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Tiered Query Engine — routes every question to the right tier:

  Tier 1 (DIRECT)  → Pure pandas on the DataFrame
                     Handles: exact filters, counts, rankings, aggregations
                     Confidence: 0.85–1.0  |  Latency: <50ms  |  Cost: $0

  Tier 2 (SEMANTIC) → TF-IDF semantic search on the indexed facts
                     Handles: fuzzy NL, concept matching, "which X is best/worst"
                     Confidence: 0.6–0.85  |  Latency: <100ms  |  Cost: $0

  Tier 3 (LLM)     → Vertex AI Gemini with structured context
                     Handles: complex reasoning, narratives, cross-entity
                     Confidence: 0.9 (but ~$0.002/call)  |  Latency: 1–3s

Routing logic:
  - Intent classifier → Tier 1 if matched
  - Semantic search score > 0.3 → Tier 2
  - Otherwise → Tier 3 (with Tier 2 results as additional context)
"""
from __future__ import annotations

import re
import logging
import time as _time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from backend.ingestion.universal_answerer import UniversalAnswerer, Answer
from backend.ingestion.schema_analyser    import SchemaAnalyser
from backend.intelligence.semantic_layer  import SemanticLayer, SemanticProfile
from backend.intelligence.dashboard_schema import UniversalDashboardSchema

log = logging.getLogger("query_engine")

# ──────────────────────────────────────────────────────────────
#  RESPONSE MODEL
# ──────────────────────────────────────────────────────────────

@dataclass
class QueryResponse:
    """Unified response from any tier."""
    reply:        str
    tier:         int             # 1 | 2 | 3
    tier_label:   str             # DIRECT | SEMANTIC | LLM
    confidence:   float
    chart:        Optional[Dict]
    row_count:    int
    latency_ms:   int
    source_refs:  List[str]       # which data facts were used
    escalated:    bool = False    # True if Tier 1 escalated to Tier 2 or 3
    data:         Optional[Any] = None
    # Attribution for audit trail
    data_source:  str = ""        # e.g. "Extracted from Tableau on 2026-04-05T14:32"
    model_used:   str = ""        # e.g. "pandas" | "tfidf" | "gemini-1.5-pro"

    def to_dict(self) -> Dict:
        import math
        
        # This safely purges rogue NaN math values from Pandas
        def sanitize_for_json(obj):
            if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
                return None
            elif isinstance(obj, list):
                return [sanitize_for_json(x) for x in obj]
            elif isinstance(obj, dict):
                return {k: sanitize_for_json(v) for k, v in obj.items()}
            return obj

        data_out = None
        if isinstance(self.data, pd.DataFrame):
            data_out = self.data.head(100).fillna("").to_dict("records")
        elif isinstance(self.data, pd.Series):
            data_out = self.data.head(100).fillna("").tolist()
        elif hasattr(self.data, "item"): 
            data_out = self.data.item()
        elif self.data is not None:
             try:
                 data_out = list(self.data) if hasattr(self.data, '__iter__') and not isinstance(self.data, (str, dict)) else self.data
             except:
                 data_out = str(self.data)

        return {
            "reply":       self.reply,
            "tier":        self.tier,
            "tier_label":  self.tier_label,
            "confidence":  self.confidence,
            "chart":       self.chart,
            "row_count":   self.row_count,
            "latency_ms":  self.latency_ms,
            "source_refs": self.source_refs,
            "escalated":   self.escalated,
            "data":        sanitize_for_json(data_out),
            "data_source": self.data_source,
            "model_used":  self.model_used,
        }


# ──────────────────────────────────────────────────────────────
#  TIER 1 — INTENT CLASSIFIER
# ──────────────────────────────────────────────────────────────

_DIRECT_INTENTS = [
    # Pattern → handler tag
    (r"\b(breach|exceed|fail|violat|out of.*threshold|below.*threshold|above.*threshold)\b", "breach"),
    (r"\b(red|amber|green|rag|status|failing|passing|compliant|non.compliant)\b",            "rag"),
    (r"\b(trend|over time|monthly|period|history|last \d+|change over)\b",                   "trend"),
    (r"\b(highest|best|top|maximum|max|most|largest)\b",                                      "highest"),
    (r"\b(lowest|worst|bottom|minimum|min|least|smallest)\b",                                 "lowest"),
    (r"\b(average|avg|mean|typical)\b",                                                       "average"),
    (r"\b(total|sum|count|how many|number of|# of)\b",                                       "count"),
    (r"\b(variance|change|delta|difference|shift|moved|improved|deteriorated)\b",             "variance"),
    (r"\b(distribution|breakdown|split|proportion|share|percentage|by each)\b",              "distribution"),
    (r"\b(compare|vs|versus|contrast|against|side by side)\b",                               "compare"),
    (r"\b(rank|ranking|list all|show all|top \d+|bottom \d+)\b",                             "ranking"),
    (r"\b(outlier|anomaly|unusual|spike|drop|unexpected)\b",                                  "outliers"),
    (r"\b(correlation|relationship between|link between)\b",                                  "correlation"),
    (r"\b(summary|overview|give me|describe|what is this)\b",                                 "summary"),
]


def _detect_intent(question: str) -> Optional[str]:
    q = question.lower()
    for pattern, intent in _DIRECT_INTENTS:
        if re.search(pattern, q):
            return intent
    return None


# ──────────────────────────────────────────────────────────────
#  QUERY ENGINE
# ──────────────────────────────────────────────────────────────

class QueryEngine:
    """
    Unified query engine for any connected dashboard.

    Usage:
        engine = QueryEngine()
        resp   = engine.query(
            question  = "Which region is underperforming?",
            df        = df,
            schema    = universal_schema,
            sem       = semantic_profile,
        )
        print(resp.reply)
        print(resp.tier_label)
    """

    def __init__(
        self,
        tier1_threshold: float = 0.70,   # min confidence to stop at Tier 1
        tier2_threshold: float = 0.30,   # min semantic score to use Tier 2
        use_llm:         bool  = True,   # allow Tier 3 escalation
    ):
        self.t1_thr = tier1_threshold
        self.t2_thr = tier2_threshold
        self.use_llm = use_llm
        self._analyser = SchemaAnalyser()

    def query(
        self,
        question: str,
        df:       pd.DataFrame,
        schema:   Optional[UniversalDashboardSchema] = None,
        sem:      Optional[SemanticProfile]          = None,
    ) -> QueryResponse:
        """
        Route question through tiers. Returns first tier that answers with
        confidence above threshold.
        """
        t0      = _time.time()
        q_lower = question.lower().strip()
        source  = f"Extracted from Tableau on {schema.extracted_at}" if schema else ""

        # ── TIER 1: Direct pandas ──────────────────────────────────────
        tier1 = self._tier1(question, df)
        if tier1 and tier1.confidence >= self.t1_thr:
            return QueryResponse(
                reply       = tier1.reply,
                tier        = 1,
                tier_label  = "DIRECT",
                confidence  = tier1.confidence,
                chart       = tier1.chart,
                row_count   = tier1.row_count,
                latency_ms  = int((_time.time() - t0) * 1000),
                source_refs = ["pandas computation on extracted DataFrame"],
                data        = tier1.data,
                data_source = source,
                model_used  = "pandas",
            )

        # ── TIER 2: Semantic search ────────────────────────────────────
        tier2_results = []
        tier2_reply   = ""
        if sem and sem.semantic_index is not None:
            tier2_results = sem.search(question, top_k=6)
            if tier2_results and tier2_results[0].get("score", 0) >= self.t2_thr:
                tier2_reply = self._format_semantic(question, tier2_results, schema)
                if tier2_reply:
                    return QueryResponse(
                        reply       = tier2_reply,
                        tier        = 2,
                        tier_label  = "SEMANTIC",
                        confidence  = min(0.85, tier2_results[0]["score"] + 0.2),
                        chart       = self._semantic_chart(tier2_results, schema),
                        row_count   = len(tier2_results),
                        latency_ms  = int((_time.time() - t0) * 1000),
                        source_refs = [f"Semantic match: {r.get('type','?')} [{r.get('score',0):.2f}]"
                                       for r in tier2_results[:3]],
                        escalated   = tier1 is not None,
                        data_source = source,
                        model_used  = "tfidf-semantic",
                    )

        # ── TIER 3: LLM ───────────────────────────────────────────────
        if self.use_llm:
            tier3 = self._tier3(question, df, schema, sem, tier1, tier2_results)
            if tier3:
                return tier3

        # ── Fallback: best we have ─────────────────────────────────────
        if tier1:
            reply = tier1.reply
        elif tier2_reply:
            reply = tier2_reply
        else:
            reply = (
                f"I couldn't find a specific answer for '{question}'. "
                f"Try asking about: {', '.join((schema.questions or [])[:3]) if schema else 'the dashboard data'}."
            )

        return QueryResponse(
            reply       = reply,
            tier        = 0,
            tier_label  = "FALLBACK",
            confidence  = 0.3,
            chart       = None,
            row_count   = 0,
            latency_ms  = int((_time.time() - t0) * 1000),
            source_refs = [],
            data_source = source,
            model_used  = "fallback",
        )

    # ──────────────────────────────────────────────────────────
    #  TIER 1
    # ──────────────────────────────────────────────────────────

    def _tier1(self, question: str, df: pd.DataFrame) -> Optional[Answer]:
        if df is None or df.empty:
            return None
        try:
            profile  = self._analyser.analyse(df)
            answerer = UniversalAnswerer(df, profile)
            return answerer.answer(question)
        except Exception as e:
            log.debug(f"Tier 1 failed: {e}")
            return None

    # ──────────────────────────────────────────────────────────
    #  TIER 2
    # ──────────────────────────────────────────────────────────

    def _format_semantic(
        self, question: str, results: List[Dict],
        schema: Optional[UniversalDashboardSchema]
    ) -> str:
        if not results:
            return ""

        q_lo = question.lower()
        parts = []

        # Group by type
        fact_results   = [r for r in results if r.get("type") == "fact"]
        status_results = [r for r in results if r.get("type") == "status"]
        trend_results  = [r for r in results if r.get("type") == "trend"]
        top_results    = [r for r in results if r.get("type") == "top_row"]

        if any(w in q_lo for w in ["underperform","worst","lowest","least","bottom","poor"]):
            if fact_results:
                fact_results.sort(key=lambda x: x.get("value", float("inf")))
                r = fact_results[0]
                parts.append(
                    f"Based on {r['dim']} analysis: {r['dim_val']} has the lowest "
                    f"{r['measure']} ({r['value']})."
                )
        elif any(w in q_lo for w in ["best","highest","top","most","outperform"]):
            if fact_results:
                fact_results.sort(key=lambda x: x.get("value", 0), reverse=True)
                r = fact_results[0]
                parts.append(
                    f"Based on {r['dim']} analysis: {r['dim_val']} has the highest "
                    f"{r['measure']} ({r['value']})."
                )
        elif any(w in q_lo for w in ["trend","over time","change","direction"]):
            if trend_results:
                r = trend_results[0]
                parts.append(
                    f"{r['measure']} shows a {r['direction']} trend "
                    f"({r['pct_change']:+.1f}% change)."
                )
        elif status_results:
            r = status_results[0]
            parts.append(
                f"{r['col']} status distribution: {r['status']} accounts for "
                f"{r['count']} items ({r['pct']}%)."
            )

        # Add supporting facts
        if fact_results and len(parts) > 0:
            supporting = fact_results[:3]
            parts.append(
                "Supporting data: " +
                ", ".join(f"{r['dim_val']}={r['value']}" for r in supporting)
            )

        # Add semantic KPI context if schema available
        if schema and schema.kpis and any(w in q_lo for w in ["kpi","performance","metric","indicator"]):
            breaching = [k for k in schema.kpis if k.get("status") == "breaching"]
            if breaching:
                parts.append(
                    f"Currently breaching: {', '.join(k['name'] for k in breaching[:3])}"
                )

        return " ".join(parts) if parts else ""

    def _semantic_chart(
        self, results: List[Dict],
        schema: Optional[UniversalDashboardSchema]
    ) -> Optional[Dict]:
        """Build a chart from semantic search results."""
        fact_results = [r for r in results if r.get("type") == "fact" and r.get("value") is not None]
        if len(fact_results) >= 2:
            labels = [r.get("dim_val", "?") for r in fact_results[:8]]
            values = [r.get("value", 0) for r in fact_results[:8]]
            return {
                "chart_type": "bar",
                "title":      f"{fact_results[0].get('measure', '')} by {fact_results[0].get('dim', '')}",
                "subtitle":   schema.source_name if schema else "",
                "labels":     [str(l) for l in labels],
                "datasets":   [{"label": fact_results[0].get("measure",""), "data": values, "color": "#0057A8"}],
            }
        return None

    # ──────────────────────────────────────────────────────────
    #  TIER 3 — LLM
    # ──────────────────────────────────────────────────────────

    def _tier3(
        self,
        question:     str,
        df:           pd.DataFrame,
        schema:       Optional[UniversalDashboardSchema],
        sem:          Optional[SemanticProfile],
        tier1_result: Optional[Answer],
        sem_results:  List[Dict],
    ) -> Optional[QueryResponse]:
        try:
            from backend.services import vertex_service
            from backend.config    import check_vertex

            if not check_vertex():
                return None

            t0 = _time.time()

            # Build rich context
            context_parts = []

            if schema:
                context_parts.append(schema.to_llm_context(max_chars=5000))

            # Add semantic profile context
            if sem:
                context_parts.append(sem.get_llm_context(max_chars=2000))

            # Add Tier 1 partial answer if exists
            if tier1_result and tier1_result.confidence > 0.2:
                context_parts.append(
                    f"\nPARTIAL PANDAS ANSWER (confidence {tier1_result.confidence:.0%}):\n"
                    f"{tier1_result.reply}"
                )

            # Add top semantic hits
            if sem_results:
                context_parts.append("\nSEMANTIC CONTEXT:")
                for r in sem_results[:4]:
                    if r.get("type") == "fact":
                        context_parts.append(f"  {r['dim_val']}: {r['measure']}={r['value']}")
                    elif r.get("type") == "trend":
                        context_parts.append(f"  Trend: {r['measure']} is {r['direction']}")

            system_prompt = (
                "You are an intelligent data analyst for a Tableau dashboard. "
                "Answer the user's question using ONLY the data context provided. "
                "Be concise and specific. Cite actual values from the data. "
                "Do not speculate beyond what the data shows. "
                "Return JSON: "
                '{"reply":"<2-3 sentence answer with specific data values>", '
                '"chart_type":"bar|line|pie|kpi|table|scorecard|text", '
                '"chart":{...chart data if applicable or null}}'
                "\n\n"
                "DATA CONTEXT:\n" + "\n\n".join(context_parts)
            )

            reply, chart, in_tok, out_tok, resp_ms = vertex_service.chat(
                system_prompt = system_prompt,
                messages      = [{"role": "user", "content": question}],
                max_tokens    = 500,
            )

            source = f"Extracted from Tableau on {schema.extracted_at}" if schema else ""
            return QueryResponse(
                reply       = reply,
                tier        = 3,
                tier_label  = "LLM",
                confidence  = 0.90,
                chart       = chart,
                row_count   = 0,
                latency_ms  = int((_time.time() - t0) * 1000),
                source_refs = [f"Gemini 1.5 Pro with {in_tok} input tokens"],
                escalated   = True,
                data_source = source,
                model_used  = "gemini-1.5-pro",
            )

        except Exception as e:
            log.error(f"Tier 3 LLM failed: {e}")
            return None


# ──────────────────────────────────────────────────────────────
#  CONVENIENCE: build engine from connected dashboard
# ──────────────────────────────────────────────────────────────

def create_engine_for(source_id: str, cache: Dict) -> Optional[QueryEngine]:
    """Get a ready QueryEngine for a connected dashboard."""
    entry = cache.get(source_id)
    if not entry:
        return None
    return QueryEngine()
