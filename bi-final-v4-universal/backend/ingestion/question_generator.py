"""
ingestion/question_generator.py

Generates relevant, data-grounded sample questions from any SchemaProfile.
Zero hardcoding — adapts to every dashboard automatically.

Output is used for:
  - Chatbot suggestion chips
  - Onboarding new dashboards
  - Validating that the system understands the data
"""

from __future__ import annotations

import random
import logging
from typing import List, Dict
from backend.ingestion.schema_analyser import SchemaProfile, KPIPattern

log = logging.getLogger("question_generator")


class DynamicQuestionGenerator:
    """
    Generates tailored questions from a SchemaProfile.

    Usage:
        profile   = analyser.analyse(df, ...)
        generator = DynamicQuestionGenerator()
        questions = generator.generate(profile, max_q=8)
        # Returns list of strings — ready for chatbot chips
    """

    def generate(
        self,
        profile:  SchemaProfile,
        max_q:    int  = 8,
        shuffle:  bool = True,
    ) -> List[str]:
        """
        Generate up to max_q relevant questions.
        Questions are ordered by expected user interest:
          1. Breach/alert questions (highest value)
          2. Status/RAG questions
          3. Trend/time-series questions
          4. Top/bottom ranking questions
          5. Comparison/dimension questions
          6. Variance questions
          7. Generic exploration questions
        """
        all_questions: List[str] = []

        all_questions += self._breach_questions(profile)
        all_questions += self._rag_questions(profile)
        all_questions += self._trend_questions(profile)
        all_questions += self._ranking_questions(profile)
        all_questions += self._comparison_questions(profile)
        all_questions += self._variance_questions(profile)
        all_questions += self._exploration_questions(profile)

        # Deduplicate
        seen = set()
        unique = []
        for q in all_questions:
            key = q.lower().strip()
            if key not in seen:
                seen.add(key)
                unique.append(q)

        if shuffle:
            # Keep breach/alert at top, shuffle the rest
            priority = [q for q in unique if any(w in q.lower() for w in ["breach","fail","red","alert","exceed"])]
            rest     = [q for q in unique if q not in priority]
            random.shuffle(rest)
            unique = priority + rest

        result = unique[:max_q]
        log.info(f"Generated {len(result)} questions for {profile.source_id} ({profile.dashboard_type})")
        return result

    # ── Question generators ────────────────────────────────────────────────

    def _breach_questions(self, p: SchemaProfile) -> List[str]:
        qs = []
        breaching = [kp for kp in p.kpi_patterns if kp.breach_detected]

        if breaching:
            qs.append(f"Which {self._kpi_term(p)} are currently breaching their threshold?")
            qs.append(f"How many {self._kpi_term(p)} have exceeded their limits?")

        if p.has_thresholds:
            val  = p.kpi_patterns[0].value_col if p.kpi_patterns else "value"
            thr  = p.kpi_patterns[0].threshold_col if p.kpi_patterns and p.kpi_patterns[0].threshold_col else "threshold"
            qs.append(f"Which records have {val} below {thr}?")

        if p.has_rag_status:
            stat = p.status_cols[0]
            qs.append(f"Show all items where {stat} is Red.")
            qs.append(f"How many items are in each {stat} category?")

        return qs

    def _rag_questions(self, p: SchemaProfile) -> List[str]:
        qs = []
        if not p.status_cols:
            return qs

        stat = p.status_cols[0]
        dim  = p.dimensions[0] if p.dimensions else None

        qs.append(f"What is the overall {stat} distribution?")
        if dim:
            qs.append(f"Show the {stat} status for each {dim}.")
            qs.append(f"Which {dim} has the most Red / Fail statuses?")

        if p.has_rag_status and p.has_time_series:
            tc = p.time_cols[0]
            qs.append(f"Has the {stat} improved over {tc}?")

        return qs

    def _trend_questions(self, p: SchemaProfile) -> List[str]:
        qs = []
        if not p.has_time_series or not p.measures:
            return qs

        tc  = p.time_cols[0]
        m   = p.measures[0]
        dim = p.dimensions[0] if p.dimensions else None

        qs.append(f"Show the trend of {m} over {tc}.")
        if p.kpi_patterns:
            kp = p.kpi_patterns[0]
            qs.append(f"Plot {kp.kpi_name} over time.")

        if dim:
            qs.append(f"How has {m} changed by {dim} over {tc}?")

        if len(p.time_cols) > 0 and len(p.measures) > 1:
            m2 = p.measures[1]
            qs.append(f"Compare {m} and {m2} trends over {tc}.")

        return qs

    def _ranking_questions(self, p: SchemaProfile) -> List[str]:
        qs = []
        if not p.measures:
            return qs

        m   = p.measures[0]
        dim = p.dimensions[0] if p.dimensions else None

        if dim:
            qs.append(f"Which {dim} has the highest {m}?")
            qs.append(f"Which {dim} is performing the worst on {m}?")
            qs.append(f"Rank all {dim} values by {m}.")

        if p.kpi_patterns:
            kp = p.kpi_patterns[0]
            qs.append(f"What is the highest value of {kp.kpi_name}?")
            qs.append(f"Show the top 5 {kp.kpi_name} values.")

        return qs

    def _comparison_questions(self, p: SchemaProfile) -> List[str]:
        qs = []

        if len(p.dimensions) >= 2:
            d1, d2 = p.dimensions[0], p.dimensions[1]
            m      = p.measures[0] if p.measures else "value"
            qs.append(f"Compare {m} by {d1} and {d2}.")
            qs.append(f"What is the breakdown of {m} across {d1}?")

        elif p.dimensions:
            d = p.dimensions[0]
            m = p.measures[0] if p.measures else "value"
            qs.append(f"What is the average {m} for each {d}?")
            qs.append(f"Show {m} grouped by {d}.")

        if p.hierarchies:
            top, child = p.hierarchies[0][0], p.hierarchies[0][1]
            qs.append(f"Show {child} breakdown within each {top}.")

        return qs

    def _variance_questions(self, p: SchemaProfile) -> List[str]:
        qs = []
        if not p.has_variance:
            return qs

        var_kpis = [kp for kp in p.kpi_patterns if kp.previous_col]
        for kp in var_kpis[:2]:
            qs.append(f"What is the change in {kp.kpi_name} from the previous period?")
            qs.append(f"Which {kp.kpi_name} had the largest variance?")

        if var_kpis:
            qs.append(f"Show all items with variance greater than 10%.")

        return qs

    def _exploration_questions(self, p: SchemaProfile) -> List[str]:
        """Generic exploration — always applicable."""
        qs = []
        term = self._kpi_term(p)

        qs.append(f"Give me a summary of this dashboard.")
        qs.append(f"How many {term} are there in total?")

        if p.measures:
            m = p.measures[0]
            qs.append(f"What is the average {m}?")
            qs.append(f"What is the total {m}?")

        if p.dimensions:
            d = p.dimensions[0]
            qs.append(f"List all unique {d} values.")

        if p.has_rag_status:
            qs.append("Show me all items that need attention.")
            qs.append("What percentage of items are compliant?")

        if p.dashboard_type == "kri":
            qs += [
                "Which KRIs are in breach?",
                "Show the KRI dashboard summary.",
                "Are there any chronic Red KRIs?",
            ]
        elif p.dashboard_type == "compliance":
            qs += [
                "Are there any control failures?",
                "Which controls have not been tested recently?",
                "Show all open breaches.",
            ]
        elif p.dashboard_type == "sales":
            qs += [
                "Which region is underperforming?",
                "What is the total revenue this quarter?",
                "Show pipeline by stage.",
            ]
        elif p.dashboard_type == "scorecard":
            qs += [
                "Which metrics are below target?",
                "Show the overall scorecard summary.",
                "What is the performance trend?",
            ]

        return qs

    # ── Helper ─────────────────────────────────────────────────────────────

    def _kpi_term(self, p: SchemaProfile) -> str:
        """Infer the right noun for KPIs in this dashboard."""
        terms = {
            "kri":        "KRIs",
            "scorecard":  "metrics",
            "compliance": "controls",
            "sales":      "deals",
            "ops":        "incidents",
            "finance":    "line items",
        }
        return terms.get(p.dashboard_type, "records")

    def generate_as_chips(self, profile: SchemaProfile, max_q: int = 6) -> List[Dict]:
        """
        Returns questions formatted as UI chip objects.
        Frontend can render these directly as clickable suggestion buttons.
        """
        questions = self.generate(profile, max_q=max_q)
        return [
            {"text": q, "source_id": profile.source_id}
            for q in questions
        ]
