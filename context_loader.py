"""
context_loader.py
Loads scorecard metadata from YAML and builds a scoped system prompt.
No DB needed — purely file-based.
"""

import yaml
import json
from pathlib import Path

METADATA_DIR = Path(__file__).parent / "metadata"


def load_scorecard_context(scorecard_id: str) -> dict:
    """
    Load YAML for a given scorecard_id.
    Returns the parsed dict or None if file not found.
    """
    yaml_path = METADATA_DIR / f"{scorecard_id}.yaml"

    if not yaml_path.exists():
        return None

    with open(yaml_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def build_system_prompt(scorecard_id: str) -> str:
    """
    Build a scoped system prompt from the scorecard YAML.
    This is injected as the system message on every API call.
    """
    context = load_scorecard_context(scorecard_id)

    if context is None:
        return (
            "You are a B&I Controls AI assistant. "
            "No specific scorecard context is loaded. "
            "Ask the user to select a scorecard from the hub."
        )

    # Pull scope instruction from YAML (written per scorecard)
    scope_instruction = context.get("scope_instruction", "")

    # Serialise the full context as readable text for the LLM
    context_text = yaml.dump(
        {k: v for k, v in context.items() if k != "scope_instruction"},
        default_flow_style=False,
        allow_unicode=True,
    )

    system_prompt = f"""{scope_instruction}

=== SCORECARD DATA (use this to answer questions) ===
{context_text}
=====================================================

RULES YOU MUST FOLLOW:
1. Only answer questions about this scorecard using the data above.
2. Never invent numbers not present in the data above.
3. When referencing values stored as decimals (e.g. 0.08), convert to % (8%) in your response.
4. If a question cannot be answered from the data above, say:
   "That information is not available in the current scorecard context."
5. Be concise. Use bullet points for multi-part answers.
6. Always state the period ({context.get('period', 'unknown period')}) when quoting figures.
"""
    return system_prompt


def list_available_scorecards() -> list[str]:
    """Return list of scorecard IDs that have metadata files."""
    return [f.stem for f in METADATA_DIR.glob("*.yaml") if not f.stem.startswith("_")]
