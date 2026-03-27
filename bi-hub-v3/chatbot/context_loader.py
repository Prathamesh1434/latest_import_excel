"""
context_loader.py
Loads scorecard YAML metadata and builds a scoped system prompt
for the AI chatbot.

Packages used (already in prath env):
    PyYAML 6.0.3
"""

import yaml
from pathlib import Path

METADATA_DIR = Path(__file__).parent / "metadata"


def load_scorecard_context(scorecard_id: str) -> dict:
    """Load YAML for a given scorecard_id. Returns dict or None."""
    yaml_path = METADATA_DIR / f"{scorecard_id}.yaml"
    if not yaml_path.exists():
        return None
    with open(yaml_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def build_system_prompt(scorecard_id: str) -> str:
    """
    Build scoped system prompt from YAML metadata.
    Injected as system message on every AI API call.
    """
    context = load_scorecard_context(scorecard_id)

    if context is None:
        return (
            "You are a B&I Controls AI assistant. "
            "No scorecard context is loaded for this ID. "
            "Tell the user to check that the YAML metadata file exists."
        )

    scope_instruction = context.get("scope_instruction", "")

    context_text = yaml.dump(
        {k: v for k, v in context.items() if k != "scope_instruction"},
        default_flow_style=False,
        allow_unicode=True,
    )

    system_prompt = f"""{scope_instruction}

=== SCORECARD DATA ===
{context_text}
======================

RULES:
1. Only answer questions about this scorecard using the data above.
2. Never invent numbers not present in the data above.
3. Decimal values like 0.08 mean 8% — always convert to % in your answer.
4. If a question cannot be answered from the data, say so clearly.
5. Be concise. Use bullet points for multi-part answers.
6. Always state the period ({context.get('period', 'unknown')}) when quoting figures.
"""
    return system_prompt


def list_available_scorecards() -> list:
    """Return list of scorecard IDs that have YAML files."""
    return [f.stem for f in METADATA_DIR.glob("*.yaml") if not f.stem.startswith("_")]
