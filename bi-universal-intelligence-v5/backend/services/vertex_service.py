"""
services/vertex_service.py
Vertex AI Gemini — returns structured JSON response including chart data.
"""
import vertexai
from vertexai.generative_models import GenerativeModel, Content, Part
import time, logging, json, re
from typing import List, Dict, Tuple, Optional
from backend.config import GOOGLE_PROJECT_ID, GOOGLE_LOCATION, VERTEX_MODEL, MAX_TOKENS

log = logging.getLogger("vertex")
_init = False

def _ensure_init():
    global _init
    if not _init:
        if not GOOGLE_PROJECT_ID:
            raise RuntimeError("GOOGLE_PROJECT_ID not set in .env")
        vertexai.init(project=GOOGLE_PROJECT_ID, location=GOOGLE_LOCATION)
        _init = True


# ── Chart-aware system prompt suffix ─────────────────────────────────────────
CHART_INSTRUCTIONS = """

=== RESPONSE FORMAT ===
You MUST respond with valid JSON only. No markdown, no backticks, no extra text.
Schema:
{
  "reply": "2-3 sentence plain text summary for the user",
  "chart_type": "bar|line|pie|doughnut|kpi|table|text",
  "chart": {
    "title": "Chart title",
    "subtitle": "Period/context",
    ... chart-specific fields (see below)
  }
}

CHART TYPE SELECTION RULES:
- Use "kpi"       for: current values, status check, single metrics ("what is", "show value", "status")
- Use "bar"       for: comparisons, rankings, side-by-side ("compare", "vs", "by product", "breakdown")  
- Use "line"      for: trends, history, over time ("trend", "over months", "history", "last N months")
- Use "pie"       for: distribution, composition, share ("breakdown", "distribution", "how many")
- Use "doughnut"  for: pass/fail splits, percentage of whole
- Use "table"     for: detailed data, multiple attributes, lists ("show all", "list", "which ones")
- Use "text"      for: explanations, definitions, process questions

KPI CHART FIELDS:
"chart": {
  "title": "...", "subtitle": "...",
  "kpis": [
    {"label":"UK-K41","value":"8%","subtitle":"Red <70%","trend":"▼ -57.9%","trend_type":"down","color":"red"}
  ]
}
trend_type: "up"|"down"|"flat"
color: "red"|"amber"|"green"|"blue"|"na"

BAR/LINE CHART FIELDS:
"chart": {
  "title": "...", "subtitle": "...",
  "labels": ["Jan","Feb","Mar"],
  "datasets": [
    {"label":"Series name","data":[10,20,30],"color":"#1565C0","dashed":false}
  ]
}

PIE/DOUGHNUT CHART FIELDS:
"chart": {
  "title": "...", "subtitle": "...",
  "labels": ["Red","Amber","Green"],
  "data": [4,2,1],
  "colors": ["#C62828","#E65100","#2E7D32"]
}

TABLE CHART FIELDS:
"chart": {
  "title": "...", "subtitle": "...",
  "columns": ["KRI","Value","Status","Variance"],
  "rows": [
    ["UK-K41","8%","red","-57.9%"]
  ]
}
In table rows, use "red"/"amber"/"green"/"na" as status values — frontend will colour them.

TEXT (no chart):
"chart": null

IMPORTANT:
- Values stored as decimals (0.08) must be shown as percentages (8%) in replies and chart data
- Always include the reporting period in subtitle
- Keep reply brief (2-3 sentences max) — the chart shows the detail
- ONLY output valid JSON
"""


def chat(
    system_prompt: str,
    messages: List[Dict],
    max_tokens: int = MAX_TOKENS,
    retries: int = 2,
) -> Tuple[str, Optional[dict], int, int, int]:
    """
    Returns (reply_text, chart_dict_or_none, input_tokens, output_tokens, response_ms)
    """
    _ensure_init()
    if not messages:
        raise ValueError("No messages")

    full_system = system_prompt + CHART_INSTRUCTIONS
    last_err = None

    for attempt in range(retries + 1):
        t0 = time.time()
        try:
            model = GenerativeModel(
                model_name=VERTEX_MODEL,
                system_instruction=full_system,
            )
            history = [
                Content(role="user" if m["role"]=="user" else "model",
                        parts=[Part.from_text(m["content"])])
                for m in messages[:-1]
                if m["role"] in ("user","assistant")
            ]
            session  = model.start_chat(history=history)
            response = session.send_message(
                messages[-1]["content"],
                generation_config={
                    "max_output_tokens": max_tokens,
                    "temperature": 0.1,
                    "response_mime_type": "application/json",
                },
            )

            raw = response.text or "{}"
            # Strip any accidental markdown fences
            raw = re.sub(r"```json|```", "", raw).strip()

            data = json.loads(raw)
            reply      = data.get("reply", "No response generated.")
            chart_data = data.get("chart", None)
            resp_ms    = int((time.time()-t0)*1000)

            in_tok  = getattr(response.usage_metadata, "prompt_token_count",     0) or 0
            out_tok = getattr(response.usage_metadata, "candidates_token_count", 0) or 0

            log.info(f"Gemini OK: in={in_tok} out={out_tok} ms={resp_ms} chart={data.get('chart_type','text')}")
            return reply, chart_data, in_tok, out_tok, resp_ms

        except json.JSONDecodeError as e:
            log.warning(f"JSON parse failed attempt {attempt+1}: {e}. Raw: {raw[:200]}")
            # Return as plain text
            reply = raw[:500] if raw else "Could not parse response."
            return reply, None, 0, 0, int((time.time()-t0)*1000)

        except Exception as e:
            last_err = e
            if attempt < retries:
                time.sleep(2 ** attempt)
                log.warning(f"Gemini attempt {attempt+1} failed: {e}")

    raise last_err


def ping() -> Tuple[bool, int]:
    t0 = time.time()
    try:
        _ensure_init()
        model = GenerativeModel(model_name=VERTEX_MODEL)
        r = model.generate_content(
            'Reply with exactly: {"reply":"ok","chart_type":"text","chart":null}',
            generation_config={"max_output_tokens": 50, "response_mime_type": "application/json"}
        )
        return bool(r.text), int((time.time()-t0)*1000)
    except Exception as e:
        log.warning(f"Gemini ping failed: {e}")
        return False, int((time.time()-t0)*1000)
