"""
chat_api.py
FastAPI backend — Tableau snapshots + Vertex AI chatbot.

Run:
    conda activate prath
    cd chatbot
    uvicorn chat_api:app --host 0.0.0.0 --port 8000 --reload

Packages used (already in prath env):
    fastapi           0.121.1
    uvicorn           0.34.0
    pydantic          2.12.3
    python-dotenv     1.2.1
    vertexai          1.43.0
    google-cloud-aiplatform  1.122.0
    tableauserverclient
    PyYAML            6.0.3
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response
from pydantic import BaseModel
from typing import List, Optional
import vertexai
from vertexai.generative_models import GenerativeModel, Content, Part
import os
from dotenv import load_dotenv
from context_loader import build_system_prompt, list_available_scorecards
from tableau_client import get_view_image_bytes, get_view_pdf_bytes

load_dotenv()

# ── Vertex AI ──────────────────────────────────────────────────────────────────
PROJECT_ID = os.getenv("GOOGLE_PROJECT_ID")
LOCATION   = os.getenv("GOOGLE_LOCATION", "us-central1")
MODEL_NAME = os.getenv("VERTEX_MODEL", "gemini-1.5-pro")

if PROJECT_ID:
    vertexai.init(project=PROJECT_ID, location=LOCATION)

# ── App ────────────────────────────────────────────────────────────────────────
app = FastAPI(title="B&I Controls Hub API", version="3.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["Content-Type"],
)


# ══════════════════════════════════════════
# SNAPSHOT ENDPOINTS
# ══════════════════════════════════════════

@app.get("/snapshot/{view_id}")
def snapshot_png(view_id: str):
    """Returns PNG snapshot of a Tableau view."""
    try:
        png_bytes = get_view_image_bytes(view_id)
        return Response(
            content=png_bytes,
            media_type="image/png",
            headers={"Cache-Control": "max-age=300"},
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Tableau snapshot error: {str(e)}")


@app.get("/snapshot/{view_id}/pdf")
def snapshot_pdf(view_id: str):
    """Returns PDF of a Tableau view."""
    try:
        pdf_bytes = get_view_pdf_bytes(view_id)
        return Response(
            content=pdf_bytes,
            media_type="application/pdf",
            headers={
                "Content-Disposition": f'attachment; filename="dashboard-{view_id}.pdf"',
                "Cache-Control": "max-age=300",
            },
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Tableau PDF error: {str(e)}")


# ══════════════════════════════════════════
# CHAT ENDPOINT — Vertex AI Gemini
# ══════════════════════════════════════════

class Message(BaseModel):
    role: str       # "user" or "assistant"
    content: str

class ChatRequest(BaseModel):
    scorecard_id: str
    messages: List[Message]
    max_tokens: Optional[int] = 500

class ChatResponse(BaseModel):
    reply: str
    scorecard_id: str
    model: str


@app.post("/chat", response_model=ChatResponse)
async def chat(req: ChatRequest):
    """AI chat scoped to a specific scorecard via YAML + Vertex AI Gemini."""
    if not req.messages:
        raise HTTPException(status_code=400, detail="No messages provided.")

    if not PROJECT_ID:
        raise HTTPException(
            status_code=500,
            detail="GOOGLE_PROJECT_ID not set in .env file."
        )

    system_prompt = build_system_prompt(req.scorecard_id)

    try:
        model = GenerativeModel(
            model_name=MODEL_NAME,
            system_instruction=system_prompt,
        )

        messages = [m for m in req.messages if m.role in ("user", "assistant")]

        # Build conversation history
        history = []
        for m in messages[:-1]:
            role = "user" if m.role == "user" else "model"
            history.append(Content(role=role, parts=[Part.from_text(m.content)]))

        chat_session = model.start_chat(history=history)

        response = chat_session.send_message(
            messages[-1].content,
            generation_config={"max_output_tokens": req.max_tokens},
        )

        reply = response.text if response.text else "No response generated."

        return ChatResponse(
            reply=reply,
            scorecard_id=req.scorecard_id,
            model=MODEL_NAME,
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Vertex AI error: {str(e)}")


# ══════════════════════════════════════════
# UTILITY
# ══════════════════════════════════════════

@app.get("/scorecards")
def get_scorecards():
    return {"scorecards": list_available_scorecards()}


@app.get("/health")
def health():
    return {
        "status":   "ok",
        "model":    MODEL_NAME,
        "project":  PROJECT_ID,
        "location": LOCATION,
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("chat_api:app", host="0.0.0.0", port=8000, reload=True)
