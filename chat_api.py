"""
chat_api.py
FastAPI backend — chat + Tableau snapshot endpoints.

Run:
    conda activate prath
    cd bi-controls-hub-v2/chatbot
    uvicorn chat_api:app --host 0.0.0.0 --port 8000 --reload

Endpoints:
    GET  /health                        → health check
    GET  /snapshot/{view_id}            → PNG image of Tableau view
    GET  /snapshot/{view_id}/pdf        → PDF of Tableau view
    POST /chat                          → AI chat scoped to scorecard
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response
from pydantic import BaseModel
from typing import List, Optional
import anthropic
import os
from dotenv import load_dotenv
from context_loader import build_system_prompt, list_available_scorecards
from tableau_client import get_view_image, get_view_pdf, sign_in

load_dotenv()

# ── App ────────────────────────────────────────────────────────────────────────
app = FastAPI(title="B&I Controls Hub API", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],        # tighten to your domain in production
    allow_methods=["GET", "POST"],
    allow_headers=["Content-Type"],
)

# ── Clients ────────────────────────────────────────────────────────────────────
anthropic_client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
MODEL = "claude-sonnet-4-6"


# ══════════════════════════════════════════════════════════════════════════════
# SNAPSHOT ENDPOINTS
# ══════════════════════════════════════════════════════════════════════════════

@app.get("/snapshot/{view_id}")
def snapshot_png(view_id: str, resolution: int = 1920):
    """
    Returns a PNG snapshot of a Tableau view.
    Called by the frontend to display the dashboard image.

    Args:
        view_id:    Tableau view ID (from scorecards.js)
        resolution: image width in pixels (default 1920)
    """
    try:
        png_bytes = get_view_image(view_id, resolution=resolution)
        return Response(
            content=png_bytes,
            media_type="image/png",
            headers={"Cache-Control": "max-age=300"},  # cache 5 min
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Tableau snapshot error: {str(e)}")


@app.get("/snapshot/{view_id}/pdf")
def snapshot_pdf(
    view_id: str,
    page_type: str = "A4",
    orientation: str = "Landscape"
):
    """
    Returns a PDF snapshot of a Tableau view.

    Args:
        view_id:     Tableau view ID
        page_type:   A4 / Letter / Legal
        orientation: Landscape / Portrait
    """
    try:
        pdf_bytes = get_view_pdf(view_id, page_type=page_type, orientation=orientation)
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


# ══════════════════════════════════════════════════════════════════════════════
# CHAT ENDPOINT
# ══════════════════════════════════════════════════════════════════════════════

class Message(BaseModel):
    role: str
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
    """AI chat scoped to a specific scorecard via YAML metadata."""
    if not req.messages:
        raise HTTPException(status_code=400, detail="No messages provided.")

    system_prompt = build_system_prompt(req.scorecard_id)

    messages = [
        {"role": m.role, "content": m.content}
        for m in req.messages
        if m.role in ("user", "assistant")
    ]

    try:
        response = anthropic_client.messages.create(
            model=MODEL,
            max_tokens=req.max_tokens,
            system=system_prompt,
            messages=messages,
        )
        reply = response.content[0].text if response.content else "No response generated."
        return ChatResponse(reply=reply, scorecard_id=req.scorecard_id, model=MODEL)

    except anthropic.AuthenticationError:
        raise HTTPException(status_code=401, detail="Invalid Anthropic API key.")
    except anthropic.RateLimitError:
        raise HTTPException(status_code=429, detail="Rate limit reached. Try again shortly.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"LLM error: {str(e)}")


# ══════════════════════════════════════════════════════════════════════════════
# UTILITY
# ══════════════════════════════════════════════════════════════════════════════

@app.get("/scorecards")
def get_scorecards():
    return {"scorecards": list_available_scorecards()}


@app.get("/health")
def health():
    return {"status": "ok", "model": MODEL}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("chat_api:app", host="0.0.0.0", port=8000, reload=True)
