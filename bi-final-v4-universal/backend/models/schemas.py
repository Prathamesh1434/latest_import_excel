"""
models/schemas.py — All Pydantic request/response models
"""
from pydantic import BaseModel, Field
from typing import List, Optional, Any, Dict
from datetime import datetime


# ── Chat ──────────────────────────────────────────────────────────────────────

class ChatMessage(BaseModel):
    role:    str = Field(..., pattern="^(user|assistant)$")
    content: str = Field(..., min_length=1)


class ChatRequest(BaseModel):
    scorecard_id:   str              = Field(..., min_length=1)
    scorecard_name: str              = Field(default="")
    messages:       List[ChatMessage]= Field(..., min_length=1)
    session_id:     Optional[str]    = None
    user_id:        Optional[str]    = "ANONYMOUS"
    view_id:        Optional[str]    = ""    # Tableau view_id for CSV injection
    max_tokens:     Optional[int]    = Field(default=800, ge=50, le=2048)


class ChartDataset(BaseModel):
    label:  str
    data:   List[Any]
    color:  Optional[Any] = None
    dashed: Optional[bool] = False


class ChartData(BaseModel):
    """
    Structured chart data returned alongside AI reply.
    chart_type: bar | line | pie | doughnut | kpi | table | text
    """
    chart_type:  str                      = "text"
    title:       Optional[str]            = ""
    subtitle:    Optional[str]            = ""
    labels:      Optional[List[str]]      = None
    datasets:    Optional[List[Dict]]     = None
    kpis:        Optional[List[Dict]]     = None
    table_cols:  Optional[List[str]]      = None
    table_rows:  Optional[List[List[str]]]= None
    colors:      Optional[List[str]]      = None


class ChatResponse(BaseModel):
    reply:        str
    session_id:   str
    scorecard_id: str
    model:        str
    chart:        Optional[ChartData] = None
    input_tokens: Optional[int]       = None
    output_tokens:Optional[int]       = None
    response_ms:  Optional[int]       = None


# ── History ───────────────────────────────────────────────────────────────────

class SessionSummary(BaseModel):
    session_id:     str
    scorecard_id:   str
    scorecard_name: Optional[str]
    message_count:  int
    created_dt:     datetime
    last_active_dt: datetime


# ── Health ────────────────────────────────────────────────────────────────────

class ServiceHealth(BaseModel):
    name:       str
    status:     str
    latency_ms: Optional[int] = None
    message:    Optional[str] = None
