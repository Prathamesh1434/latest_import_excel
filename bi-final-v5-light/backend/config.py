"""
config.py — Centralised configuration loaded from .env

CHANGES (from Copilot session 5 Apr 2026):
  - TABLEAU_API_VERSION default changed 3.0 → 3.1
  - tiktoken removed; token counting uses len(text) // 4
"""
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

BASE_DIR     = Path(__file__).parent.parent
METADATA_DIR = BASE_DIR / "metadata"

# Vertex AI
GOOGLE_PROJECT_ID = os.getenv("GOOGLE_PROJECT_ID", "")
GOOGLE_LOCATION   = os.getenv("GOOGLE_LOCATION", "us-central1")
VERTEX_MODEL      = os.getenv("VERTEX_MODEL", "gemini-1.5-pro")

# Tableau  ← api_version default changed to 3.1 (Copilot fix #4)
TABLEAU_SERVER      = os.getenv("TABLEAU_SERVER", "")
TABLEAU_USERNAME    = os.getenv("TABLEAU_USERNAME", "")
TABLEAU_PASSWORD    = os.getenv("TABLEAU_PASSWORD", "")
TABLEAU_SITE        = os.getenv("TABLEAU_SITE", "")
TABLEAU_API_VERSION = os.getenv("TABLEAU_API_VERSION", "3.1")   # was "3.0"
TABLEAU_SSL_CERT    = os.getenv("TABLEAU_SSL_CERT_PATH", "")

# Oracle DB
ORACLE_USER     = os.getenv("ORACLE_USER", "")
ORACLE_PASSWORD = os.getenv("ORACLE_PASSWORD", "")
ORACLE_DSN      = os.getenv("ORACLE_DSN", "")
ORACLE_POOL_MIN = int(os.getenv("ORACLE_POOL_MIN", "2"))
ORACLE_POOL_MAX = int(os.getenv("ORACLE_POOL_MAX", "10"))

# App
MAX_TOKENS = int(os.getenv("MAX_TOKENS", "800"))
MAX_HISTORY = int(os.getenv("MAX_HISTORY_MESSAGES", "20"))
CACHE_TTL   = int(os.getenv("SNAPSHOT_CACHE_TTL", "300"))

# Token counting — NO tiktoken (Copilot fix #3: offline-friendly)
# 1 token ≈ 4 characters (rough estimate, no network calls)
def count_tokens(text: str) -> int:
    return len(text) // 4

def check_tableau(): return all([TABLEAU_SERVER, TABLEAU_USERNAME, TABLEAU_PASSWORD])
def check_vertex():  return bool(GOOGLE_PROJECT_ID)
def check_oracle():  return all([ORACLE_USER, ORACLE_PASSWORD, ORACLE_DSN])
