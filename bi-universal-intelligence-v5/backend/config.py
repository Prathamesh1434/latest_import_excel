"""
config.py — Centralised configuration + structured logging

CHANGES v5 (6 Apr 2026):
  - Added setup_logging() — structured format, forces uvicorn to use it
  - Added TIMEOUT constants for Tableau operations
  - Added diagnostic helpers for log-based debugging
  - Removed all Workstream2 / Control Codification references
"""
import os
import sys
import logging
import logging.config
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

BASE_DIR     = Path(__file__).parent.parent
METADATA_DIR = BASE_DIR / "metadata"

# ── Vertex AI ─────────────────────────────────────────────────────────
GOOGLE_PROJECT_ID = os.getenv("GOOGLE_PROJECT_ID", "")
GOOGLE_LOCATION   = os.getenv("GOOGLE_LOCATION", "us-central1")
VERTEX_MODEL      = os.getenv("VERTEX_MODEL", "gemini-1.5-pro")

# ── Tableau ───────────────────────────────────────────────────────────
TABLEAU_SERVER      = os.getenv("TABLEAU_SERVER", "")
TABLEAU_USERNAME    = os.getenv("TABLEAU_USERNAME", "")
TABLEAU_PASSWORD    = os.getenv("TABLEAU_PASSWORD", "")
TABLEAU_SITE        = os.getenv("TABLEAU_SITE", "")
TABLEAU_API_VERSION = os.getenv("TABLEAU_API_VERSION", "3.1")
TABLEAU_SSL_CERT    = os.getenv("TABLEAU_SSL_CERT_PATH", "")

# ── Tableau Timeouts (seconds) — prevents stuck extraction ────────────
TABLEAU_AUTH_TIMEOUT    = int(os.getenv("TABLEAU_AUTH_TIMEOUT", "30"))
TABLEAU_CSV_TIMEOUT     = int(os.getenv("TABLEAU_CSV_TIMEOUT", "120"))
TABLEAU_REST_TIMEOUT    = int(os.getenv("TABLEAU_REST_TIMEOUT", "60"))
TABLEAU_IMAGE_TIMEOUT   = int(os.getenv("TABLEAU_IMAGE_TIMEOUT", "45"))

# ── Oracle DB ─────────────────────────────────────────────────────────
ORACLE_USER     = os.getenv("ORACLE_USER", "")
ORACLE_PASSWORD = os.getenv("ORACLE_PASSWORD", "")
ORACLE_DSN      = os.getenv("ORACLE_DSN", "")
ORACLE_POOL_MIN = int(os.getenv("ORACLE_POOL_MIN", "2"))
ORACLE_POOL_MAX = int(os.getenv("ORACLE_POOL_MAX", "10"))

# ── App ───────────────────────────────────────────────────────────────
MAX_TOKENS  = int(os.getenv("MAX_TOKENS", "800"))
MAX_HISTORY = int(os.getenv("MAX_HISTORY_MESSAGES", "20"))
CACHE_TTL   = int(os.getenv("SNAPSHOT_CACHE_TTL", "300"))
LOG_LEVEL   = os.getenv("LOG_LEVEL", "INFO").upper()

# ── Token counting (offline — no tiktoken, no network calls) ──────────
def count_tokens(text: str) -> int:
    return len(text) // 4


# ── Health checks ─────────────────────────────────────────────────────
def check_tableau() -> bool:
    return all([TABLEAU_SERVER, TABLEAU_USERNAME, TABLEAU_PASSWORD])

def check_vertex() -> bool:
    return bool(GOOGLE_PROJECT_ID)

def check_oracle() -> bool:
    return all([ORACLE_USER, ORACLE_PASSWORD, ORACLE_DSN])


# ── Structured Logging Setup ─────────────────────────────────────────
# CRITICAL: Must be called BEFORE uvicorn starts logging,
# otherwise uvicorn's own config overrides everything.

LOG_FORMAT = (
    "%(asctime)s │ %(levelname)-5s │ %(name)-22s │ %(message)s"
)
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

def setup_logging(level: str = "") -> None:
    """
    Configure structured logging for the entire application.
    Call this in main.py BEFORE app creation.

    This forces uvicorn's loggers to use our format so that
    every log line — from FastAPI, Tableau, Vertex, or our code —
    appears in the same structured format in the terminal.
    """
    lvl = getattr(logging, (level or LOG_LEVEL), logging.INFO)

    # Root logger
    root = logging.getLogger()
    root.setLevel(lvl)

    # Clear existing handlers (uvicorn may have added some)
    root.handlers.clear()

    # Single stderr handler with structured format
    handler = logging.StreamHandler(sys.stderr)
    handler.setLevel(lvl)
    handler.setFormatter(logging.Formatter(LOG_FORMAT, datefmt=LOG_DATE_FORMAT))
    root.addHandler(handler)

    # Force uvicorn loggers to propagate to root (use OUR format)
    for name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        uv_log = logging.getLogger(name)
        uv_log.handlers.clear()
        uv_log.propagate = True

    # Set specific levels
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)  # reduce noise
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)

    logging.getLogger("config").info(
        f"Logging initialised: level={logging.getLevelName(lvl)}"
    )


def env_diagnostic() -> dict:
    """
    Returns a diagnostic dict showing which services are configured.
    Safe to log — never exposes passwords.

    USE: When Prathmesh sends a log snapshot, this block at the top
    tells us immediately what's configured and what's missing.
    """
    return {
        "tableau": {
            "configured": check_tableau(),
            "server":     TABLEAU_SERVER[:40] + "…" if len(TABLEAU_SERVER) > 40 else TABLEAU_SERVER,
            "user":       TABLEAU_USERNAME,
            "site":       TABLEAU_SITE or "(default)",
            "api_ver":    TABLEAU_API_VERSION,
            "ssl_cert":   bool(TABLEAU_SSL_CERT),
            "ssl_exists": os.path.exists(TABLEAU_SSL_CERT) if TABLEAU_SSL_CERT else False,
            "timeouts":   {
                "auth_s": TABLEAU_AUTH_TIMEOUT,
                "csv_s":  TABLEAU_CSV_TIMEOUT,
                "rest_s": TABLEAU_REST_TIMEOUT,
                "img_s":  TABLEAU_IMAGE_TIMEOUT,
            },
        },
        "vertex": {
            "configured": check_vertex(),
            "project":    GOOGLE_PROJECT_ID or "(not set)",
            "location":   GOOGLE_LOCATION,
            "model":      VERTEX_MODEL,
        },
        "oracle": {
            "configured": check_oracle(),
            "dsn":        ORACLE_DSN[:30] + "…" if len(ORACLE_DSN) > 30 else (ORACLE_DSN or "(not set)"),
            "user":       ORACLE_USER or "(not set)",
            "pool":       f"{ORACLE_POOL_MIN}–{ORACLE_POOL_MAX}",
        },
    }
