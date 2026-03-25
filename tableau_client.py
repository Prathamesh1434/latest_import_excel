"""
tableau_client.py
Handles Tableau REST API authentication and snapshot fetching.

Packages used (already in prath env):
    requests==2.32.5
    python-dotenv==1.2.1
"""

import requests
import os
from dotenv import load_dotenv

load_dotenv()

# ── Config from .env ───────────────────────────────────────────────────────────
TABLEAU_SERVER   = os.getenv("TABLEAU_SERVER")        # e.g. https://tableau.company.com
TABLEAU_USERNAME = os.getenv("TABLEAU_USERNAME")
TABLEAU_PASSWORD = os.getenv("TABLEAU_PASSWORD")
TABLEAU_SITE     = os.getenv("TABLEAU_SITE", "")      # blank string = Default site
TABLEAU_SSL_CERT = os.getenv("TABLEAU_SSL_CERT_PATH", True)  # path or True/False
TABLEAU_API_VER  = os.getenv("TABLEAU_API_VERSION", "3.18")  # check your server version

# ── Internal session state ─────────────────────────────────────────────────────
_token   = None   # auth token, reused until it expires
_site_id = None   # site ID returned at sign-in


def _base_url() -> str:
    return f"{TABLEAU_SERVER}/api/{TABLEAU_API_VER}"


def _ssl() -> str | bool:
    """Returns cert path string or True (verify with system certs) or False (skip verify)."""
    if TABLEAU_SSL_CERT and TABLEAU_SSL_CERT not in ("True", "False", "true", "false"):
        return TABLEAU_SSL_CERT   # it's a file path
    return TABLEAU_SSL_CERT not in ("False", "false")


def sign_in() -> str:
    """
    Sign in to Tableau Server using username + password.
    Returns the auth token. Stores token + site_id globally for reuse.
    """
    global _token, _site_id

    url = f"{_base_url()}/auth/signin"

    payload = {
        "credentials": {
            "name":     TABLEAU_USERNAME,
            "password": TABLEAU_PASSWORD,
            "site":     {"contentUrl": TABLEAU_SITE}
        }
    }

    resp = requests.post(
        url,
        json=payload,
        verify=_ssl(),
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        timeout=30,
    )
    resp.raise_for_status()

    data     = resp.json()
    _token   = data["credentials"]["token"]
    _site_id = data["credentials"]["site"]["id"]

    return _token


def sign_out() -> None:
    """Sign out of Tableau Server and clear the token."""
    global _token, _site_id

    if not _token:
        return

    requests.post(
        f"{_base_url()}/auth/signout",
        headers={"x-tableau-auth": _token},
        verify=_ssl(),
        timeout=10,
    )

    _token   = None
    _site_id = None


def _get_token() -> str:
    """Return current token, signing in first if needed."""
    global _token
    if not _token:
        sign_in()
    return _token


def get_view_image(view_id: str, resolution: int = 1920) -> bytes:
    """
    Fetch a PNG snapshot of a Tableau view by view ID.

    Args:
        view_id:    Tableau view ID (from your scorecard config)
        resolution: image width in pixels (default 1920)

    Returns:
        PNG image bytes
    """
    token = _get_token()

    url = (
        f"{_base_url()}/sites/{_site_id}"
        f"/views/{view_id}/image"
        f"?resolution={resolution}"
    )

    resp = requests.get(
        url,
        headers={"x-tableau-auth": token},
        verify=_ssl(),
        timeout=60,
    )

    # Token expired → sign in again and retry once
    if resp.status_code == 401:
        sign_in()
        resp = requests.get(
            url,
            headers={"x-tableau-auth": _token},
            verify=_ssl(),
            timeout=60,
        )

    resp.raise_for_status()
    return resp.content   # PNG bytes


def get_view_pdf(view_id: str, page_type: str = "A4", orientation: str = "Landscape") -> bytes:
    """
    Fetch a PDF snapshot of a Tableau view by view ID.

    Args:
        view_id:     Tableau view ID
        page_type:   A4 / Letter / Legal / Tabloid / Unspecified
        orientation: Landscape / Portrait

    Returns:
        PDF bytes
    """
    token = _get_token()

    url = (
        f"{_base_url()}/sites/{_site_id}"
        f"/views/{view_id}/pdf"
        f"?type={page_type}&orientation={orientation}"
    )

    resp = requests.get(
        url,
        headers={"x-tableau-auth": token},
        verify=_ssl(),
        timeout=60,
    )

    if resp.status_code == 401:
        sign_in()
        resp = requests.get(
            url,
            headers={"x-tableau-auth": _token},
            verify=_ssl(),
            timeout=60,
        )

    resp.raise_for_status()
    return resp.content   # PDF bytes
