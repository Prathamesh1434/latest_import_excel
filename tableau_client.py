"""
tableau_client.py
Tableau connection using tableauserverclient (TSC) — same library
as your existing run_tableau_download.py script.

Packages used (already in prath env):
    tableauserverclient
    python-dotenv
"""

import tableauserverclient as TSC
import os
from dotenv import load_dotenv

load_dotenv()

# ── Config from .env ──────────────────────────────────────────────────────────
CONFIG = {
    "server_url":    os.getenv("TABLEAU_SERVER"),
    "username":      os.getenv("TABLEAU_USERNAME"),
    "password":      os.getenv("TABLEAU_PASSWORD"),
    "site":          os.getenv("TABLEAU_SITE", ""),
    "ssl_cert_path": os.getenv("TABLEAU_SSL_CERT_PATH", ""),
    "api_version":   os.getenv("TABLEAU_API_VERSION", "3.0"),
}


def _get_server() -> TSC.Server:
    """
    Create and return an authenticated TSC Server object.
    Mirrors exactly what your run_tableau_download.py does.
    """
    ssl_cert = CONFIG["ssl_cert_path"]

    # Verify SSL cert exists if path is provided
    if ssl_cert and not os.path.exists(ssl_cert):
        raise FileNotFoundError(f"SSL cert not found at: {ssl_cert}")

    # Same as your script — TSC.TableauAuth
    tableau_auth = TSC.TableauAuth(
        username=CONFIG["username"],
        password=CONFIG["password"],
        site_id=CONFIG["site"],
    )

    # Same as your script — TSC.Server + version pin
    server = TSC.Server(CONFIG["server_url"])
    server.version = CONFIG["api_version"]

    # Same as your script — add_http_options for SSL
    if ssl_cert:
        server.add_http_options({"verify": ssl_cert})
    else:
        server.add_http_options({"verify": False})

    # Sign in — same as your script
    server.auth.sign_in(tableau_auth)
    print(f"Successfully signed in to Tableau: {server.baseurl}")

    return server


def get_view_image_bytes(view_id: str) -> bytes:
    """
    Fetch PNG snapshot of a Tableau view by view_id.
    Returns PNG bytes to serve directly to the browser.
    """
    server = _get_server()
    try:
        # Same as your script: server.views.get_by_id(view_id)
        view_item = server.views.get_by_id(view_id)
        print(f"Found view: '{view_item.name}'")

        # Populate the image
        server.views.populate_image(view_item)

        return view_item.image   # PNG bytes

    finally:
        server.auth.sign_out()


def get_view_pdf_bytes(
    view_id: str,
    page_type: str = "A4",
    orientation: str = "Landscape"
) -> bytes:
    """
    Fetch PDF of a Tableau view by view_id.
    Same logic as your existing populate_pdf block.
    """
    server = _get_server()
    try:
        view_item = server.views.get_by_id(view_id)

        # Same as your script
        pdf_req_option = TSC.PDFRequestOptions(
            page_type=TSC.PDFRequestOptions.PageType.Unspecified,
        )

        server.views.populate_pdf(view_item, pdf_req_option)

        return view_item.pdf   # PDF bytes

    finally:
        server.auth.sign_out()


def get_view_csv_bytes(view_id: str) -> bytes:
    """
    Fetch CSV data of a Tableau view by view_id.
    Same logic as your existing populate_csv block.
    """
    server = _get_server()
    try:
        view_item = server.views.get_by_id(view_id)

        csv_req_option = TSC.CSVRequestOptions()
        server.views.populate_csv(view_item, csv_req_option)

        # Same as your script — iterate chunks
        csv_bytes = b"".join(chunk for chunk in view_item.csv)
        return csv_bytes

    finally:
        server.auth.sign_out()
