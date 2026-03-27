"""
tableau_client.py
Tableau connection using tableauserverclient (TSC).
Same pattern as your existing run_tableau_download.py script.

Packages used (already in prath env):
    tableauserverclient
    python-dotenv
"""

import tableauserverclient as TSC
import os
from dotenv import load_dotenv

load_dotenv()

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
    Creates and returns authenticated TSC Server.
    Mirrors your existing run_tableau_download.py exactly.
    """
    ssl_cert = CONFIG["ssl_cert_path"]

    if ssl_cert and not os.path.exists(ssl_cert):
        raise FileNotFoundError(f"SSL cert not found at: {ssl_cert}")

    tableau_auth = TSC.TableauAuth(
        username=CONFIG["username"],
        password=CONFIG["password"],
        site_id=CONFIG["site"],
    )

    server = TSC.Server(CONFIG["server_url"])
    server.version = CONFIG["api_version"]

    if ssl_cert:
        server.add_http_options({"verify": ssl_cert})
    else:
        server.add_http_options({"verify": False})

    server.auth.sign_in(tableau_auth)
    print(f"Signed in to Tableau: {server.baseurl}")
    return server


def get_view_image_bytes(view_id: str) -> bytes:
    """Fetch PNG snapshot of a Tableau view. Returns PNG bytes."""
    server = _get_server()
    try:
        view_item = server.views.get_by_id(view_id)
        print(f"Found view: '{view_item.name}'")
        server.views.populate_image(view_item)
        return view_item.image
    finally:
        server.auth.sign_out()


def get_view_pdf_bytes(view_id: str) -> bytes:
    """Fetch PDF of a Tableau view. Returns PDF bytes."""
    server = _get_server()
    try:
        view_item = server.views.get_by_id(view_id)
        pdf_req_option = TSC.PDFRequestOptions(
            page_type=TSC.PDFRequestOptions.PageType.Unspecified,
        )
        server.views.populate_pdf(view_item, pdf_req_option)
        return view_item.pdf
    finally:
        server.auth.sign_out()


def get_view_csv_bytes(view_id: str) -> bytes:
    """Fetch CSV data of a Tableau view. Returns CSV bytes."""
    server = _get_server()
    try:
        view_item = server.views.get_by_id(view_id)
        csv_req_option = TSC.CSVRequestOptions()
        server.views.populate_csv(view_item, csv_req_option)
        return b"".join(chunk for chunk in view_item.csv)
    finally:
        server.auth.sign_out()
