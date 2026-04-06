"""
ingestion/tableau_extractor.py

Generalised Tableau data extractor.
Replaces the hardcoded run_tableau_download.py pattern you already have.

Supports: PNG · PDF · CSV · underlying data via REST API
Auth:      Username+Password  OR  Personal Access Token (PAT)

All packages from prath env:
    tableauserverclient   requests==2.32.5   pandas==2.0.3
"""

from __future__ import annotations

import io
import os
import time
import logging
import requests
import pandas as pd
import tableauserverclient as TSC
from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List, Tuple

log = logging.getLogger("tableau_extractor")


# ─────────────────────────────────────────────────────────────
# CONNECTION CONFIG  — the ONLY thing callers need to supply
# ─────────────────────────────────────────────────────────────

@dataclass
class TableauConnection:
    """
    All the details needed to connect to ANY Tableau Server.
    Mirrors your existing CONFIG dict — now a proper dataclass.
    """
    server_url:   str
    username:     str
    password:     str
    site_id:      str  = ""          # blank = Default site
    api_version:  str  = "3.1"       # updated: Copilot fix #4
    ssl_cert_path: str = ""          # path to .pem, or "" to skip verify

    # Optional PAT (Personal Access Token) auth — preferred for production
    pat_name:     Optional[str] = None
    pat_value:    Optional[str] = None

    @classmethod
    def from_env(cls) -> "TableauConnection":
        """Build from environment variables (your .env file)."""
        return cls(
            server_url   = os.getenv("TABLEAU_SERVER", ""),
            username     = os.getenv("TABLEAU_USERNAME", ""),
            password     = os.getenv("TABLEAU_PASSWORD", ""),
            site_id      = os.getenv("TABLEAU_SITE", ""),
            api_version  = os.getenv("TABLEAU_API_VERSION", "3.1"),
            ssl_cert_path= os.getenv("TABLEAU_SSL_CERT_PATH", ""),
            pat_name     = os.getenv("TABLEAU_PAT_NAME", None),
            pat_value    = os.getenv("TABLEAU_PAT_VALUE", None),
        )


@dataclass
class ViewTarget:
    """
    Identifies which dashboard view to extract.
    Only view_id is required — everything else is optional metadata.
    """
    view_id:    str
    view_name:  str = ""
    workbook_id:str = ""
    scorecard_id: str = ""    # your internal ID (e.g. "uk-kri")
    description: str = ""


# ─────────────────────────────────────────────────────────────
# EXTRACTOR
# ─────────────────────────────────────────────────────────────

class TableauExtractor:
    """
    Generalised, reusable Tableau data extractor.

    Usage:
        conn    = TableauConnection.from_env()
        target  = ViewTarget(view_id="c69d5ca6-...", scorecard_id="uk-kri")
        extract = TableauExtractor(conn)

        png_bytes  = extract.get_image(target)
        pdf_bytes  = extract.get_pdf(target)
        df         = extract.get_dataframe(target)   ← key for AI pipeline
        raw_json   = extract.get_underlying_json(target)
    """

    def __init__(self, conn: TableauConnection, retries: int = 2):
        self.conn    = conn
        self.retries = retries
        self._server: Optional[TSC.Server] = None

    # ── Internal TSC connection (matches your existing script) ──────────
    def _get_server(self) -> TSC.Server:
        """
        Creates authenticated TSC.Server.
        Exactly mirrors your run_tableau_download.py pattern.
        Supports both username+password AND PAT auth.
        """
        c   = self.conn
        ssl = c.ssl_cert_path

        if ssl and not os.path.exists(ssl):
            raise FileNotFoundError(f"SSL cert not found: {ssl}")

        # Auth: PAT takes priority over username/password
        if c.pat_name and c.pat_value:
            auth = TSC.PersonalAccessTokenAuth(
                token_name=c.pat_name,
                personal_access_token=c.pat_value,
                site_id=c.site_id,
            )
        else:
            auth = TSC.TableauAuth(
                username=c.username,
                password=c.password,
                site_id=c.site_id,
            )

        server = TSC.Server(c.server_url)
        server.version = c.api_version           # same as your existing script
        server.add_http_options({"verify": ssl if ssl else False})
        server.auth.sign_in(auth)

        log.info(f"Tableau sign-in OK: {server.baseurl}")
        return server

    def _retry(self, fn, *args, **kwargs):
        """Execute fn with exponential backoff retry."""
        last_err = None
        for attempt in range(self.retries + 1):
            try:
                return fn(*args, **kwargs)
            except Exception as e:
                last_err = e
                if attempt < self.retries:
                    wait = 2 ** attempt
                    log.warning(f"Attempt {attempt+1} failed: {e} — retrying in {wait}s")
                    time.sleep(wait)
        raise last_err

    # ── PNG snapshot ──────────────────────────────────────────────────────
    def get_image(self, target: ViewTarget, resolution: int = 1920) -> bytes:
        """Returns PNG bytes. Matches your existing populate_image call."""
        def _fetch():
            server = self._get_server()
            try:
                view = server.views.get_by_id(target.view_id)
                log.info(f"Found view: {view.name}")
                server.views.populate_image(view)
                return view.image
            finally:
                server.auth.sign_out()

        return self._retry(_fetch)

    # ── PDF download ──────────────────────────────────────────────────────
    def get_pdf(self,
                target: ViewTarget,
                page_type: str = "Unspecified",
                orientation: str = "Landscape") -> bytes:
        """
        Returns PDF bytes.
        Mirrors your existing populate_pdf block exactly.
        """
        page_map = {
            "Unspecified": TSC.PDFRequestOptions.PageType.Unspecified,
            "A4":          TSC.PDFRequestOptions.PageType.A4,
            "Letter":      TSC.PDFRequestOptions.PageType.Letter,
        }
        orient_map = {
            "Landscape": TSC.PDFRequestOptions.Orientation.Landscape,
            "Portrait":  TSC.PDFRequestOptions.Orientation.Portrait,
        }

        def _fetch():
            server = self._get_server()
            try:
                view    = server.views.get_by_id(target.view_id)
                pdf_opt = TSC.PDFRequestOptions(
                    page_type   = page_map.get(page_type, TSC.PDFRequestOptions.PageType.Unspecified),
                    orientation = orient_map.get(orientation, TSC.PDFRequestOptions.Orientation.Landscape),
                )
                server.views.populate_pdf(view, pdf_opt)
                return view.pdf
            finally:
                server.auth.sign_out()

        return self._retry(_fetch)

    # ── CSV → DataFrame ───────────────────────────────────────────────────
    def get_dataframe(self,
                      target: ViewTarget,
                      filters: Optional[Dict[str, str]] = None) -> pd.DataFrame:
        """
        Core method for AI pipeline.
        Fetches CSV underlying data and returns a clean pandas DataFrame.

        Args:
            target:  Which view to extract
            filters: Optional Tableau action filters e.g. {"MONTH(Report Date)": "Nov-25"}
        """
        def _fetch():
            server  = self._get_server()
            try:
                view    = server.views.get_by_id(target.view_id)
                csv_opt = TSC.CSVRequestOptions()

                # Apply any action filters (equivalent to your commented-out section)
                if filters:
                    for field_name, value in filters.items():
                        csv_opt.vf(field_name, value)
                        log.info(f"Filter applied: {field_name} = {value}")

                server.views.populate_csv(view, csv_opt)
                raw = b"".join(chunk for chunk in view.csv)
                return raw
            finally:
                server.auth.sign_out()

        raw_bytes = self._retry(_fetch)

        if not raw_bytes:
            log.warning(f"Empty CSV for view {target.view_id}")
            return pd.DataFrame()

        df = pd.read_csv(
            io.BytesIO(raw_bytes),
            na_values=["N/A", "-", "null", "NULL", "", " "],
            low_memory=False,
        )
        # Clean string columns
        for col in df.select_dtypes("object").columns:
            df[col] = df[col].str.strip()

        log.info(f"Extracted {len(df)} rows × {len(df.columns)} cols from {target.view_id}")
        return df

    # ── Underlying data via Metadata API (REST) ────────────────────────────
    def get_underlying_json(self, target: ViewTarget) -> List[Dict[str, Any]]:
        """
        Fetches full underlying data as JSON list using Tableau REST API directly.
        Returns all rows as [{col: val, ...}, ...]

        Uses requests (not TSC) for finer control.
        """
        c   = self.conn
        ssl = c.ssl_cert_path or False

        # Step 1 — sign in to get token + site_id
        signin_url = f"{c.server_url}/api/{c.api_version}/auth/signin"
        payload = {
            "credentials": {
                "name": c.username,
                "password": c.password,
                "site": {"contentUrl": c.site_id}
            }
        }
        resp = requests.post(signin_url, json=payload, verify=ssl, timeout=30)
        resp.raise_for_status()

        creds    = resp.json()["credentials"]
        token    = creds["token"]
        site_id  = creds["site"]["id"]
        headers  = {"x-tableau-auth": token, "Accept": "application/json"}

        try:
            # Step 2 — get data from view
            data_url = (
                f"{c.server_url}/api/{c.api_version}/sites/{site_id}"
                f"/views/{target.view_id}/data"
            )
            data_resp = requests.get(data_url, headers=headers, verify=ssl, timeout=60)
            data_resp.raise_for_status()

            rows = data_resp.json().get("data", [])
            log.info(f"REST API: {len(rows)} rows from {target.view_id}")
            return rows

        finally:
            # Always sign out
            requests.post(
                f"{c.server_url}/api/{c.api_version}/auth/signout",
                headers=headers, verify=ssl, timeout=10
            )

    # ── Discover all views in a workbook ────────────────────────────────────
    def list_views(self, workbook_id: str) -> List[Dict[str, str]]:
        """
        List all views in a workbook.
        Useful for auto-discovery — no hardcoding required.
        """
        def _fetch():
            server = self._get_server()
            try:
                wb   = server.workbooks.get_by_id(workbook_id)
                server.workbooks.populate_views(wb)
                return [
                    {
                        "view_id":   v.id,
                        "view_name": v.name,
                        "content_url": v.content_url,
                    }
                    for v in wb.views
                ]
            finally:
                server.auth.sign_out()

        return self._retry(_fetch)

    # ── Discover all workbooks on the site ─────────────────────────────────
    def list_workbooks(self, max_results: int = 100) -> List[Dict[str, str]]:
        """
        Auto-discover all workbooks.
        No dashboard ID required — explore programmatically.
        """
        def _fetch():
            server  = self._get_server()
            request = TSC.RequestOptions(pagesize=max_results)
            try:
                workbooks, _ = server.workbooks.get(request)
                return [
                    {
                        "workbook_id":   wb.id,
                        "workbook_name": wb.name,
                        "project_name":  wb.project_name,
                        "owner_id":      wb.owner_id,
                        "updated_at":    str(wb.updated_at),
                    }
                    for wb in workbooks
                ]
            finally:
                server.auth.sign_out()

        return self._retry(_fetch)

    # ── Visual dashboard extraction ────────────────────────────────────────────

    def extract_visual_snapshot(
        self,
        target:      "ViewTarget",
        source_name: str = "",
        filters:     Optional[Dict[str, str]] = None,
    ) -> "DashboardSnapshot":
        """
        Full visual extraction for chart-heavy dashboards.
        Combines CSV underlying data + VisualDashboardExtractor.

        Handles both:
          - DCRM Data Quality Metrics (donut + bar charts)
          - UK KRI Scorecard (RAG matrix)
          - Any other visual dashboard

        Returns DashboardSnapshot with all visuals structured.
        """
        from backend.ingestion.visual_extractor import VisualDashboardExtractor

        log.info(f"Visual extraction: {target.view_id} | {source_name}")

        # Step 1: Get underlying CSV data
        try:
            df = self.get_dataframe(target, filters=filters)
            log.info(f"CSV extracted: {len(df)} rows × {len(df.columns)} cols")
        except Exception as e:
            log.error(f"CSV extraction failed: {e}")
            from backend.ingestion.visual_extractor import DashboardSnapshot
            return DashboardSnapshot(
                view_id="", source_id=target.scorecard_id,
                source_name=source_name, dashboard_type="error",
                total_records=0, visuals=[], raw_columns=[],
                kpi_summary=f"Extraction failed: {e}"
            )

        if df.empty:
            log.warning(f"Empty DataFrame for {target.view_id}")

        # Step 2: Extract visual structure
        extractor = VisualDashboardExtractor()
        snapshot  = extractor.extract(
            df          = df,
            source_id   = target.scorecard_id or target.view_id[:12],
            source_name = source_name or target.view_name,
            view_id     = target.view_id,
        )
        return snapshot

    def ping(self) -> Tuple[bool, int]:
        """Quick connectivity check."""
        t0 = __import__("time").time()
        try:
            s = self._get_server()
            s.auth.sign_out()
            return True, int((__import__("time").time() - t0) * 1000)
        except Exception as e:
            log.warning(f"Ping failed: {e}")
            return False, int((__import__("time").time() - t0) * 1000)

    # ── Multi-strategy extraction for complex dashboards ──────────────────────

    def extract_with_fallback(
        self,
        target:      "ViewTarget",
        source_name: str = "",
        filters:     Optional[Dict[str, str]] = None,
    ) -> "DashboardSnapshot":
        """
        Robust multi-strategy extraction for ANY Tableau dashboard.

        Tries in order:
          1. populate_csv (TSC) → underlying tabular data [fastest]
          2. REST API /views/{id}/data → JSON view data [richer]
          3. populate_image → PNG only [last resort — no structured data]

        Works for all visual types:
          bar, line, pie, scatter, map, KPI, scorecard, treemap,
          heat map, histogram, waterfall, Gantt, crosstab, etc.
        """
        from backend.ingestion.visual_extractor import VisualDashboardExtractor, DashboardSnapshot
        import pandas as pd, time as _time

        log.info(f"Multi-strategy extraction: {target.view_id} ({source_name})")
        vde       = VisualDashboardExtractor()
        source_id = target.scorecard_id or target.view_id[:12]
        t0        = _time.time()

        # ── Strategy 1: TSC populate_csv ─────────────────────────────────
        df = pd.DataFrame()
        try:
            df = self.get_dataframe(target, filters=filters)
            log.info(f"Strategy 1 (CSV): {len(df)} rows × {len(df.columns)} cols")
        except Exception as e:
            log.warning(f"Strategy 1 (CSV) failed: {e}")

        # ── Strategy 2: REST API /data endpoint ─────────────────────────
        if df.empty:
            try:
                rows = self.get_underlying_json(target)
                if rows:
                    df = pd.DataFrame(rows)
                    log.info(f"Strategy 2 (REST): {len(df)} rows")
            except Exception as e:
                log.warning(f"Strategy 2 (REST) failed: {e}")

        # ── Strategy 3: Image only (structured extraction not possible) ──
        if df.empty:
            log.warning(f"All data strategies failed — returning image-only snapshot")
            try:
                img = self.get_image(target)
                return DashboardSnapshot(
                    view_id=target.view_id, source_id=source_id,
                    source_name=source_name, total_rows=0, total_cols=0,
                    all_columns=[], col_profiles=[], visual_types=[], agg_blocks=[],
                    schema_text="Image-only extraction — no underlying data accessible.",
                    summary_text=(
                        f"{source_name}: Image captured ({len(img)} bytes). "
                        "Underlying data not accessible — check view permissions or filter state."
                    ),
                    has_time=False, has_geo=False, has_rag=False,
                    has_threshold=False, has_multi_measure=False,
                    dashboard_type="image_only",
                    extraction_ms=int((_time.time()-t0)*1000),
                )
            except Exception as e:
                log.error(f"Image capture also failed: {e}")

        # ── Build snapshot from whatever data we got ──────────────────────
        snap = vde.extract(
            df          = df,
            source_id   = source_id,
            source_name = source_name or target.view_name,
            view_id     = target.view_id,
        )
        snap.extraction_ms = int((_time.time()-t0)*1000)
        return snap

    def extract_workbook_all_views(
        self,
        workbook_id: str,
        source_name_prefix: str = "",
    ) -> Dict[str, "DashboardSnapshot"]:
        """
        Extract data from ALL views in a workbook.
        Useful for complex multi-sheet dashboards.

        Returns dict of {view_name: DashboardSnapshot}.
        """
        from backend.ingestion.visual_extractor import VisualDashboardExtractor
        vde     = VisualDashboardExtractor()
        results = {}

        try:
            views = self.list_views(workbook_id)
        except Exception as e:
            log.error(f"Cannot list views for workbook {workbook_id}: {e}")
            return results

        log.info(f"Extracting {len(views)} views from workbook {workbook_id}")
        for v in views:
            vid  = v["view_id"]
            vname = v.get("view_name", vid)
            target = ViewTarget(
                view_id=vid,
                view_name=vname,
                workbook_id=workbook_id,
                scorecard_id=vname.lower().replace(" ","_"),
            )
            try:
                snap = self.extract_with_fallback(
                    target      = target,
                    source_name = f"{source_name_prefix} — {vname}" if source_name_prefix else vname,
                )
                results[vname] = snap
                log.info(f"  ✓ {vname}: {snap.total_rows} rows | {snap.dashboard_type}")
            except Exception as e:
                log.error(f"  ✗ {vname}: {e}")

        return results
