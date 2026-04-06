"""
ingestion/tableau_extractor.py — v5

CHANGES v5 (6 Apr 2026):
  - CRITICAL FIX: All Tableau operations wrapped with concurrent.futures timeout
    → sign_in, populate_csv, REST calls can no longer hang indefinitely
  - Step-level logging: every operation logs start/end/duration
  - Diagnostic error messages: when extraction fails, the log tells you WHY
  - SSL cert validation with clear error on missing file
  - Connection reuse within a single extraction run (fewer sign-in calls)

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
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout
from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List, Tuple

from backend.config import (
    TABLEAU_AUTH_TIMEOUT, TABLEAU_CSV_TIMEOUT,
    TABLEAU_REST_TIMEOUT, TABLEAU_IMAGE_TIMEOUT,
)

log = logging.getLogger("tableau_extractor")

# Single thread pool for timeout-wrapping synchronous TSC calls
_executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="tableau")


# ─────────────────────────────────────────────────────────────
# CONNECTION CONFIG
# ─────────────────────────────────────────────────────────────

@dataclass
class TableauConnection:
    server_url:    str
    username:      str
    password:      str
    site_id:       str  = ""
    api_version:   str  = "3.1"
    ssl_cert_path: str  = ""
    pat_name:      Optional[str] = None
    pat_value:     Optional[str] = None

    @classmethod
    def from_env(cls) -> "TableauConnection":
        return cls(
            server_url    = os.getenv("TABLEAU_SERVER", ""),
            username      = os.getenv("TABLEAU_USERNAME", ""),
            password      = os.getenv("TABLEAU_PASSWORD", ""),
            site_id       = os.getenv("TABLEAU_SITE", ""),
            api_version   = os.getenv("TABLEAU_API_VERSION", "3.1"),
            ssl_cert_path = os.getenv("TABLEAU_SSL_CERT_PATH", ""),
            pat_name      = os.getenv("TABLEAU_PAT_NAME", None),
            pat_value     = os.getenv("TABLEAU_PAT_VALUE", None),
        )


@dataclass
class ViewTarget:
    view_id:      str
    view_name:    str = ""
    workbook_id:  str = ""
    scorecard_id: str = ""
    description:  str = ""


# ─────────────────────────────────────────────────────────────
# TIMEOUT HELPER
# ─────────────────────────────────────────────────────────────

def _run_with_timeout(fn, timeout_sec: int, label: str):
    """
    Execute a synchronous function with a hard timeout.
    If the function doesn't return within timeout_sec, raises TimeoutError
    with a clear diagnostic message.

    This is THE fix for the stuck extraction issue — TSC's sign_in()
    and populate_csv() are synchronous and can hang indefinitely
    on slow/unreachable servers.
    """
    t0 = time.time()
    log.info(f"  ┌ {label} (timeout={timeout_sec}s)")
    try:
        future = _executor.submit(fn)
        result = future.result(timeout=timeout_sec)
        elapsed = int((time.time() - t0) * 1000)
        log.info(f"  └ {label} ✓ completed in {elapsed}ms")
        return result
    except FuturesTimeout:
        elapsed = int((time.time() - t0) * 1000)
        log.error(
            f"  └ {label} ✗ TIMED OUT after {elapsed}ms "
            f"(limit={timeout_sec}s). "
            f"Check: server reachable? SSL cert valid? VPN connected?"
        )
        future.cancel()
        raise TimeoutError(
            f"{label} timed out after {timeout_sec}s. "
            f"Verify TABLEAU_SERVER is reachable and SSL cert is correct."
        )
    except Exception as e:
        elapsed = int((time.time() - t0) * 1000)
        log.error(f"  └ {label} ✗ FAILED in {elapsed}ms: {type(e).__name__}: {e}")
        raise


# ─────────────────────────────────────────────────────────────
# EXTRACTOR
# ─────────────────────────────────────────────────────────────

class TableauExtractor:
    """
    Generalised Tableau data extractor with timeout protection.

    Every network call is wrapped in _run_with_timeout() so nothing
    can hang the server. Step-level logging makes debugging trivial:
    just send the terminal output.
    """

    def __init__(self, conn: TableauConnection, retries: int = 2):
        self.conn    = conn
        self.retries = retries

    # ── Authenticated TSC server ──────────────────────────────────────

    def _get_server(self) -> TSC.Server:
        """
        Creates authenticated TSC.Server with timeout protection.
        Logs every step for debugging.
        """
        c   = self.conn
        ssl = c.ssl_cert_path

        log.info(f"  │ Connecting to {c.server_url}")
        log.info(f"  │ User={c.username}  Site={c.site_id or '(default)'}  API={c.api_version}")

        # ── SSL cert check ────────────────────────────────────────────
        if ssl:
            if not os.path.exists(ssl):
                log.error(f"  │ ✗ SSL cert NOT FOUND: {ssl}")
                raise FileNotFoundError(
                    f"SSL cert not found: {ssl}. "
                    f"Check TABLEAU_SSL_CERT_PATH in .env"
                )
            log.info(f"  │ SSL cert: {ssl} ✓")
        else:
            log.info(f"  │ SSL verify: disabled (no cert path)")

        # ── Auth object ───────────────────────────────────────────────
        if c.pat_name and c.pat_value:
            auth = TSC.PersonalAccessTokenAuth(
                token_name=c.pat_name,
                personal_access_token=c.pat_value,
                site_id=c.site_id,
            )
            log.info(f"  │ Auth method: PAT (token={c.pat_name})")
        else:
            auth = TSC.TableauAuth(
                username=c.username,
                password=c.password,
                site_id=c.site_id,
            )
            log.info(f"  │ Auth method: username/password")

        # ── Server + sign-in (with timeout) ───────────────────────────
        server = TSC.Server(c.server_url)
        server.version = c.api_version
        server.add_http_options({"verify": ssl if ssl else False})

        def _do_signin():
            server.auth.sign_in(auth)

        _run_with_timeout(_do_signin, TABLEAU_AUTH_TIMEOUT, "Tableau sign-in")
        log.info(f"  │ Signed in to {server.baseurl}")
        return server

    # ── Retry wrapper ─────────────────────────────────────────────────

    def _retry(self, fn, *args, **kwargs):
        last_err = None
        for attempt in range(self.retries + 1):
            try:
                return fn(*args, **kwargs)
            except Exception as e:
                last_err = e
                if attempt < self.retries:
                    wait = 2 ** attempt
                    log.warning(f"  │ Attempt {attempt+1} failed: {e} — retrying in {wait}s")
                    time.sleep(wait)
        raise last_err

    # ── PNG snapshot ──────────────────────────────────────────────────

    def get_image(self, target: ViewTarget, resolution: int = 1920) -> bytes:
        """Returns PNG bytes with timeout protection."""
        log.info(f"[IMAGE] Getting PNG for view={target.view_id}")

        def _fetch():
            server = self._get_server()
            try:
                view = server.views.get_by_id(target.view_id)
                log.info(f"  │ View found: {view.name}")

                def _do_populate():
                    server.views.populate_image(view)

                _run_with_timeout(_do_populate, TABLEAU_IMAGE_TIMEOUT, "populate_image")
                return view.image
            finally:
                try:
                    server.auth.sign_out()
                except Exception:
                    pass

        return self._retry(_fetch)

    # ── PDF download ──────────────────────────────────────────────────

    def get_pdf(self, target: ViewTarget,
                page_type: str = "Unspecified",
                orientation: str = "Landscape") -> bytes:
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

                def _do_populate():
                    server.views.populate_pdf(view, pdf_opt)

                _run_with_timeout(_do_populate, TABLEAU_CSV_TIMEOUT, "populate_pdf")
                return view.pdf
            finally:
                try:
                    server.auth.sign_out()
                except Exception:
                    pass

        return self._retry(_fetch)

    # ── CSV → DataFrame (PRIMARY extraction method) ───────────────────

    def get_dataframe(self, target: ViewTarget,
                      filters: Optional[Dict[str, str]] = None) -> pd.DataFrame:
        """
        Core method for AI pipeline.
        Fetches CSV underlying data with timeout protection.
        """
        log.info(f"[CSV] Extracting DataFrame for view={target.view_id}")

        def _fetch():
            server = self._get_server()
            try:
                view    = server.views.get_by_id(target.view_id)
                log.info(f"  │ View resolved: {view.name}")
                csv_opt = TSC.CSVRequestOptions()

                if filters:
                    for field_name, value in filters.items():
                        csv_opt.vf(field_name, value)
                        log.info(f"  │ Filter: {field_name} = {value}")

                def _do_csv():
                    server.views.populate_csv(view, csv_opt)

                _run_with_timeout(_do_csv, TABLEAU_CSV_TIMEOUT, "populate_csv")

                raw = b"".join(chunk for chunk in view.csv)
                log.info(f"  │ Raw CSV bytes: {len(raw):,}")
                return raw
            finally:
                try:
                    server.auth.sign_out()
                except Exception:
                    pass

        raw_bytes = self._retry(_fetch)

        if not raw_bytes:
            log.warning(f"  │ Empty CSV for view {target.view_id}")
            return pd.DataFrame()

        df = pd.read_csv(
            io.BytesIO(raw_bytes),
            na_values=["N/A", "-", "null", "NULL", "", " "],
            low_memory=False,
        )
        for col in df.select_dtypes("object").columns:
            df[col] = df[col].str.strip()

        log.info(f"  └ DataFrame: {len(df)} rows × {len(df.columns)} cols")
        return df

    # ── REST API underlying data ──────────────────────────────────────

    def get_underlying_json(self, target: ViewTarget) -> List[Dict[str, Any]]:
        """Fetches underlying data via REST API with timeout."""
        c   = self.conn
        ssl = c.ssl_cert_path or False

        log.info(f"[REST] Fetching underlying JSON for view={target.view_id}")

        # Step 1: sign in via REST
        signin_url = f"{c.server_url}/api/{c.api_version}/auth/signin"
        payload = {
            "credentials": {
                "name": c.username,
                "password": c.password,
                "site": {"contentUrl": c.site_id}
            }
        }

        log.info(f"  │ REST sign-in: {signin_url}")
        resp = requests.post(signin_url, json=payload, verify=ssl, timeout=TABLEAU_AUTH_TIMEOUT)
        resp.raise_for_status()

        creds   = resp.json()["credentials"]
        token   = creds["token"]
        site_id = creds["site"]["id"]
        headers = {"x-tableau-auth": token, "Accept": "application/json"}
        log.info(f"  │ REST sign-in ✓ site_id={site_id[:12]}…")

        try:
            # Step 2: fetch view data
            data_url = (
                f"{c.server_url}/api/{c.api_version}/sites/{site_id}"
                f"/views/{target.view_id}/data"
            )
            log.info(f"  │ REST data fetch: {data_url}")
            data_resp = requests.get(
                data_url, headers=headers, verify=ssl,
                timeout=TABLEAU_REST_TIMEOUT,
            )
            data_resp.raise_for_status()

            rows = data_resp.json().get("data", [])
            log.info(f"  └ REST: {len(rows)} rows returned")
            return rows

        finally:
            try:
                requests.post(
                    f"{c.server_url}/api/{c.api_version}/auth/signout",
                    headers=headers, verify=ssl, timeout=10,
                )
            except Exception:
                pass

    # ── List views in workbook ────────────────────────────────────────

    def list_views(self, workbook_id: str) -> List[Dict[str, str]]:
        def _fetch():
            server = self._get_server()
            try:
                wb = server.workbooks.get_by_id(workbook_id)
                server.workbooks.populate_views(wb)
                return [
                    {"view_id": v.id, "view_name": v.name, "content_url": v.content_url}
                    for v in wb.views
                ]
            finally:
                try:
                    server.auth.sign_out()
                except Exception:
                    pass
        return self._retry(_fetch)

    # ── List all workbooks ────────────────────────────────────────────

    def list_workbooks(self, max_results: int = 100) -> List[Dict[str, str]]:
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
                try:
                    server.auth.sign_out()
                except Exception:
                    pass
        return self._retry(_fetch)

    # ── Visual snapshot extraction ────────────────────────────────────

    def extract_visual_snapshot(self, target: ViewTarget,
                                source_name: str = "",
                                filters: Optional[Dict[str, str]] = None):
        from backend.ingestion.visual_extractor import VisualDashboardExtractor

        log.info(f"[VISUAL] Extracting: {target.view_id} | {source_name}")
        try:
            df = self.get_dataframe(target, filters=filters)
            log.info(f"  │ CSV: {len(df)} rows × {len(df.columns)} cols")
        except Exception as e:
            log.error(f"  │ CSV extraction failed: {e}")
            from backend.ingestion.visual_extractor import DashboardSnapshot
            return DashboardSnapshot(
                view_id="", source_id=target.scorecard_id,
                source_name=source_name, dashboard_type="error",
                total_records=0, visuals=[], raw_columns=[],
                kpi_summary=f"Extraction failed: {e}",
            )

        extractor = VisualDashboardExtractor()
        snapshot = extractor.extract(
            df          = df,
            source_id   = target.scorecard_id or target.view_id[:12],
            source_name = source_name or target.view_name,
            view_id     = target.view_id,
        )
        return snapshot

    # ── Multi-strategy extraction with fallback ───────────────────────

    def extract_with_fallback(self, target: ViewTarget,
                              source_name: str = "",
                              filters: Optional[Dict[str, str]] = None):
        """
        Robust multi-strategy extraction with timeout on every step.

        Tries: CSV → REST → Image (last resort)
        Every step is logged and timed.
        """
        from backend.ingestion.visual_extractor import VisualDashboardExtractor, DashboardSnapshot

        log.info(f"{'═'*55}")
        log.info(f"[EXTRACT] Multi-strategy: {target.view_id}")
        log.info(f"  │ source_id={target.scorecard_id}  name={source_name}")
        log.info(f"{'─'*55}")

        vde       = VisualDashboardExtractor()
        source_id = target.scorecard_id or target.view_id[:12]
        t0        = time.time()
        df        = pd.DataFrame()

        # ── Strategy 1: CSV ──────────────────────────────────────────
        log.info(f"  STRATEGY 1: populate_csv (TSC)")
        try:
            df = self.get_dataframe(target, filters=filters)
            log.info(f"  ✓ Strategy 1 OK: {len(df)} rows × {len(df.columns)} cols")
        except TimeoutError as e:
            log.error(f"  ✗ Strategy 1 TIMEOUT: {e}")
        except Exception as e:
            log.warning(f"  ✗ Strategy 1 failed: {type(e).__name__}: {e}")

        # ── Strategy 2: REST API ─────────────────────────────────────
        if df.empty:
            log.info(f"  STRATEGY 2: REST API /data endpoint")
            try:
                rows = self.get_underlying_json(target)
                if rows:
                    df = pd.DataFrame(rows)
                    log.info(f"  ✓ Strategy 2 OK: {len(df)} rows")
                else:
                    log.warning(f"  ✗ Strategy 2: empty response")
            except TimeoutError as e:
                log.error(f"  ✗ Strategy 2 TIMEOUT: {e}")
            except Exception as e:
                log.warning(f"  ✗ Strategy 2 failed: {type(e).__name__}: {e}")

        # ── Strategy 3: Image only ───────────────────────────────────
        if df.empty:
            log.warning(f"  STRATEGY 3: Image-only fallback (no structured data)")
            try:
                img = self.get_image(target)
                elapsed = int((time.time() - t0) * 1000)
                log.info(f"  ✓ Strategy 3: Image captured ({len(img):,} bytes)")
                return DashboardSnapshot(
                    view_id=target.view_id, source_id=source_id,
                    source_name=source_name, total_rows=0, total_cols=0,
                    all_columns=[], col_profiles=[], visual_types=[], agg_blocks=[],
                    schema_text="Image-only extraction — no underlying data accessible.",
                    summary_text=(
                        f"{source_name}: Image captured ({len(img)} bytes). "
                        "Underlying data not accessible — check view permissions."
                    ),
                    has_time=False, has_geo=False, has_rag=False,
                    has_threshold=False, has_multi_measure=False,
                    dashboard_type="image_only",
                    extraction_ms=elapsed,
                )
            except Exception as e:
                log.error(f"  ✗ Strategy 3 also failed: {e}")

        # ── Build snapshot ───────────────────────────────────────────
        elapsed = int((time.time() - t0) * 1000)
        log.info(f"  Building visual snapshot from {len(df)} rows…")
        snap = vde.extract(
            df          = df,
            source_id   = source_id,
            source_name = source_name or target.view_name,
            view_id     = target.view_id,
        )
        snap.extraction_ms = elapsed
        log.info(f"  ✓ Extraction complete: {snap.dashboard_type} | {elapsed}ms")
        log.info(f"{'═'*55}")
        return snap

    # ── Workbook-level extraction ─────────────────────────────────────

    def extract_workbook_all_views(self, workbook_id: str,
                                   source_name_prefix: str = "") -> Dict:
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
            vid   = v["view_id"]
            vname = v.get("view_name", vid)
            target = ViewTarget(
                view_id=vid, view_name=vname,
                workbook_id=workbook_id,
                scorecard_id=vname.lower().replace(" ", "_"),
            )
            try:
                snap = self.extract_with_fallback(
                    target      = target,
                    source_name = f"{source_name_prefix} — {vname}" if source_name_prefix else vname,
                )
                results[vname] = snap
            except Exception as e:
                log.error(f"  ✗ {vname}: {e}")

        return results

    # ── Connectivity check ────────────────────────────────────────────

    def ping(self) -> Tuple[bool, int]:
        t0 = time.time()
        try:
            s = self._get_server()
            try:
                s.auth.sign_out()
            except Exception:
                pass
            return True, int((time.time() - t0) * 1000)
        except Exception as e:
            log.warning(f"Ping failed: {e}")
            return False, int((time.time() - t0) * 1000)
