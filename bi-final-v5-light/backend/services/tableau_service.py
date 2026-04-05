"""
services/tableau_service.py
Tableau via TSC — snapshot cache + retry logic.
"""
import tableauserverclient as TSC
import time, logging, os
from typing import Dict, Optional, Tuple
from backend.config import (TABLEAU_SERVER, TABLEAU_USERNAME, TABLEAU_PASSWORD,
                             TABLEAU_SITE, TABLEAU_API_VERSION, TABLEAU_SSL_CERT,
                             CACHE_TTL, check_tableau)

log = logging.getLogger("tableau")

# In-memory cache: { "viewid:type": (bytes, timestamp) }
_cache: Dict[str, Tuple[bytes, float]] = {}


def _cache_key(view_id: str, ftype: str) -> str:
    return f"{view_id}:{ftype}"

def _get_cached(view_id: str, ftype: str) -> Optional[bytes]:
    k = _cache_key(view_id, ftype)
    if k in _cache:
        data, ts = _cache[k]
        if time.time() - ts < CACHE_TTL:
            return data
        del _cache[k]
    return None

def _set_cache(view_id: str, ftype: str, data: bytes):
    _cache[_cache_key(view_id, ftype)] = (data, time.time())

def cache_stats() -> dict:
    now = time.time()
    valid = sum(1 for _,(_, ts) in _cache.items() if now - ts < CACHE_TTL)
    return {"total": len(_cache), "valid": valid, "ttl_sec": CACHE_TTL}


def _get_server() -> TSC.Server:
    if not check_tableau():
        raise RuntimeError("Tableau credentials not configured in .env")
    ssl = TABLEAU_SSL_CERT
    if ssl and not os.path.exists(ssl):
        raise FileNotFoundError(f"SSL cert not found: {ssl}")
    auth = TSC.TableauAuth(
        username=TABLEAU_USERNAME,
        password=TABLEAU_PASSWORD,
        site_id=TABLEAU_SITE,
    )
    server = TSC.Server(TABLEAU_SERVER)
    server.version = TABLEAU_API_VERSION
    server.add_http_options({"verify": ssl if ssl else False})
    server.auth.sign_in(auth)
    log.info(f"Tableau sign-in OK: {server.baseurl}")
    return server


def _retry(fn, retries=2):
    err = None
    for i in range(retries + 1):
        try:
            return fn()
        except Exception as e:
            err = e
            if i < retries:
                time.sleep(2 ** i)
                log.warning(f"Tableau retry {i+1}: {e}")
    raise err


def get_view_image(view_id: str, force=False) -> Tuple[bytes, int]:
    if not force:
        c = _get_cached(view_id, "PNG")
        if c: return c, 0
    def _f():
        t0 = time.time()
        s = _get_server()
        try:
            v = s.views.get_by_id(view_id)
            s.views.populate_image(v)
            _set_cache(view_id, "PNG", v.image)
            return v.image, int((time.time()-t0)*1000)
        finally:
            s.auth.sign_out()
    return _retry(_f)


def get_view_pdf(view_id: str, force=False) -> Tuple[bytes, int]:
    if not force:
        c = _get_cached(view_id, "PDF")
        if c: return c, 0
    def _f():
        t0 = time.time()
        s = _get_server()
        try:
            v = s.views.get_by_id(view_id)
            s.views.populate_pdf(v, TSC.PDFRequestOptions(
                page_type=TSC.PDFRequestOptions.PageType.Unspecified))
            _set_cache(view_id, "PDF", v.pdf)
            return v.pdf, int((time.time()-t0)*1000)
        finally:
            s.auth.sign_out()
    return _retry(_f)


def get_view_csv(view_id: str) -> Tuple[bytes, int]:
    def _f():
        t0 = time.time()
        s = _get_server()
        try:
            v = s.views.get_by_id(view_id)
            s.views.populate_csv(v, TSC.CSVRequestOptions())
            data = b"".join(chunk for chunk in v.csv)
            return data, int((time.time()-t0)*1000)
        finally:
            s.auth.sign_out()
    return _retry(_f)


def ping() -> Tuple[bool, int]:
    t0 = time.time()
    try:
        s = _get_server(); s.auth.sign_out()
        return True, int((time.time()-t0)*1000)
    except Exception as e:
        log.warning(f"Tableau ping failed: {e}")
        return False, int((time.time()-t0)*1000)
