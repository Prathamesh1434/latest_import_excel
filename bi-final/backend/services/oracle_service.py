"""
services/oracle_service.py
Oracle DB — connection pool, chat history, audit, analytics.
Package: oracledb==3.4.0
"""
import oracledb, uuid, logging, time
from typing import List, Optional, Dict
from backend.config import (ORACLE_USER, ORACLE_PASSWORD, ORACLE_DSN,
                             ORACLE_POOL_MIN, ORACLE_POOL_MAX, check_oracle)

log = logging.getLogger("oracle")
_pool: Optional[oracledb.ConnectionPool] = None


def _get_pool():
    global _pool
    if _pool is None:
        if not check_oracle():
            raise RuntimeError("Oracle not configured in .env")
        _pool = oracledb.create_pool(
            user=ORACLE_USER, password=ORACLE_PASSWORD, dsn=ORACLE_DSN,
            min=ORACLE_POOL_MIN, max=ORACLE_POOL_MAX, increment=1,
        )
        log.info("Oracle pool created")
    return _pool


# ── Sessions ──────────────────────────────────────────────────────────────────

def get_or_create_session(user_id: str, scorecard_id: str, name: str, session_id: Optional[str]=None) -> str:
    sid = session_id or str(uuid.uuid4())
    try:
        with _get_pool().acquire() as conn:
            if session_id:
                row = conn.fetchone(
                    "SELECT SESSION_ID FROM BI_CHAT_SESSION WHERE SESSION_ID=:1 AND USER_ID=:2 AND STATUS='ACTIVE'",
                    [session_id, user_id])
                if row:
                    conn.execute("UPDATE BI_CHAT_SESSION SET LAST_ACTIVE_DT=SYSTIMESTAMP WHERE SESSION_ID=:1", [session_id])
                    conn.commit()
                    return session_id
            conn.execute(
                "INSERT INTO BI_CHAT_SESSION(SESSION_ID,USER_ID,SCORECARD_ID,SCORECARD_NAME,CREATED_DT,LAST_ACTIVE_DT,MESSAGE_COUNT,STATUS) VALUES(:1,:2,:3,:4,SYSTIMESTAMP,SYSTIMESTAMP,0,'ACTIVE')",
                [sid, user_id, scorecard_id, name])
            conn.commit()
        return sid
    except Exception as e:
        log.error(f"get_or_create_session: {e}")
        return str(uuid.uuid4())


def save_message(session_id: str, user_id: str, scorecard_id: str,
                 role: str, content: str, model_ver="", in_tok=0, out_tok=0, resp_ms=0):
    try:
        with _get_pool().acquire() as conn:
            conn.execute(
                "INSERT INTO BI_CHAT_HISTORY(SESSION_ID,USER_ID,SCORECARD_ID,ROLE,CONTENT,MODEL_VERSION,INPUT_TOKENS,OUTPUT_TOKENS,RESPONSE_TIME_MS,CREATED_DT) VALUES(:1,:2,:3,:4,:5,:6,:7,:8,:9,SYSTIMESTAMP)",
                [session_id, user_id, scorecard_id, role, content, model_ver, in_tok, out_tok, resp_ms])
            conn.execute(
                "UPDATE BI_CHAT_SESSION SET MESSAGE_COUNT=MESSAGE_COUNT+1,LAST_ACTIVE_DT=SYSTIMESTAMP WHERE SESSION_ID=:1",
                [session_id])
            conn.commit()
        return True
    except Exception as e:
        log.error(f"save_message: {e}")
        return False


def get_user_sessions(user_id: str, scorecard_id: Optional[str]=None, limit: int=20) -> List[Dict]:
    try:
        with _get_pool().acquire() as conn:
            sc_clause = "AND SCORECARD_ID=:3" if scorecard_id else ""
            params = [user_id, limit] + ([scorecard_id] if scorecard_id else [])
            rows = conn.fetchall(
                f"SELECT SESSION_ID,SCORECARD_ID,SCORECARD_NAME,MESSAGE_COUNT,CREATED_DT,LAST_ACTIVE_DT FROM BI_CHAT_SESSION WHERE USER_ID=:1 {sc_clause} ORDER BY LAST_ACTIVE_DT DESC FETCH FIRST :2 ROWS ONLY",
                params)
            return [{"session_id":r[0],"scorecard_id":r[1],"scorecard_name":r[2],"message_count":r[3],"created_dt":r[4],"last_active_dt":r[5]} for r in rows]
    except Exception as e:
        log.error(f"get_user_sessions: {e}")
        return []


def get_session_messages(session_id: str, user_id: str, limit: int=50) -> Dict:
    try:
        with _get_pool().acquire() as conn:
            s = conn.fetchone(
                "SELECT SESSION_ID,SCORECARD_ID,SCORECARD_NAME,MESSAGE_COUNT,CREATED_DT,LAST_ACTIVE_DT FROM BI_CHAT_SESSION WHERE SESSION_ID=:1 AND USER_ID=:2",
                [session_id, user_id])
            if not s: return {}
            msgs = conn.fetchall(
                "SELECT ID,ROLE,CONTENT,CREATED_DT,RESPONSE_TIME_MS FROM BI_CHAT_HISTORY WHERE SESSION_ID=:1 ORDER BY CREATED_DT ASC FETCH FIRST :2 ROWS ONLY",
                [session_id, limit])
            return {
                "session": {"session_id":s[0],"scorecard_id":s[1],"scorecard_name":s[2],"message_count":s[3],"created_dt":s[4],"last_active_dt":s[5]},
                "messages": [{"id":r[0],"role":r[1],"content":r[2],"created_dt":r[3],"response_ms":r[4]} for r in msgs]
            }
    except Exception as e:
        log.error(f"get_session_messages: {e}")
        return {}


def get_recent_for_llm(session_id: str, limit: int=20) -> List[Dict]:
    try:
        with _get_pool().acquire() as conn:
            rows = conn.fetchall(
                "SELECT ROLE,CONTENT FROM (SELECT ROLE,CONTENT,CREATED_DT FROM BI_CHAT_HISTORY WHERE SESSION_ID=:1 AND ROLE IN ('user','assistant') ORDER BY CREATED_DT DESC FETCH FIRST :2 ROWS ONLY) ORDER BY CREATED_DT ASC",
                [session_id, limit])
            return [{"role":r[0],"content":r[1]} for r in rows]
    except Exception as e:
        log.error(f"get_recent_for_llm: {e}")
        return []


# ── Audit ─────────────────────────────────────────────────────────────────────

def log_api(endpoint: str, method: str, user_id="", scorecard_id="",
            status=200, resp_ms=0, error="", ip=""):
    try:
        with _get_pool().acquire() as conn:
            conn.execute(
                "INSERT INTO BI_API_AUDIT(ENDPOINT,HTTP_METHOD,USER_ID,SCORECARD_ID,STATUS_CODE,RESPONSE_TIME_MS,ERROR_MESSAGE,IP_ADDRESS,CREATED_DT) VALUES(:1,:2,:3,:4,:5,:6,:7,:8,SYSTIMESTAMP)",
                [endpoint, method, user_id, scorecard_id, status, resp_ms, error[:2000], ip])
            conn.commit()
    except Exception as e:
        log.warning(f"log_api failed (non-critical): {e}")


def log_snapshot(view_id: str, scorecard_id: str, user_id: str,
                 ftype: str, success: bool, error="", size=0, ms=0):
    try:
        with _get_pool().acquire() as conn:
            conn.execute(
                "INSERT INTO BI_SNAPSHOT_LOG(VIEW_ID,SCORECARD_ID,USER_ID,FILE_TYPE,SUCCESS,ERROR_MESSAGE,FILE_SIZE_BYTES,RESPONSE_TIME_MS,CREATED_DT) VALUES(:1,:2,:3,:4,:5,:6,:7,:8,SYSTIMESTAMP)",
                [view_id, scorecard_id, user_id, ftype, 'Y' if success else 'N', error[:500], size, ms])
            conn.commit()
    except Exception as e:
        log.warning(f"log_snapshot failed (non-critical): {e}")


# ── Analytics ─────────────────────────────────────────────────────────────────

def get_analytics(days: int=30) -> Dict:
    try:
        with _get_pool().acquire() as conn:
            summ = conn.fetchone(
                f"SELECT COUNT(DISTINCT s.SESSION_ID),COUNT(h.ID),COUNT(DISTINCT s.USER_ID),AVG(h.RESPONSE_TIME_MS) FROM BI_CHAT_SESSION s LEFT JOIN BI_CHAT_HISTORY h ON s.SESSION_ID=h.SESSION_ID WHERE s.CREATED_DT>=SYSTIMESTAMP-INTERVAL '{days}' DAY") or (0,0,0,None)
            by_sc = conn.fetchall(
                f"SELECT s.SCORECARD_ID,COUNT(DISTINCT s.SESSION_ID),COUNT(h.ID),COUNT(DISTINCT s.USER_ID),AVG(h.RESPONSE_TIME_MS) FROM BI_CHAT_SESSION s LEFT JOIN BI_CHAT_HISTORY h ON s.SESSION_ID=h.SESSION_ID WHERE s.CREATED_DT>=SYSTIMESTAMP-INTERVAL '{days}' DAY GROUP BY s.SCORECARD_ID ORDER BY 2 DESC") or []
        return {
            "total_sessions": summ[0] or 0,
            "total_messages": summ[1] or 0,
            "unique_users":   summ[2] or 0,
            "avg_response_ms":round(summ[3],1) if summ[3] else None,
            "most_queried":   by_sc[0][0] if by_sc else None,
            "by_scorecard":   [{"scorecard_id":r[0],"sessions":r[1]or 0,"messages":r[2]or 0,"users":r[3]or 0,"avg_ms":round(r[4],1) if r[4] else None} for r in by_sc],
        }
    except Exception as e:
        log.error(f"get_analytics: {e}")
        return {"error": str(e)}


def ping() -> bool:
    try:
        with _get_pool().acquire() as conn:
            conn.fetchone("SELECT 1 FROM DUAL")
        return True
    except:
        return False
