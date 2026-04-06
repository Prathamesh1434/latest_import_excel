"""
tests/test_all.py
Run: conda activate prath && cd bi-final && python -m pytest tests/ -v
"""
import sys, os, json, time
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import pytest
from unittest.mock import patch, MagicMock


# ══════════════════════════════════════
# 1. CONFIG
# ══════════════════════════════════════
class TestConfig:
    def test_metadata_dir_exists(self):
        from backend.config import METADATA_DIR
        assert METADATA_DIR.exists()

    def test_check_functions_are_callable(self):
        from backend.config import check_tableau, check_vertex, check_oracle
        assert all(callable(f) for f in [check_tableau, check_vertex, check_oracle])


# ══════════════════════════════════════
# 2. CONTEXT LOADER
# ══════════════════════════════════════
class TestContextLoader:
    def test_list_scorecards(self):
        from backend.context.loader import list_scorecards
        result = list_scorecards()
        assert isinstance(result, list)
        assert "_template" not in result

    def test_unknown_scorecard_prompt(self):
        from backend.context.loader import build_system_prompt
        p = build_system_prompt("does-not-exist")
        assert "does-not-exist" in p

    def test_known_scorecard_prompt(self):
        from backend.context.loader import build_system_prompt, list_scorecards
        scs = list_scorecards()
        if not scs: pytest.skip("No YAML files")
        p = build_system_prompt(scs[0])
        assert len(p) > 100
        assert "RULES" in p

    def test_csv_injection(self):
        from backend.context.loader import build_system_prompt, list_scorecards
        scs = list_scorecards()
        if not scs: pytest.skip("No YAML files")
        csv = b"KRI_ID,VALUE,RAG\nUK-K41,0.08,Red\n"
        p = build_system_prompt(scs[0], csv_bytes=csv)
        assert "LIVE" in p or "CSV" in p

    def test_bad_csv_doesnt_crash(self):
        from backend.context.loader import build_system_prompt, list_scorecards
        scs = list_scorecards()
        if not scs: pytest.skip("No YAML files")
        p = build_system_prompt(scs[0], csv_bytes=b"\xff\xfe bad data")
        assert isinstance(p, str)


# ══════════════════════════════════════
# 3. SCHEMAS
# ══════════════════════════════════════
class TestSchemas:
    def test_valid_chat_request(self):
        from backend.models.schemas import ChatRequest, ChatMessage
        req = ChatRequest(scorecard_id="uk-kri",
                          messages=[ChatMessage(role="user", content="hello")])
        assert req.scorecard_id == "uk-kri"

    def test_empty_messages_rejected(self):
        from backend.models.schemas import ChatRequest
        with pytest.raises(Exception):
            ChatRequest(scorecard_id="uk-kri", messages=[])

    def test_invalid_role_rejected(self):
        from backend.models.schemas import ChatMessage
        with pytest.raises(Exception):
            ChatMessage(role="system", content="hello")

    def test_empty_content_rejected(self):
        from backend.models.schemas import ChatMessage
        with pytest.raises(Exception):
            ChatMessage(role="user", content="")

    def test_max_tokens_bounds(self):
        from backend.models.schemas import ChatRequest, ChatMessage
        msg = [ChatMessage(role="user", content="test")]
        with pytest.raises(Exception):
            ChatRequest(scorecard_id="uk-kri", messages=msg, max_tokens=10)
        with pytest.raises(Exception):
            ChatRequest(scorecard_id="uk-kri", messages=msg, max_tokens=9999)

    def test_chart_data_model(self):
        from backend.models.schemas import ChartData
        c = ChartData(chart_type="bar", title="Test", labels=["A","B"], datasets=[])
        assert c.chart_type == "bar"

    def test_chat_response_model(self):
        from backend.models.schemas import ChatResponse
        r = ChatResponse(reply="ok", session_id="abc", scorecard_id="uk-kri", model="gemini")
        assert r.reply == "ok"
        assert r.chart is None


# ══════════════════════════════════════
# 4. TABLEAU SERVICE (mocked)
# ══════════════════════════════════════
class TestTableauService:
    @patch("backend.services.tableau_service._get_server")
    def test_get_image_returns_bytes(self, mock_srv):
        from backend.services.tableau_service import get_view_image, _cache
        _cache.clear()
        v = MagicMock(); v.image = b"\x89PNG"
        s = MagicMock(); s.views.get_by_id.return_value = v
        mock_srv.return_value = s
        with patch("backend.config.TABLEAU_SERVER","https://x"), \
             patch("backend.config.TABLEAU_USERNAME","u"), \
             patch("backend.config.TABLEAU_PASSWORD","p"):
            data, ms = get_view_image("view1")
        assert isinstance(data, bytes)
        s.auth.sign_out.assert_called_once()

    @patch("backend.services.tableau_service._get_server")
    def test_cache_hit_skips_server(self, mock_srv):
        from backend.services.tableau_service import get_view_image, _cache, _cache_key
        _cache[_cache_key("view-cached","PNG")] = (b"cached", time.time())
        data, ms = get_view_image("view-cached")
        assert data == b"cached"
        assert ms == 0
        mock_srv.assert_not_called()

    @patch("backend.services.tableau_service._get_server")
    def test_retry_on_failure(self, mock_srv):
        from backend.services.tableau_service import get_view_image, _cache
        _cache.clear()
        fail = MagicMock(); fail.views.get_by_id.side_effect = Exception("timeout")
        ok_v = MagicMock(); ok_v.image = b"retry ok"
        ok_s = MagicMock(); ok_s.views.get_by_id.return_value = ok_v
        mock_srv.side_effect = [fail, ok_s]
        with patch("backend.config.TABLEAU_SERVER","https://x"), \
             patch("backend.config.TABLEAU_USERNAME","u"), \
             patch("backend.config.TABLEAU_PASSWORD","p"), \
             patch("time.sleep"):
            data, _ = get_view_image("view-retry")
        assert data == b"retry ok"

    def test_cache_stats_shape(self):
        from backend.services.tableau_service import cache_stats
        s = cache_stats()
        assert "total" in s and "valid" in s and "ttl_sec" in s


# ══════════════════════════════════════
# 5. ORACLE SERVICE (mocked)
# ══════════════════════════════════════
class TestOracleService:
    @patch("backend.services.oracle_service._get_pool")
    def test_create_session_returns_uuid(self, mock_pool):
        from backend.services.oracle_service import get_or_create_session
        conn = MagicMock()
        conn.__enter__ = MagicMock(return_value=conn)
        conn.__exit__  = MagicMock(return_value=False)
        mock_pool.return_value.acquire.return_value = conn
        sid = get_or_create_session("user","sc","name")
        assert len(sid) == 36

    @patch("backend.services.oracle_service._get_pool")
    def test_db_failure_still_returns_id(self, mock_pool):
        from backend.services.oracle_service import get_or_create_session
        mock_pool.side_effect = Exception("DB down")
        sid = get_or_create_session("u","sc","n")
        assert isinstance(sid, str) and len(sid) > 0

    @patch("backend.services.oracle_service._get_pool")
    def test_audit_log_silent_failure(self, mock_pool):
        from backend.services.oracle_service import log_api
        mock_pool.side_effect = Exception("down")
        log_api("/test/","GET")  # must not raise

    @patch("backend.services.oracle_service._get_pool")
    def test_snapshot_log_silent_failure(self, mock_pool):
        from backend.services.oracle_service import log_snapshot
        mock_pool.side_effect = Exception("down")
        log_snapshot("v","sc","u","PNG",True)  # must not raise

    @patch("backend.services.oracle_service._get_pool")
    def test_ping_false_on_error(self, mock_pool):
        from backend.services.oracle_service import ping
        mock_pool.side_effect = Exception("down")
        assert ping() is False


# ══════════════════════════════════════
# 6. VERTEX SERVICE (mocked)
# ══════════════════════════════════════
class TestVertexService:
    @patch("backend.services.vertex_service.GenerativeModel")
    def test_chat_returns_correct_tuple(self, MockModel):
        import backend.services.vertex_service as vs
        vs._init = True
        mock_resp = MagicMock()
        mock_resp.text = json.dumps({
            "reply": "UK-K41 is at 8%.",
            "chart_type": "kpi",
            "chart": {"title":"KRI","kpis":[{"label":"UK-K41","value":"8%"}]}
        })
        mock_resp.usage_metadata.prompt_token_count = 80
        mock_resp.usage_metadata.candidates_token_count = 40
        session = MagicMock()
        session.send_message.return_value = mock_resp
        MockModel.return_value.start_chat.return_value = session
        reply, chart, in_t, out_t, ms = vs.chat(
            "system", [{"role":"user","content":"UK-K41?"}]
        )
        assert reply == "UK-K41 is at 8%."
        assert chart is not None
        assert chart.get("chart_type") == "kpi"
        assert in_t == 80 and out_t == 40

    @patch("backend.services.vertex_service.GenerativeModel")
    def test_empty_messages_raises(self, _):
        import backend.services.vertex_service as vs
        vs._init = True
        with pytest.raises(ValueError):
            vs.chat("system", [])

    @patch("backend.services.vertex_service.GenerativeModel")
    def test_bad_json_returns_text(self, MockModel):
        import backend.services.vertex_service as vs
        vs._init = True
        mock_resp = MagicMock()
        mock_resp.text = "This is not JSON at all"
        mock_resp.usage_metadata.prompt_token_count = 10
        mock_resp.usage_metadata.candidates_token_count = 5
        session = MagicMock()
        session.send_message.return_value = mock_resp
        MockModel.return_value.start_chat.return_value = session
        reply, chart, _, _, _ = vs.chat("system", [{"role":"user","content":"hello"}])
        assert isinstance(reply, str)
        assert chart is None

    @patch("backend.services.vertex_service.GenerativeModel")
    def test_retry_on_failure(self, MockModel):
        import backend.services.vertex_service as vs
        vs._init = True
        fail_s = MagicMock(); fail_s.send_message.side_effect = Exception("quota")
        ok_resp = MagicMock()
        ok_resp.text = json.dumps({"reply":"ok","chart_type":"text","chart":None})
        ok_resp.usage_metadata.prompt_token_count=5
        ok_resp.usage_metadata.candidates_token_count=3
        ok_s = MagicMock(); ok_s.send_message.return_value = ok_resp
        MockModel.return_value.start_chat.side_effect = [fail_s, ok_s]
        with patch("time.sleep"):
            reply, _, _, _, _ = vs.chat("s",[{"role":"user","content":"hi"}])
        assert reply == "ok"


# ══════════════════════════════════════
# 7. FASTAPI ENDPOINTS (TestClient)
# ══════════════════════════════════════
class TestAPI:
    @pytest.fixture(scope="class")
    def client(self):
        from fastapi.testclient import TestClient
        from backend.main import app
        return TestClient(app)

    def test_root_200(self, client):
        r = client.get("/")
        assert r.status_code == 200
        assert "service" in r.json()

    def test_health_structure(self, client):
        r = client.get("/health/")
        assert r.status_code == 200
        d = r.json()
        assert "status" in d
        names = [s["name"] for s in d["services"]]
        assert "Tableau" in names and "VertexAI" in names and "Oracle" in names

    def test_health_has_cache_stats(self, client):
        r = client.get("/health/")
        assert "cache" in r.json()

    def test_snapshot_no_tableau_503(self, client):
        with patch("backend.config.check_tableau", return_value=False):
            r = client.get("/snapshot/fake-id")
            assert r.status_code == 503

    def test_chat_no_vertex_503(self, client):
        with patch("backend.config.check_vertex", return_value=False):
            r = client.post("/chat/", json={
                "scorecard_id":"uk-kri",
                "messages":[{"role":"user","content":"hello"}]
            })
            assert r.status_code == 503

    def test_chat_empty_messages_422(self, client):
        r = client.post("/chat/", json={"scorecard_id":"uk-kri","messages":[]})
        assert r.status_code == 422

    def test_history_no_oracle_graceful(self, client):
        with patch("backend.config.check_oracle", return_value=False):
            r = client.get("/history/sessions")
            assert r.status_code == 200
            assert r.json()["sessions"] == []

    def test_analytics_no_oracle_graceful(self, client):
        with patch("backend.config.check_oracle", return_value=False):
            r = client.get("/analytics/summary")
            assert r.status_code == 200

    def test_scorecards_list(self, client):
        r = client.get("/scorecards/")
        assert r.status_code == 200
        assert "scorecards" in r.json()

    def test_cache_stats_endpoint(self, client):
        r = client.get("/cache/stats")
        assert r.status_code == 200

    def test_timing_header(self, client):
        r = client.get("/health/")
        assert "x-response-ms" in r.headers

    def test_docs_accessible(self, client):
        r = client.get("/docs")
        assert r.status_code == 200


if __name__ == "__main__":
    import subprocess
    r = subprocess.run(["python","-m","pytest",__file__,"-v","--tb=short"],
                       cwd=os.path.join(os.path.dirname(__file__),".."))
    sys.exit(r.returncode)
