"""Tests for the HTTP layer in server.py.

Before these existed the request handler had no coverage at all, which is how
an unauthenticated `POST /api/alerts/config` (rewrites the Telegram bot token)
shipped to a public deployment.
"""

from __future__ import annotations

import json
import sys
import threading
import urllib.error
import urllib.request
from http import HTTPStatus
from http.server import ThreadingHTTPServer
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import server


@pytest.fixture(scope="module")
def live_server(tmp_path_factory):
    # Point the handler's store/service at a throwaway wallets file so these
    # tests never touch the real data/ directory tracked in this repo.
    tmp_dir = tmp_path_factory.mktemp("http_api_data")
    test_store = server.WalletStore(tmp_dir / "tracked_wallets.json")
    test_service = server.WalletTrackerService(test_store, server.HyperliquidClient())

    original_store = server.AppHandler.store
    original_service = server.AppHandler.service
    server.AppHandler.store = test_store
    server.AppHandler.service = test_service

    httpd = ThreadingHTTPServer(("127.0.0.1", 0), server.AppHandler)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    host, port = httpd.server_address[:2]
    try:
        yield f"http://{host}:{port}"
    finally:
        httpd.shutdown()
        httpd.server_close()
        thread.join(timeout=5)
        server.AppHandler.store = original_store
        server.AppHandler.service = original_service


def request(
    base: str,
    path: str,
    *,
    method: str = "GET",
    body: bytes | None = None,
    headers: dict[str, str] | None = None,
):
    req = urllib.request.Request(base + path, data=body, method=method)
    for key, value in (headers or {}).items():
        req.add_header(key, value)
    try:
        with urllib.request.urlopen(req, timeout=10) as response:
            return response.status, response.read(), dict(response.headers)
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read(), dict(exc.headers)


@pytest.fixture()
def token(monkeypatch):
    value = "test-token-abc123"
    monkeypatch.setattr(server, "API_TOKEN", value)
    return value


class TestAuthentication:
    def test_health_is_public_even_with_a_token_set(self, live_server, token):
        status, body, _ = request(live_server, "/api/health")
        assert status == HTTPStatus.OK
        assert json.loads(body)["ok"] is True

    def test_loopback_is_trusted_when_no_token_is_configured(self, live_server, monkeypatch):
        monkeypatch.setattr(server, "API_TOKEN", "")
        monkeypatch.setattr(server, "ALLOW_UNAUTHENTICATED_LOOPBACK", True)
        status, _, _ = request(live_server, "/api/wallets")
        assert status == HTTPStatus.OK

    def test_loopback_can_be_locked_down(self, live_server, monkeypatch):
        monkeypatch.setattr(server, "API_TOKEN", "")
        monkeypatch.setattr(server, "ALLOW_UNAUTHENTICATED_LOOPBACK", False)
        status, body, _ = request(live_server, "/api/wallets")
        assert status == HTTPStatus.SERVICE_UNAVAILABLE
        assert "HYPERWATCH_API_TOKEN" in json.loads(body)["error"]

    def test_missing_token_is_rejected(self, live_server, token):
        status, body, _ = request(live_server, "/api/wallets")
        assert status == HTTPStatus.UNAUTHORIZED
        assert "Unauthorized" in json.loads(body)["error"]

    def test_wrong_token_is_rejected(self, live_server, token):
        status, _, _ = request(
            live_server, "/api/wallets", headers={"Authorization": "Bearer wrong-token"}
        )
        assert status == HTTPStatus.UNAUTHORIZED

    def test_bearer_token_is_accepted(self, live_server, token):
        status, _, _ = request(
            live_server, "/api/wallets", headers={"Authorization": f"Bearer {token}"}
        )
        assert status == HTTPStatus.OK

    def test_x_api_token_header_is_accepted(self, live_server, token):
        status, _, _ = request(live_server, "/api/wallets", headers={"X-Api-Token": token})
        assert status == HTTPStatus.OK

    def test_cookie_is_accepted(self, live_server, token):
        status, _, _ = request(
            live_server,
            "/api/wallets",
            headers={"Cookie": f"{server.API_TOKEN_COOKIE}={token}"},
        )
        assert status == HTTPStatus.OK

    def test_malformed_cookie_header_does_not_crash(self, live_server, token):
        status, _, _ = request(live_server, "/api/wallets", headers={"Cookie": "=;;bad"})
        assert status == HTTPStatus.UNAUTHORIZED

    @pytest.mark.parametrize(
        ("method", "path"),
        [
            ("POST", "/api/alerts/config"),
            ("POST", "/api/wallets"),
            ("POST", "/api/wallets/import"),
            ("POST", "/api/alerts/check"),
            ("DELETE", "/api/wallets/0x" + "a" * 40),
        ],
    )
    def test_mutating_routes_require_a_token(self, live_server, token, method, path):
        status, _, _ = request(
            live_server, path, method=method, body=b"{}" if method == "POST" else None
        )
        assert status == HTTPStatus.UNAUTHORIZED


class TestTokenLogin:
    def test_valid_token_sets_an_httponly_cookie(self, live_server, token):
        status, _, headers = request(live_server, f"/api/session?token={token}")
        assert status == HTTPStatus.OK
        cookie = headers["Set-Cookie"]
        assert cookie.startswith(f"{server.API_TOKEN_COOKIE}={token}")
        assert "HttpOnly" in cookie
        assert "SameSite=Strict" in cookie

    def test_invalid_token_is_rejected(self, live_server, token):
        status, _, headers = request(live_server, "/api/session?token=nope")
        assert status == HTTPStatus.UNAUTHORIZED
        assert "Set-Cookie" not in headers

    def test_login_is_unavailable_without_a_configured_token(self, live_server, monkeypatch):
        monkeypatch.setattr(server, "API_TOKEN", "")
        status, _, _ = request(live_server, "/api/session?token=anything")
        assert status == HTTPStatus.SERVICE_UNAVAILABLE


class TestRequestBodies:
    def test_malformed_json_returns_400_not_500(self, live_server, token):
        status, body, _ = request(
            live_server,
            "/api/wallets",
            method="POST",
            body=b"{not json",
            headers={"Authorization": f"Bearer {token}"},
        )
        assert status == HTTPStatus.BAD_REQUEST
        assert "valid JSON" in json.loads(body)["error"]

    def test_non_object_json_is_rejected(self, live_server, token):
        status, body, _ = request(
            live_server,
            "/api/wallets",
            method="POST",
            body=b"[1, 2, 3]",
            headers={"Authorization": f"Bearer {token}"},
        )
        assert status == HTTPStatus.BAD_REQUEST
        assert "JSON object" in json.loads(body)["error"]

    def test_oversized_body_is_refused(self, live_server, token, monkeypatch):
        monkeypatch.setattr(server, "MAX_REQUEST_BODY_BYTES", 64)
        status, body, _ = request(
            live_server,
            "/api/wallets",
            method="POST",
            body=b'{"address": "' + b"a" * 500 + b'"}',
            headers={"Authorization": f"Bearer {token}"},
        )
        assert status == HTTPStatus.REQUEST_ENTITY_TOO_LARGE
        assert "byte limit" in json.loads(body)["error"]

    def test_invalid_address_returns_400(self, live_server, token):
        status, body, _ = request(
            live_server,
            "/api/wallets",
            method="POST",
            body=json.dumps({"address": "not-an-address"}).encode(),
            headers={"Authorization": f"Bearer {token}"},
        )
        assert status == HTTPStatus.BAD_REQUEST
        assert "42-character hex" in json.loads(body)["error"]

    def test_non_list_addresses_returns_400(self, live_server, token):
        status, body, _ = request(
            live_server,
            "/api/discovery/scan",
            method="POST",
            body=json.dumps({"addresses": "0xabc"}).encode(),
            headers={"Authorization": f"Bearer {token}"},
        )
        assert status == HTTPStatus.BAD_REQUEST
        assert "must be a list" in json.loads(body)["error"]

    def test_non_integer_limit_returns_400(self, live_server, token):
        status, body, _ = request(
            live_server,
            "/api/discovery/scan",
            method="POST",
            body=json.dumps({"addresses": [], "limit": "many"}).encode(),
            headers={"Authorization": f"Bearer {token}"},
        )
        assert status == HTTPStatus.BAD_REQUEST
        assert "must be an integer" in json.loads(body)["error"]


class TestRoutingAndHeaders:
    def test_unknown_api_route_returns_json_404(self, live_server, token):
        status, body, headers = request(
            live_server, "/api/nope", headers={"Authorization": f"Bearer {token}"}
        )
        assert status == HTTPStatus.NOT_FOUND
        assert headers["Content-Type"] == "application/json"
        assert json.loads(body)["error"] == "Route not found."

    def test_unknown_post_route_returns_json_404(self, live_server, token):
        status, _, _ = request(
            live_server,
            "/api/nope",
            method="POST",
            body=b"{}",
            headers={"Authorization": f"Bearer {token}"},
        )
        assert status == HTTPStatus.NOT_FOUND

    def test_security_headers_are_present(self, live_server):
        _, _, headers = request(live_server, "/api/health")
        assert headers["X-Content-Type-Options"] == "nosniff"
        assert headers["Cache-Control"] == "no-store"
        assert headers["Referrer-Policy"] == "no-referrer"

    def test_security_headers_are_present_on_static_responses(self, live_server):
        """The headers must not be a property of the JSON helper alone.

        They used to be sent only from send_json(), so every static file - the
        dashboard itself included - went out bare.
        """
        for path in ("/", "/app.js"):
            _, _, headers = request(live_server, path)
            assert headers["X-Content-Type-Options"] == "nosniff", path
            assert headers["Cache-Control"] == "no-store", path
            assert headers["Referrer-Policy"] == "no-referrer", path

    def test_security_headers_are_not_duplicated(self, live_server):
        """end_headers() adds them; send_json() must not add them again."""
        import http.client
        from urllib.parse import urlsplit

        parts = urlsplit(live_server)
        conn = http.client.HTTPConnection(parts.hostname, parts.port, timeout=5)
        try:
            conn.request("GET", "/api/health")
            response = conn.getresponse()
            response.read()
            assert response.headers.get_all("X-Content-Type-Options") == ["nosniff"]
        finally:
            conn.close()

    def test_static_index_is_served_without_a_token(self, live_server, token):
        status, body, _ = request(live_server, "/")
        assert status == HTTPStatus.OK
        assert b"<html" in body.lower()

    def test_directory_listing_is_disabled(self, live_server, tmp_path):
        handler = server.AppHandler.__new__(server.AppHandler)
        sent: dict = {}
        handler.send_error = lambda code, message=None: sent.update(code=code)
        assert handler.list_directory(str(tmp_path)) is None
        assert sent["code"] == HTTPStatus.NOT_FOUND

    def test_deleting_an_unknown_wallet_returns_404(self, live_server, token):
        status, body, _ = request(
            live_server,
            "/api/wallets/0x" + "b" * 40,
            method="DELETE",
            headers={"Authorization": f"Bearer {token}"},
        )
        assert status == HTTPStatus.NOT_FOUND
        assert json.loads(body)["error"] == "Wallet not found."

    def test_deleting_an_invalid_address_returns_400(self, live_server, token):
        status, body, _ = request(
            live_server,
            "/api/wallets/garbage",
            method="DELETE",
            headers={"Authorization": f"Bearer {token}"},
        )
        assert status == HTTPStatus.BAD_REQUEST
        assert "42-character hex" in json.loads(body)["error"]


class TestErrorHandling:
    def test_unhandled_service_error_returns_clean_500(self, live_server, token, monkeypatch):
        def boom() -> dict:
            raise RuntimeError("upstream exploded with secret=hunter2")

        monkeypatch.setattr(server.AppHandler.service, "dashboard", boom)
        status, body, _ = request(
            live_server, "/api/dashboard", headers={"Authorization": f"Bearer {token}"}
        )
        assert status == HTTPStatus.INTERNAL_SERVER_ERROR
        payload = json.loads(body)
        assert payload["error"] == "Internal server error."
        assert "hunter2" not in body.decode()


class TestTokenIsNotLogged:
    """A token in the query string must never reach the access log.

    /api/session takes the token as a query parameter, and the default
    log_message() writes the request line verbatim to stderr - so without
    redaction every login wrote the credential to stdout, journald, or the
    hosting platform's log aggregator.
    """

    @pytest.mark.parametrize(
        "line",
        [
            "GET /api/session?token=s3cr3t-value HTTP/1.1",
            "GET /login?token=s3cr3t-value&next=/ HTTP/1.1",
            "GET /api/session?api_token=s3cr3t-value HTTP/1.1",
        ],
    )
    def test_query_token_is_redacted(self, capsys, monkeypatch, line):
        # QUIET_HTTP short-circuits log_message entirely, and CI sets it, so
        # pin it off here: this test is about what gets written when logging
        # is on, not about whether the environment happens to enable it.
        monkeypatch.delenv("QUIET_HTTP", raising=False)
        handler = server.AppHandler.__new__(server.AppHandler)
        handler.client_address = ("127.0.0.1", 1)
        handler.requestline = line
        server.AppHandler.log_message(handler, '"%s" %s %s', line, "200", "-")
        logged = capsys.readouterr().err
        assert "s3cr3t-value" not in logged
        assert "[redacted]" in logged

    def test_ordinary_request_lines_are_untouched(self, capsys, monkeypatch):
        monkeypatch.delenv("QUIET_HTTP", raising=False)
        handler = server.AppHandler.__new__(server.AppHandler)
        handler.client_address = ("127.0.0.1", 1)
        line = "GET /api/dashboard HTTP/1.1"
        server.AppHandler.log_message(handler, '"%s" %s %s', line, "200", "-")
        assert "/api/dashboard" in capsys.readouterr().err

    def test_quiet_http_suppresses_the_line_entirely(self, capsys, monkeypatch):
        monkeypatch.setenv("QUIET_HTTP", "1")
        handler = server.AppHandler.__new__(server.AppHandler)
        handler.client_address = ("127.0.0.1", 1)
        line = "GET /api/session?token=s3cr3t-value HTTP/1.1"
        server.AppHandler.log_message(handler, '"%s" %s %s', line, "200", "-")
        assert capsys.readouterr().err == ""
