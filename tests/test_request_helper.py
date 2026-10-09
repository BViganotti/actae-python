"""Direct tests of the _request HTTP helper — the core engine of the SDK.

Tests mock aiohttp at the session level to verify the _request method
handles all content types, error responses, status codes, and edge cases
correctly. No server required.
"""

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from actae_client.errors import APIError, RateLimitError


class AsyncContextManagerMock:
    """Wraps a mock response so 'async with' works correctly."""

    def __init__(self, mock_resp):
        self._resp = mock_resp

    async def __aenter__(self):
        return self._resp

    async def __aexit__(self, *args):
        pass


def _make_request_client():
    """Create a bare ActaeClient and wire up a mock aiohttp session."""
    from actae_client.client import ActaeClient

    client = ActaeClient(api_key="sk-test", endpoint="http://localhost:8002")
    mock_session = MagicMock()
    mock_session.closed = False
    client._session = mock_session
    return client, mock_session


def _make_response(status=200, headers=None, body=None, json_data=None):
    """Create a mock aiohttp response."""
    mock_resp = MagicMock()
    mock_resp.status = status
    mock_resp.headers = headers or {"Content-Type": "application/json"}
    if json_data is not None:
        mock_resp.json = AsyncMock(return_value=json_data)
        mock_resp.text = AsyncMock(return_value=json.dumps(json_data))
    elif body is not None:
        mock_resp.text = AsyncMock(return_value=body)
        mock_resp.json = AsyncMock(side_effect=json.JSONDecodeError("", "", 0))
    else:
        mock_resp.json = AsyncMock(return_value={})
        mock_resp.text = AsyncMock(return_value="{}")
    return mock_resp


class TestRequestContentTypes:
    def test_application_json(self):
        client, session = _make_request_client()
        resp = _make_response(json_data={"status": "ok"})
        session.request = MagicMock(return_value=AsyncContextManagerMock(resp))

        result = asyncio.run(client._request("GET", "/api/v1/channels"))
        assert result == {"status": "ok"}

    def test_text_plain(self):
        client, session = _make_request_client()
        resp = _make_response(
            headers={"Content-Type": "text/plain; version=0.0.4"},
            body="# HELP actae_events\n# TYPE actae_events counter\nactae_events_total 42\n",
        )
        session.request = MagicMock(return_value=AsyncContextManagerMock(resp))

        result = asyncio.run(client._request("GET", "/metrics"))
        assert isinstance(result, str)
        assert "actae_events_total" in result

    def test_text_html_is_treated_as_text(self):
        client, session = _make_request_client()
        resp = _make_response(
            headers={"Content-Type": "text/html"},
            body="<html><body>OK</body></html>",
        )
        session.request = MagicMock(return_value=AsyncContextManagerMock(resp))

        result = asyncio.run(client._request("GET", "/healthz"))
        assert isinstance(result, str)

    def test_application_text(self):
        client, session = _make_request_client()
        resp = _make_response(
            headers={"Content-Type": "application/text"},
            body="ok",
        )
        session.request = MagicMock(return_value=AsyncContextManagerMock(resp))

        result = asyncio.run(client._request("GET", "/healthz"))
        assert isinstance(result, str)

    def test_unknown_content_type_defaults_json(self):
        client, session = _make_request_client()
        resp = _make_response(
            headers={"Content-Type": "application/vnd.custom+json"},
            json_data={"key": "val"},
        )
        session.request = MagicMock(return_value=AsyncContextManagerMock(resp))

        result = asyncio.run(client._request("GET", "/api/v1/channels"))
        assert result == {"key": "val"}


class TestRequestErrors:
    def test_400_raises_api_error(self):
        client, session = _make_request_client()
        resp = _make_response(status=400, json_data={"error": "Bad Request"})
        session.request = MagicMock(return_value=AsyncContextManagerMock(resp))

        with pytest.raises(APIError) as exc:
            asyncio.run(client._request("GET", "/api/v1/channels"))
        assert exc.value.status_code == 400

    def test_404_with_message(self):
        client, session = _make_request_client()
        resp = _make_response(status=404, json_data={"error": "Channel not found"})
        session.request = MagicMock(return_value=AsyncContextManagerMock(resp))

        with pytest.raises(APIError) as exc:
            asyncio.run(client._request("GET", "/api/v1/channels/nonexistent"))
        assert "Channel not found" in str(exc.value)

    def test_500_raises_api_error(self):
        client, session = _make_request_client()
        resp = _make_response(status=500)
        session.request = MagicMock(return_value=AsyncContextManagerMock(resp))

        with pytest.raises(APIError) as exc:
            asyncio.run(client._request("GET", "/api/v1/channels"))
        assert exc.value.status_code == 500

    def test_429_raises_rate_limit_error(self):
        client, session = _make_request_client()
        resp = _make_response(status=429, json_data={"retry_after_seconds": 30})
        session.request = MagicMock(return_value=AsyncContextManagerMock(resp))

        with pytest.raises(RateLimitError) as exc:
            asyncio.run(client._request("GET", "/api/v1/channels"))
        assert exc.value.retry_after_seconds == 30

    def test_429_with_message(self):
        client, session = _make_request_client()
        resp = _make_response(
            status=429,
            json_data={"retry_after_seconds": 60, "error": "Too Many Requests"},
        )
        session.request = MagicMock(return_value=AsyncContextManagerMock(resp))

        with pytest.raises(RateLimitError) as exc:
            asyncio.run(client._request("GET", "/api/v1/channels"))
        assert exc.value.retry_after_seconds == 60
        assert "Too Many Requests" in str(exc.value)

    def test_text_error_response(self):
        """Some servers return plain text for errors (e.g. nginx)."""
        client, session = _make_request_client()
        resp = _make_response(status=502, headers={"Content-Type": "text/plain"}, body="Bad Gateway")
        session.request = MagicMock(return_value=AsyncContextManagerMock(resp))

        with pytest.raises(APIError) as exc:
            asyncio.run(client._request("GET", "/api/v1/channels"))
        assert exc.value.status_code == 502

    def test_503_raises_api_error(self):
        client, session = _make_request_client()
        resp = _make_response(status=503, json_data={"error": "Service Unavailable"})
        session.request = MagicMock(return_value=AsyncContextManagerMock(resp))

        with pytest.raises(APIError) as exc:
            asyncio.run(client._request("GET", "/healthz"))
        assert exc.value.status_code == 503

    def test_connection_refused_wrapped_as_connection_error(self):
        """Transport failures (connect refused, DNS, timeouts) must surface
        as ActaeConnectionError so `except ActaeError` catches everything."""
        from actae_client import ActaeConnectionError
        import aiohttp

        client, session = _make_request_client()
        session.request = MagicMock(
            side_effect=aiohttp.ClientConnectionError("localhost", "refused")
        )

        with pytest.raises(ActaeConnectionError) as exc:
            asyncio.run(client._request("GET", "/api/v1/channels"))
        assert "HTTP request to /api/v1/channels failed" in str(exc.value)

    def test_request_timeout_wrapped_as_connection_error(self):
        from actae_client import ActaeConnectionError

        client, session = _make_request_client()
        session.request = MagicMock(side_effect=asyncio.TimeoutError())

        with pytest.raises(ActaeConnectionError):
            asyncio.run(client._request("GET", "/api/v1/channels"))

    def test_premature_close_wrapped_as_connection_error(self):
        from actae_client import ActaeConnectionError
        import aiohttp

        client, session = _make_request_client()
        session.request = MagicMock(side_effect=aiohttp.ServerDisconnectedError())

        with pytest.raises(ActaeConnectionError):
            asyncio.run(client._request("GET", "/api/v1/channels"))

    def test_401_raises_auth_error(self):
        from actae_client import AuthError

        client, session = _make_request_client()
        resp = _make_response(status=401, json_data={"error": "Unauthorized"})
        session.request = MagicMock(return_value=AsyncContextManagerMock(resp))

        with pytest.raises(AuthError):
            asyncio.run(client._request("GET", "/api/v1/admin/stats"))


class TestRequestURLConstruction:
    def test_url_concatenation(self):
        client, session = _make_request_client()
        resp = _make_response(json_data={})
        session.request = MagicMock(return_value=AsyncContextManagerMock(resp))

        asyncio.run(client._request("POST", "/api/v1/events/record", json={"key": "val"}))
        call_url = session.request.call_args[0][1] if len(session.request.call_args[0]) > 1 else session.request.call_args[1].get("url", "")
        assert call_url == "http://localhost:8002/api/v1/events/record"

    def test_url_with_custom_endpoint(self):
        from actae_client.client import ActaeClient

        client = ActaeClient(api_key="sk-test", endpoint="https://api.actae.io")
        mock_session = MagicMock()
        mock_session.closed = False
        client._session = mock_session
        resp = _make_response(json_data={})
        mock_session.request = MagicMock(return_value=AsyncContextManagerMock(resp))

        asyncio.run(client._request("GET", "/healthz"))
        call_url = mock_session.request.call_args[0][1] if len(mock_session.request.call_args[0]) > 1 else mock_session.request.call_args[1].get("url", "")
        assert call_url == "https://api.actae.io/healthz"


class TestRequestHeaders:
    def test_api_key_header(self):
        client, session = _make_request_client()
        resp = _make_response(json_data={})
        session.request = MagicMock(return_value=AsyncContextManagerMock(resp))

        asyncio.run(client._request("GET", "/healthz"))
        headers = session.request.call_args[1].get("headers", {})
        assert headers.get("x-api-key") == "sk-test"

    def test_custom_headers_merged(self):
        client, session = _make_request_client()
        resp = _make_response(json_data={})
        session.request = MagicMock(return_value=AsyncContextManagerMock(resp))

        asyncio.run(client._request("GET", "/healthz", headers={"authorization": "Bearer jwt-token"}))
        headers = session.request.call_args[1].get("headers", {})
        assert headers.get("authorization") == "Bearer jwt-token"
        assert headers.get("x-api-key") == "sk-test"


class TestRequestTimeout:
    def test_timeout_passed(self):
        client, session = _make_request_client()
        client._timeout = 15.0
        resp = _make_response(json_data={})
        session.request = MagicMock(return_value=AsyncContextManagerMock(resp))

        asyncio.run(client._request("GET", "/healthz"))
        timeout = session.request.call_args[1].get("timeout")
        assert timeout is not None
        assert timeout.total == 15.0

    def test_default_timeout(self):
        client, session = _make_request_client()
        resp = _make_response(json_data={})
        session.request = MagicMock(return_value=AsyncContextManagerMock(resp))

        asyncio.run(client._request("GET", "/healthz"))
        timeout = session.request.call_args[1].get("timeout")
        assert timeout.total == 30.0


class TestRequestSessionLifecycle:
    def test_creates_session_if_none(self):
        from actae_client.client import ActaeClient

        client = ActaeClient(api_key="sk-test", endpoint="http://localhost:8002")
        assert client._session is None

        resp = _make_response(json_data={})
        with patch("aiohttp.ClientSession") as mock_session_cls:
            mock_session = MagicMock()
            mock_session.closed = False
            mock_session_cls.return_value = mock_session
            mock_session.request = MagicMock(return_value=AsyncContextManagerMock(resp))

            asyncio.run(client._request("GET", "/healthz"))
            mock_session_cls.assert_called_once()

    def test_reuses_existing_session(self):
        client, session = _make_request_client()
        resp = _make_response(json_data={})
        session.request = MagicMock(return_value=AsyncContextManagerMock(resp))

        asyncio.run(client._request("GET", "/healthz"))
        asyncio.run(client._request("GET", "/healthz"))
        assert session.request.call_count == 2


class TestRequestMethods:
    def test_get(self):
        client, session = _make_request_client()
        resp = _make_response(json_data={})
        session.request = MagicMock(return_value=AsyncContextManagerMock(resp))

        asyncio.run(client._request("GET", "/healthz"))
        assert session.request.call_args[0][0] == "GET"

    def test_post(self):
        client, session = _make_request_client()
        resp = _make_response(json_data={})
        session.request = MagicMock(return_value=AsyncContextManagerMock(resp))

        asyncio.run(client._request("POST", "/api/v1/events/record", json={"key": "val"}))
        assert session.request.call_args[0][0] == "POST"

    def test_delete(self):
        client, session = _make_request_client()
        resp = _make_response(json_data={})
        session.request = MagicMock(return_value=AsyncContextManagerMock(resp))

        asyncio.run(client._request("DELETE", "/api/v1/state/ch-1/version/1"))
        assert session.request.call_args[0][0] == "DELETE"

    def test_put(self):
        client, session = _make_request_client()
        resp = _make_response(json_data={})
        session.request = MagicMock(return_value=AsyncContextManagerMock(resp))

        asyncio.run(client._request("PUT", "/api/v1/channels/ch-1/metadata", json={}))
        assert session.request.call_args[0][0] == "PUT"


class TestSSLMtlsConfig:
    def test_ssl_context_passed_to_connector(self):
        import ssl

        ctx = ssl.create_default_context()
        from actae_client.client import ActaeClient

        client = ActaeClient(api_key="sk-test", endpoint="http://localhost:8002", ssl_context=ctx)
        assert client._ssl is ctx

    def test_no_ssl_by_default(self):
        from actae_client.client import ActaeClient

        client = ActaeClient(api_key="sk-test", endpoint="http://localhost:8002")
        assert client._ssl is None

    def test_ca_cert_creates_context(self):
        from actae_client.client import ActaeClient

        with patch("ssl.create_default_context") as mock_ctx:
            client = ActaeClient(api_key="sk-test", endpoint="http://localhost:8002", ca_cert="/path/to/ca.pem")
            mock_ctx.assert_called_once_with(cafile="/path/to/ca.pem")
            assert client._ssl is mock_ctx.return_value

    def test_mtls_with_cert_and_key(self):
        from actae_client.client import ActaeClient

        with patch("ssl.create_default_context") as mock_ctx:
            mock_ctx.return_value.load_cert_chain = MagicMock()
            client = ActaeClient(
                api_key="sk-test", endpoint="http://localhost:8002",
                client_cert="/path/to/cert.pem", client_key="/path/to/key.pem",
            )
            mock_ctx.return_value.load_cert_chain.assert_called_once_with(
                "/path/to/cert.pem", "/path/to/key.pem",
            )

    def test_mtls_and_ca_cert(self):
        from actae_client.client import ActaeClient

        with patch("ssl.create_default_context") as mock_ctx:
            mock_ctx.return_value.load_cert_chain = MagicMock()
            client = ActaeClient(
                api_key="sk-test", endpoint="http://localhost:8002",
                client_cert="/path/to/cert.pem", ca_cert="/path/to/ca.pem",
            )
            mock_ctx.assert_called_once_with(cafile="/path/to/ca.pem")


class TestRequestResponseParsing:
    def test_empty_json_response(self):
        client, session = _make_request_client()
        resp = _make_response(json_data={})
        session.request = MagicMock(return_value=AsyncContextManagerMock(resp))

        result = asyncio.run(client._request("GET", "/healthz"))
        assert result == {}

    def test_list_json_response(self):
        client, session = _make_request_client()
        resp = _make_response(json_data=[1, 2, 3])
        session.request = MagicMock(return_value=AsyncContextManagerMock(resp))

        result = asyncio.run(client._request("GET", "/api/v1/channels"))
        assert result == [1, 2, 3]

    def test_null_response(self):
        client, session = _make_request_client()
        resp = _make_response(body="null")
        resp.json = AsyncMock(return_value=None)
        session.request = MagicMock(return_value=AsyncContextManagerMock(resp))

        result = asyncio.run(client._request("GET", "/api/v1/channels/nonexistent"))
        assert result is None
