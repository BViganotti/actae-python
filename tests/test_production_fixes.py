"""
Tests for CRITICAL production-readiness fixes in the Actae Python SDK.

Run: python3 sdks/python/tests/test_production_fixes.py
"""

import asyncio
import logging
from unittest.mock import AsyncMock, MagicMock, patch


#  Helper: create an async context manager from a mock 
class AsyncContextManagerMock:
    """Wraps a mock response so 'async with' works correctly."""

    def __init__(self, mock_resp):
        self._resp = mock_resp

    async def __aenter__(self):
        return self._resp

    async def __aexit__(self, *args):
        pass


#  Test #5: _dispatch handles exceptions 

def test_dispatch_handles_exception():
    from actae_client.client import ActaeClient

    client = ActaeClient(api_key="sk-test", ws_endpoint="ws://localhost:9999/ws")

    malformed = {"type": "broadcast", "topic": "test", "payload": None}

    async def run():
        def failing_cb(topic, event):
            raise ValueError("Simulated dispatch failure")

        client._on_message_cb = failing_cb
        client._authenticated = True
        client._connected = True

        await client._dispatch(malformed)
        assert True, "Dispatch handled exception without crashing"

    asyncio.run(run())


#  Test #6: _request handles text content type 

def test_request_handles_text_content_type():
    from actae_client.client import ActaeClient

    client = ActaeClient(api_key="sk-test", ws_endpoint="ws://localhost:9999/ws")

    async def run():
        mock_resp = MagicMock()
        mock_resp.status = 200
        mock_resp.headers = {"Content-Type": "text/plain; version=0.0.4"}
        mock_resp.text = AsyncMock(return_value="# HELP actae_events_total\n# TYPE actae_events_total counter\nactae_events_total 42\n")

        mock_session = MagicMock()
        mock_session.closed = False
        mock_session.request = MagicMock(return_value=AsyncContextManagerMock(mock_resp))

        client._session = mock_session

        result = await client._request("GET", "/metrics")
        assert isinstance(result, str), f"Expected str, got {type(result)}"
        assert "actae_events_total" in result
        assert "42" in result

    asyncio.run(run())


def test_request_handles_json_content_type():
    from actae_client.client import ActaeClient

    client = ActaeClient(api_key="sk-test", ws_endpoint="ws://localhost:9999/ws")

    async def run():
        mock_resp = MagicMock()
        mock_resp.status = 200
        mock_resp.headers = {"Content-Type": "application/json"}
        mock_resp.json = AsyncMock(return_value={"status": "ok", "events": []})

        mock_session = MagicMock()
        mock_session.closed = False
        mock_session.request = MagicMock(return_value=AsyncContextManagerMock(mock_resp))

        client._session = mock_session

        result = await client._request("GET", "/api/v1/channels")
        assert isinstance(result, dict)
        assert result["status"] == "ok"

    asyncio.run(run())


#  Test #7: ClientSession cleanup on WS connect failure 

def test_do_connect_cleans_up_session_on_failure():
    from actae_client.client import ActaeClient
    from actae_client.errors import ActaeConnectionError as ActaeActaeConnectionError

    client = ActaeClient(api_key="sk-test", endpoint="http://localhost:1", ws_endpoint="ws://localhost:1/ws")

    async def run():
        try:
            await client._do_connect()
            assert False, "Should have raised ActaeConnectionError"
        except ActaeActaeConnectionError:
            assert client._session is None, f"Session should be None after failed connect"
        except Exception as exc:
            assert False, f"Should raise ActaeActaeConnectionError, got {type(exc).__name__}: {exc}"

    asyncio.run(run())


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    test_dispatch_handles_exception()
    print("  test_dispatch_handles_exception")
    test_request_handles_text_content_type()
    print("  test_request_handles_text_content_type")
    test_request_handles_json_content_type()
    print("  test_request_handles_json_content_type")
    test_do_connect_cleans_up_session_on_failure()
    print("  test_do_connect_cleans_up_session_on_failure")
    print("\n  All Python production-readiness tests passed!")
