import builtins

import pytest

from actae_client import (
    ActaeError,
    AuthError,
    ActaeConnectionError,
    APIError,
    RateLimitError,
    SessionError,
    SessionCompletedError,
)


class TestConstructorValidation:
    def test_requires_api_key(self):
        from actae_client.client import ActaeClient

        with pytest.raises(ValueError, match="api_key is required"):
            ActaeClient(api_key="", endpoint="http://localhost:8002")

    def test_requires_endpoint_or_ws(self):
        from actae_client.client import ActaeClient

        with pytest.raises(ValueError, match="at least one of endpoint or ws_endpoint is required"):
            ActaeClient(api_key="sk-test")

    def test_endpoint_only_derives_ws(self):
        from actae_client.client import ActaeClient

        client = ActaeClient(api_key="sk-test", endpoint="http://localhost:8002")
        assert client._endpoint == "http://localhost:8002"
        # Actae serves WebSocket at /ws; derived from the HTTP endpoint.
        assert client._ws_endpoint == "ws://localhost:8002/ws"

    def test_https_endpoint_derives_wss(self):
        from actae_client.client import ActaeClient

        client = ActaeClient(api_key="sk-test", endpoint="https://api.actae.example.com")
        assert client._ws_endpoint == "wss://api.actae.example.com/ws"

    def test_explicit_ws_endpoint(self):
        from actae_client.client import ActaeClient

        client = ActaeClient(
            api_key="sk-test",
            endpoint="http://localhost:8002",
            ws_endpoint="wss://custom.example.com/ws",
        )
        assert client._ws_endpoint == "wss://custom.example.com/ws"

    def test_timeout_default(self):
        from actae_client.client import ActaeClient

        client = ActaeClient(api_key="sk-test", endpoint="http://localhost:8002")
        assert client._timeout == 30.0

    def test_custom_timeout(self):
        from actae_client.client import ActaeClient

        client = ActaeClient(api_key="sk-test", endpoint="http://localhost:8002", timeout=60.0)
        assert client._timeout == 60.0

    def test_auto_reconnect_default_true(self):
        from actae_client.client import ActaeClient

        client = ActaeClient(api_key="sk-test", endpoint="http://localhost:8002")
        assert client._auto_reconnect is True

    def test_auto_reconnect_false(self):
        from actae_client.client import ActaeClient

        client = ActaeClient(api_key="sk-test", endpoint="http://localhost:8002", auto_reconnect=False)
        assert client._auto_reconnect is False

    def test_ws_endpoint_only(self):
        from actae_client.client import ActaeClient

        client = ActaeClient(api_key="sk-test", ws_endpoint="ws://localhost:8002")
        assert client._ws_endpoint == "ws://localhost:8002"
        assert client._endpoint == ""


class TestErrorClasses:
    def test_actae_error_base(self):
        err = ActaeError("something broke")
        assert str(err) == "something broke"
        assert isinstance(err, Exception)

    def test_auth_error_default(self):
        err = AuthError()
        assert str(err) == "Authentication failed"
        assert isinstance(err, ActaeError)

    def test_auth_error_custom(self):
        err = AuthError("Invalid API key")
        assert str(err) == "Invalid API key"

    def test_connection_error_default(self):
        err = ActaeConnectionError()
        assert str(err) == "Connection failed"
        assert isinstance(err, ActaeError)

    def test_connection_error_custom(self):
        err = ActaeConnectionError("WebSocket handshake failed")
        assert str(err) == "WebSocket handshake failed"

    def test_api_error_with_message(self):
        err = APIError(404, "Channel not found")
        assert err.status_code == 404
        assert str(err) == "HTTP 404: Channel not found"

    def test_api_error_without_message(self):
        err = APIError(500)
        assert err.status_code == 500
        assert str(err) == "HTTP 500"

    def test_rate_limit_error_with_message(self):
        err = RateLimitError(retry_after_seconds=30, message="Too fast")
        assert err.retry_after_seconds == 30
        assert "30" in str(err)
        assert "Too fast" in str(err)

    def test_rate_limit_error_without_message(self):
        err = RateLimitError(retry_after_seconds=60)
        assert err.retry_after_seconds == 60
        assert "60" in str(err)

    def test_session_error(self):
        err = SessionError("Cannot step on completed session")
        assert str(err) == "Cannot step on completed session"

    def test_session_completed_error(self):
        err = SessionCompletedError("Session is done")
        assert str(err) == "Session is done"
        assert isinstance(err, SessionError)

    def test_rate_limit_error_negative_retry(self):
        err = RateLimitError(retry_after_seconds=-1)
        assert err.retry_after_seconds == -1


class TestActaeConnectionErrorDoesNotShadowBuiltin:
    def test_sdk_connection_error_is_actae_error(self):
        from actae_client import ActaeConnectionError

        assert ActaeConnectionError is not builtins.ConnectionError
        assert issubclass(ActaeConnectionError, ActaeError)

    def test_connection_error_not_exported(self):
        import actae_client

        assert not hasattr(actae_client, "ConnectionError")

    def test_builtin_connection_error_unaffected(self):
        try:
            raise builtins.ConnectionError("test")
        except builtins.ConnectionError:
            pass


class TestExecutionErrors:
    def test_idempotency_key_mismatch_error(self):
        from actae_client import IdempotencyKeyMismatchError
        err = IdempotencyKeyMismatchError()
        assert str(err).startswith("Idempotency key")

    def test_execution_not_owned_error(self):
        from actae_client import ExecutionNotOwnedError
        err = ExecutionNotOwnedError("stale token")
        assert str(err) == "stale token"

    def test_execution_not_found_error(self):
        from actae_client import ExecutionNotFoundError
        err = ExecutionNotFoundError()
        assert str(err).startswith("Execution not found")

    def test_execution_errors_are_actae_errors(self):
        from actae_client import (
            ExecutionNotFoundError,
            ExecutionNotOwnedError,
            IdempotencyKeyMismatchError,
            ActaeError,
        )
        assert issubclass(IdempotencyKeyMismatchError, ActaeError)
        assert issubclass(ExecutionNotOwnedError, ActaeError)
        assert issubclass(ExecutionNotFoundError, ActaeError)
