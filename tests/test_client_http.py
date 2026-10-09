import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from actae_client import (
    Event,
    HealthStatus,
    ReadinessResult,
    MetricsSnapshot,
    AuthResult,
    UserInfo,
    ChannelMetadata,
    ForkInfo,
    ForkReceipt,
)


def _make_client(endpoint="http://localhost:8002"):
    from actae_client.client import ActaeClient

    return ActaeClient(
        api_key="sk-test",
        endpoint=endpoint,
        ws_endpoint="ws://localhost:8002",
    )


def _mock_request(client, return_value=None, side_effect=None):
    """Replace _request with a mock that returns the given value."""
    mock = AsyncMock(return_value=return_value, side_effect=side_effect)
    client._request = mock
    return mock


class TestRecord:
    def test_record_basic(self):
        client = _make_client()
        mock = _mock_request(client, return_value={"event": {
            "id": "evt-1", "channel_id": "ch-1", "type": "test.event",
            "payload": {"msg": "hello"}, "actor": "agent-1",
            "cursor": 42, "channel_cursor": 5, "timestamp": "2026-01-01T00:00:00Z",
        }})
        result = asyncio.run(client.record("ch-1", "test.event", {"msg": "hello"}, actor="agent-1"))
        assert isinstance(result, Event)
        assert result.id == "evt-1"
        assert result.cursor == 42
        call_kwargs = mock.call_args[1]
        assert call_kwargs["json"]["channel_id"] == "ch-1"
        assert call_kwargs["json"]["event_type"] == "test.event"
        assert call_kwargs["json"]["metadata"]["actor"] == "agent-1"


    def test_record_with_all_metadata(self):
        client = _make_client()
        mock = _mock_request(client, return_value={"event": {
            "id": "evt-1", "channel_id": "ch-1", "type": "t",
            "payload": {}, "actor": "a", "cursor": 1, "channel_cursor": 1, "timestamp": "",
        }})
        asyncio.run(client.record(
            "ch-1", "t", {"key": "val"},
            actor="agent-1", agent_id="ag-1", user_id="u-1",
            metadata={"source": "test"},
        ))
        meta = mock.call_args[1]["json"]["metadata"]
        assert meta["agent_id"] == "ag-1"
        assert meta["user_id"] == "u-1"
        assert meta["metadata"] == {"source": "test"}


    def test_record_with_operation_id(self):
        client = _make_client()
        mock = _mock_request(client, return_value={"event": {
            "id": "evt-1", "channel_id": "ch-1", "type": "t",
            "payload": {}, "actor": "a", "cursor": 1, "channel_cursor": 1, "timestamp": "",
        }})
        asyncio.run(client.record("ch-1", "t", {}, actor="a", operation_id="op-1"))
        body = mock.call_args[1]["json"]
        assert body["operation_id"] == "op-1"

        # Omitted operation_id must not appear in the request body.
        asyncio.run(client.record("ch-1", "t", {}, actor="a"))
        assert "operation_id" not in mock.call_args[1]["json"]

    def test_record_fills_metadata_on_returned_event(self):
        client = _make_client()
        _mock_request(client, return_value={"event": {
            "id": "evt-1", "channel_id": "ch-1", "type": "t",
            "payload": {}, "actor": "a", "cursor": 1, "channel_cursor": 1, "timestamp": "",
        }})
        ev = asyncio.run(client.record(
            "ch-1", "t", {},
            actor="a", agent_id="ag-1", user_id="u-1", metadata={"src": "x"},
        ))
        # The record response omits these fields; the SDK fills them from
        # what was sent (Go SDK parity).
        assert ev.metadata == {"src": "x"}
        assert ev.agent_id == "ag-1"
        assert ev.user_id == "u-1"


class TestReplay:
    def test_replay_basic(self):
        client = _make_client()
        mock = _mock_request(client, return_value={"events": [
            {"id": "e1", "channel_id": "ch-1", "type": "t", "payload": {}, "actor": "a",
             "cursor": 1, "channel_cursor": 1, "timestamp": ""},
        ]})
        events = asyncio.run(client.replay("ch-1"))
        assert len(events) == 1
        assert events[0].cursor == 1
        assert mock.call_args[0] == ("GET", "/api/v1/events/replay/ch-1")

    def test_replay_with_cursor_and_limit(self):
        client = _make_client()
        mock = _mock_request(client, return_value={"events": []})
        asyncio.run(client.replay("ch-1", cursor=10, limit=50, event_type="agent.step"))
        params = mock.call_args[1].get("params", {})
        assert params["cursor"] == "10"
        assert params["limit"] == "50"
        assert params["event_type"] == "agent.step"

    def test_replay_empty(self):
        client = _make_client()
        _mock_request(client, return_value={"events": []})
        events = asyncio.run(client.replay("ch-1"))
        assert events == []


class TestQuery:
    def test_query_all_filters(self):
        client = _make_client()
        mock = _mock_request(client, return_value={"events": []})
        asyncio.run(client.query(
            channel_ids=["ch-1", "ch-2"],
            event_type="test.event",
            actor="agent-1",
            cursor_start=10,
            cursor_end=100,
            from_time="2026-01-01T00:00:00Z",
            to_time="2026-12-31T23:59:59Z",
            limit=50,
            offset=10,
        ))
        body = mock.call_args[1]["json"]
        assert body["channel_ids"] == ["ch-1", "ch-2"]
        assert body["event_type"] == "test.event"
        assert body["actor"] == "agent-1"
        assert body["cursor_start"] == 10
        assert body["cursor_end"] == 100
        assert body["from"] == "2026-01-01T00:00:00Z"
        assert body["to"] == "2026-12-31T23:59:59Z"
        assert body["limit"] == 50
        assert body["offset"] == 10

    def test_query_minimal(self):
        client = _make_client()
        mock = _mock_request(client, return_value={"events": []})
        asyncio.run(client.query())
        body = mock.call_args[1]["json"]
        assert body["limit"] == 100
        assert "channel_ids" not in body


class TestGetCursor:
    def test_get_cursor_returns_int(self):
        client = _make_client()
        _mock_request(client, return_value={"latest_cursor": 42})
        cursor = asyncio.run(client.get_cursor("ch-1"))
        assert cursor == 42

    def test_get_cursor_returns_none(self):
        client = _make_client()
        _mock_request(client, return_value={})
        cursor = asyncio.run(client.get_cursor("ch-1"))
        assert cursor is None

    def test_latest_cursor_alias(self):
        client = _make_client()
        _mock_request(client, return_value={"latest_cursor": 42})
        cursor = asyncio.run(client.latest_cursor("ch-1"))
        assert cursor == 42



class TestState:
    def test_save_state_returns_version(self):
        client = _make_client()
        _mock_request(client, return_value={"version": 3})
        version = asyncio.run(client.save_state("ch-1", cursor=42, state={"key": "val"}))
        assert version == 3

    def test_latest_state_returns_dict(self):
        client = _make_client()
        _mock_request(client, return_value={"cursor": 42, "state": {"key": "val"}})
        result = asyncio.run(client.latest_state("ch-1"))
        assert result == {"cursor": 42, "state": {"key": "val"}}

    def test_latest_state_returns_none(self):
        client = _make_client()
        _mock_request(client, return_value={})
        result = asyncio.run(client.latest_state("ch-1"))
        assert result is None

    def test_list_states(self):
        client = _make_client()
        _mock_request(client, return_value={"versions": [
            {"version": 1, "cursor": 10, "timestamp": "2026-01-01T00:00:00Z"},
            {"version": 2, "cursor": 20, "timestamp": "2026-01-02T00:00:00Z"},
        ]})
        versions = asyncio.run(client.list_states("ch-1", limit=10, offset=0))
        assert len(versions) == 2
        assert versions[0]["version"] == 1

    def test_get_state_returns_dict(self):
        client = _make_client()
        _mock_request(client, return_value={"cursor": 42, "version": 1, "state": {"key": "val"}})
        result = asyncio.run(client.get_state("ch-1", version=1))
        assert result["version"] == 1
        assert result["state"]["key"] == "val"

    def test_get_state_returns_none(self):
        client = _make_client()
        _mock_request(client, return_value={})
        result = asyncio.run(client.get_state("ch-1", version=99))
        assert result is None

    def test_delete_state(self):
        client = _make_client()
        mock = _mock_request(client, return_value=None)
        asyncio.run(client.delete_state("ch-1", version=1))
        assert mock.call_args[0] == ("DELETE", "/api/v1/state/ch-1/version/1")


class TestChannelsAndFork:
    def test_list_channels(self):
        client = _make_client()
        _mock_request(client, return_value={"channels": ["ch-1", "ch-2"]})
        channels = asyncio.run(client.list_channels())
        assert channels == ["ch-1", "ch-2"]

    def test_fork_basic(self):
        client = _make_client()
        _mock_request(client, return_value={
            "channel_id": "fork-1",
            "parent_channel_id": "ch-1",
            "source_channel_id": "ch-1",
            "child_channel_id": "fork-1",
            "origin_run_id": "ch-1",
            "forked_at_cursor": 42,
            "forked_at": "2026-01-01T00:00:00Z",
            "display_name": "Fork A",
            "reason": "testing",
            "experiment_metadata": None,
            "requested_at_cursor": 42,
            "requested_cursor": 42,
            "resolved_cursor": 42,
            "source_state_version": 3,
            "source_state_sha256": "abc123",
            "restorable": True,
            "reproducibility": "state_exact",
            "replayed": False,
        })
        receipt = asyncio.run(client.fork("ch-1", "fork-1", 42, display_name="Fork A", reason="testing"))
        assert isinstance(receipt, ForkReceipt)
        assert receipt.fork_id == "fork-1"
        assert receipt.source_channel_id == "ch-1"
        assert receipt.requested_cursor == 42
        assert receipt.resolved_cursor == 42
        assert receipt.source_state_version == 3
        assert receipt.source_state_sha256 == "abc123"
        assert receipt.restorable is True
        assert receipt.reproducibility == "state_exact"

    def test_fork_returns_immutable_receipt_fields(self):
        client = _make_client()
        _mock_request(client, return_value={
            "channel_id": "fork-1",
            "parent_channel_id": "ch-1",
            "origin_run_id": "ch-1",
            "forked_at_cursor": 40,
            "forked_at": "2026-01-01T00:00:00Z",
            "requested_at_cursor": 42,
            "resolved_cursor": 40,
            "resolved_event_id": "evt-9",
            "source_state_version": 2,
            "source_state_sha256": "deadbeef",
            "restorable": True,
            "manifest": {"model": "gpt-5"},
            "reproducibility": "context_exact",
            "replayed": True,
        })
        receipt = asyncio.run(client.fork("ch-1", "fork-1", 42))
        assert receipt.requested_cursor == 42
        assert receipt.resolved_cursor == 40
        assert receipt.resolved_event_id == "evt-9"
        assert receipt.manifest == {"model": "gpt-5"}
        assert receipt.replayed is True

    def test_get_fork_receipt(self):
        client = _make_client()
        _mock_request(client, return_value={
            "fork_id": "fork-1",
            "source_channel_id": "ch-1",
            "child_channel_id": "fork-1",
            "requested_cursor": 0,
            "resolved_cursor": 12,
            "resolved_event_id": None,
            "source_state_version": 4,
            "source_state_sha256": "feed",
            "restorable": True,
            "replayed": False,
            "manifest": None,
            "reproducibility": "state_exact",
        })
        receipt = asyncio.run(client.get_fork_receipt("fork-1"))
        assert receipt.fork_id == "fork-1"
        assert receipt.resolved_cursor == 12
        assert receipt.reproducibility == "state_exact"

    def test_get_channel_metadata(self):
        client = _make_client()
        _mock_request(client, return_value={
            "channel_id": "ch-1", "parent_channel_id": None,
            "origin_run_id": "ch-1", "forked_at_cursor": 0,
            "forked_at": "2026-01-01T00:00:00Z",
        })
        meta = asyncio.run(client.get_channel_metadata("ch-1"))
        assert meta.channel_id == "ch-1"

    def test_get_channel_metadata_404(self):
        from actae_client.errors import APIError

        client = _make_client()
        _mock_request(client, side_effect=APIError(404, "Not found"))
        meta = asyncio.run(client.get_channel_metadata("ch-404"))
        assert meta is None

    def test_get_channel_metadata_500(self):
        from actae_client.errors import APIError

        client = _make_client()
        _mock_request(client, side_effect=APIError(500, "Internal"))
        with pytest.raises(APIError):
            asyncio.run(client.get_channel_metadata("ch-1"))

    def test_list_forks(self):
        client = _make_client()
        _mock_request(client, return_value={"forks": [
            {"channel_id": "fork-1", "parent_channel_id": "ch-1", "origin_run_id": "ch-1",
             "forked_at_cursor": 10, "forked_at": "2026-01-01T00:00:00Z",
             "display_name": "Fork A", "experiment_metadata": None},
        ]})
        forks = asyncio.run(client.list_forks("ch-1"))
        assert len(forks) == 1
        assert forks[0].channel_id == "fork-1"

    def test_list_forks_empty(self):
        client = _make_client()
        _mock_request(client, return_value={"forks": []})
        forks = asyncio.run(client.list_forks("ch-1"))
        assert forks == []

    def test_get_fork_tree(self):
        client = _make_client()
        _mock_request(client, return_value={
            "channel_id": "root",
            "display_name": "Root", "reason": None,
            "forked_at_cursor": 0, "event_count": 10, "latest_cursor": 20,
            "children": [
                {"channel_id": "fork-1", "display_name": "Fork A", "reason": "test",
                 "forked_at_cursor": 10, "event_count": 5, "latest_cursor": 15, "children": []},
            ],
        })
        tree = asyncio.run(client.get_fork_tree("root"))
        assert isinstance(tree, ForkInfo)
        assert tree.channel_id == "root"
        assert len(tree.children) == 1
        assert tree.children[0].channel_id == "fork-1"

    def test_update_metadata(self):
        client = _make_client()
        _mock_request(client, return_value={
            "channel_id": "ch-1", "parent_channel_id": None,
            "origin_run_id": "ch-1", "forked_at_cursor": 0,
            "forked_at": "2026-01-01T00:00:00Z",
            "display_name": "Updated", "reason": "changed",
            "experiment_metadata": {"key": "val"},
        })
        meta = asyncio.run(client.update_metadata(
            "ch-1", display_name="Updated", reason="changed",
            experiment_metadata={"key": "val"},
        ))
        assert meta.display_name == "Updated"
        assert meta.experiment_metadata == {"key": "val"}


class TestHealth:
    def test_health_check(self):
        client = _make_client()
        _mock_request(client, return_value={
            "status": "ok", "timestamp": "", "instance_id": "i-1",
            "check_duration_ms": 5, "components": {},
        })
        result = asyncio.run(client.health_check())
        assert isinstance(result, HealthStatus)
        assert result.status == "ok"

    def test_readiness_check(self):
        client = _make_client()
        _mock_request(client, return_value={
            "status": "ready", "timestamp": "", "instance_id": "i-1",
            "check_duration_ms": 3, "readiness_checks": {},
        })
        result = asyncio.run(client.readiness_check())
        assert isinstance(result, ReadinessResult)

    def test_get_metrics_text(self):
        client = _make_client()
        _mock_request(client, return_value="# HELP actae_events\n# TYPE actae_events counter\nactae_events 42\n")
        text = asyncio.run(client.get_metrics_text())
        assert "actae_events 42" in text

    def test_get_metrics_json(self):
        client = _make_client()
        _mock_request(client, return_value={
            "status": "ok", "timestamp": "", "uptime_seconds": 100,
            "websocket": {"connections": 5, "topics": 2, "messages_sent": 10,
                          "messages_received": 8, "total_messages": 18, "back_pressure": {}},
        })
        metrics = asyncio.run(client.get_metrics_json())
        assert isinstance(metrics, MetricsSnapshot)


class TestAuth:
    def test_signup(self):
        client = _make_client()
        _mock_request(client, return_value={
            "user": {"id": "u-1", "email": "test@test.com", "email_verified": False, "name": None, "image": None},
            "token": "jwt-1",
        })
        result = asyncio.run(client.signup("test@test.com", "password", name="Test"))
        assert isinstance(result, AuthResult)
        assert result.token == "jwt-1"

    def test_login(self):
        client = _make_client()
        _mock_request(client, return_value={
            "user": {"id": "u-1", "email": "test@test.com", "email_verified": True, "name": None, "image": None},
            "token": "jwt-1",
        })
        result = asyncio.run(client.login("test@test.com", "password"))
        assert isinstance(result, AuthResult)
        assert result.token == "jwt-1"

    def test_logout(self):
        client = _make_client()
        mock = _mock_request(client, return_value=None)
        asyncio.run(client.logout("jwt-1"))
        headers = mock.call_args[1].get("headers", {})
        assert headers.get("authorization") == "Bearer jwt-1"

    def test_get_me(self):
        client = _make_client()
        _mock_request(client, return_value={
            "user": {"id": "u-1", "email": "test@test.com", "email_verified": True, "name": "Test", "image": None},
        })
        result = asyncio.run(client.get_me("jwt-1"))
        assert isinstance(result, UserInfo)
        assert result.email == "test@test.com"


class TestRequestErrors:
    def test_api_error_4xx(self):
        from actae_client.errors import APIError

        client = _make_client()
        _mock_request(client, side_effect=APIError(400, "Bad request"))
        with pytest.raises(APIError) as exc:
            asyncio.run(client.list_channels())
        assert exc.value.status_code == 400

    def test_api_error_5xx(self):
        from actae_client.errors import APIError

        client = _make_client()
        _mock_request(client, side_effect=APIError(503, "Service unavailable"))
        with pytest.raises(APIError) as exc:
            asyncio.run(client.list_channels())
        assert exc.value.status_code == 503

    def test_rate_limit_error(self):
        from actae_client.errors import RateLimitError

        client = _make_client()
        _mock_request(client, side_effect=RateLimitError(retry_after_seconds=30))
        with pytest.raises(RateLimitError) as exc:
            asyncio.run(client.list_channels())
        assert exc.value.retry_after_seconds == 30


class TestSonicRsInResponses:
    def test_event_record_unwraps_sonic(self):
        client = _make_client()
        _mock_request(client, return_value={"event": {
            "id": "e1", "channel_id": "ch-1", "type": "t",
            "payload": {"$sonic_rs::private::JsonNumber": "42"},
            "actor": "a", "cursor": 1, "channel_cursor": 1, "timestamp": "",
        }})
        event = asyncio.run(client.record("ch-1", "t", {}, actor="a"))
        assert event.payload == 42

    def test_latest_state_unwraps_sonic(self):
        client = _make_client()
        _mock_request(client, return_value={
            "cursor": 1,
            "state": {"count": {"$sonic_rs::private::JsonNumber": "99"}},
        })
        result = asyncio.run(client.latest_state("ch-1"))
        assert result["state"]["count"] == 99


class TestEventFactories:
    def test_event_from_record_with_metadata(self):
        data = {
            "id": "e1", "channel_id": "ch-1", "type": "t", "payload": {},
            "actor": "a", "cursor": 1, "channel_cursor": 1, "timestamp": "2026-01-01T00:00:00Z",
        }
        ev = Event.from_record(data)
        assert ev.depends_on is None

    def test_event_from_broadcast_ignores_unknown_fields(self):
        data = {
            "id": "e1", "channel_id": "ch-1", "type": "broadcast",
            "payload": {}, "actor": "", "cursor": 0, "channel_cursor": None,
            "timestamp": "", "delivery_id": "del-1",  # legacy server field — ignored
        }
        ev = Event.from_broadcast(data)
        assert ev.id == "e1"
        assert not hasattr(ev, "delivery_id")

    def test_event_from_replay_with_metadata(self):
        data = {
            "id": "e1", "channel_id": "ch-1", "type": "t", "payload": {},
            "actor": "a", "cursor": 1, "channel_cursor": 1, "timestamp": "",
            "agent_id": "ag-1", "metadata": {"src": "test"},
        }
        ev = Event.from_replay(data)
        assert ev.agent_id == "ag-1"
        assert ev.metadata == {"src": "test"}

    def test_event_from_query_parses_depends_on(self):
        data = {
            "id": "e1", "channel_id": "ch-1", "type": "fork.started",
            "payload": {}, "actor": "a", "cursor": 1, "channel_cursor": 1,
            "timestamp": "", "depends_on": "parent-evt",
        }
        ev = Event.from_query(data)
        assert ev.depends_on == "parent-evt"

    def test_event_from_query(self):
        data = {
            "id": "e1", "channel_id": "ch-1", "type": "t", "payload": {},
            "actor": "a", "cursor": 1, "channel_cursor": 1, "timestamp": "",
            "agent_id": "ag-1",
        }
        ev = Event.from_query(data)
        assert ev.agent_id == "ag-1"
        assert ev.metadata is None  # from_query doesn't include metadata


class TestContextManager:
    def test_aenter_calls_connect(self):
        client = _make_client()
        with patch.object(client, "connect", new_callable=AsyncMock) as mock_connect:
            with patch.object(client, "disconnect", new_callable=AsyncMock):
                async def run():
                    async with client as c:
                        assert c is client
                asyncio.run(run())
                mock_connect.assert_awaited_once()

    def test_aexit_calls_disconnect(self):
        client = _make_client()
        with patch.object(client, "connect", new_callable=AsyncMock):
            with patch.object(client, "disconnect", new_callable=AsyncMock) as mock_disconnect:
                async def run():
                    async with client:
                        pass
                asyncio.run(run())
                mock_disconnect.assert_awaited_once()


class TestTransition:
    def test_transition_basic(self):
        client = _make_client()
        mock = _mock_request(client, return_value={
            "event": {
                "id": "evt-t1", "channel_id": "ch-1", "type": "agent.started",
                "payload": {"seq": 1}, "actor": "bot", "cursor": 1,
                "channel_cursor": 1, "timestamp": "2026-01-01T00:00:00Z",
            },
            "state_version": 7,
        })
        result = asyncio.run(client.transition(
            "ch-1", "agent.started", {"seq": 1}, {"turns": 1}, actor="bot",
        ))
        assert result.state_version == 7
        assert result.event.cursor == 1
        body = mock.call_args[1]["json"]
        assert body["state"] == {"turns": 1}
        assert body["metadata"]["actor"] == "bot"

    def test_transition_with_guards(self):
        client = _make_client()
        _mock_request(client, return_value={
            "event": {
                "id": "evt-t2", "channel_id": "ch-1", "type": "step",
                "payload": {}, "actor": "bot", "cursor": 2,
                "channel_cursor": 2, "timestamp": "2026-01-01T00:00:00Z",
            },
            "state_version": 8,
        })
        asyncio.run(client.transition(
            "ch-1", "step", {}, {}, expected_version=7, expected_cursor=2,
        ))
        body = asyncio.run(_last_body(client))
        assert body["expected_version"] == 7
        assert body["expected_cursor"] == 2

    def test_transition_operation_id_passthrough(self):
        client = _make_client()
        _mock_request(client, return_value={
            "event": {
                "id": "evt-t3", "channel_id": "ch-1", "type": "step",
                "payload": {}, "actor": "bot", "cursor": 3,
                "channel_cursor": 3, "timestamp": "2026-01-01T00:00:00Z",
            },
            "state_version": 9,
        })
        asyncio.run(client.transition("ch-1", "step", {}, {}, operation_id="op-t3"))
        body = asyncio.run(_last_body(client))
        assert body["operation_id"] == "op-t3"

    def test_transition_omits_operation_id_by_default(self):
        client = _make_client()
        _mock_request(client, return_value={
            "event": {
                "id": "evt-t4", "channel_id": "ch-1", "type": "step",
                "payload": {}, "actor": "bot", "cursor": 4,
                "channel_cursor": 4, "timestamp": "2026-01-01T00:00:00Z",
            },
            "state_version": 10,
        })
        asyncio.run(client.transition("ch-1", "step", {}, {}))
        body = asyncio.run(_last_body(client))
        assert "operation_id" not in body


async def _last_body(client):
    return client._request.call_args[1]["json"]


class TestConditionalSaveState:
    def test_save_state_with_guards(self):
        client = _make_client()
        mock = _mock_request(client, return_value={"version": 3})
        version = asyncio.run(client.save_state(
            "ch-1", 42, {"k": "v"}, expected_version=2,
        ))
        assert version == 3
        body = mock.call_args[1]["json"]
        assert body["expected_version"] == 2

    def test_save_state_guard_conflict_raises(self):
        from actae_client.errors import VersionConflictError
        client = _make_client()
        _mock_request(client, side_effect=VersionConflictError("conflict"))
        with pytest.raises(VersionConflictError):
            asyncio.run(client.save_state("ch-1", 42, {}, expected_version=9))


class TestForkIdempotency:
    def test_fork_sends_operation_id(self):
        client = _make_client()
        mock = _mock_request(client, return_value={
            "channel_id": "child-1", "parent_channel_id": "parent-1",
            "source_channel_id": "parent-1", "child_channel_id": "child-1",
            "origin_run_id": "parent-1", "forked_at_cursor": 3,
            "forked_at": "2026-01-01T00:00:00Z", "display_name": None,
            "reason": None, "experiment_metadata": None,
            "requested_cursor": 3, "resolved_cursor": 3, "restorable": True,
        })
        meta = asyncio.run(client.fork("parent-1", "child-1", 3))
        assert meta.fork_id == "child-1"
        assert meta.resolved_cursor == 3
        body = mock.call_args[1]["json"]
        assert body["at_cursor"] == 3
        assert body["operation_id"]  # auto-generated UUID

    def test_fork_explicit_operation_id_passthrough(self):
        client = _make_client()
        mock = _mock_request(client, return_value={
            "channel_id": "child-1", "parent_channel_id": None,
            "origin_run_id": "child-1", "forked_at_cursor": 0,
            "forked_at": "2026-01-01T00:00:00Z", "display_name": None,
            "reason": None, "experiment_metadata": None,
        })
        asyncio.run(client.fork("parent-1", "child-1", operation_id="op-xyz"))
        assert mock.call_args[1]["json"]["operation_id"] == "op-xyz"

    def test_fork_boundary_error_raises(self):
        from actae_client.errors import SnapshotBoundaryError
        client = _make_client()
        _mock_request(client, side_effect=SnapshotBoundaryError("no boundary"))
        with pytest.raises(SnapshotBoundaryError):
            asyncio.run(client.fork("parent-1", "child-1", 50))

    def test_diff_states(self):
        client = _make_client()
        mock = _mock_request(client, return_value={
            "left_channel_id": "fix-a",
            "right_channel_id": "fix-b",
            "left": {"channel_id": "fix-a", "cursor": 1, "state": {"step": 2, "x": True}},
            "right": {"channel_id": "fix-b", "cursor": 1, "state": {"step": 3, "y": True}},
            "common": {"channel_id": "root", "cursor": 1, "state": {"step": 1}},
            "left_diverged_at_cursor": 1,
            "right_diverged_at_cursor": 1,
            "truncated": False,
            "entry_count_total": 3,
            "max_entries": 500,
            "entries": [
                {"path": ["step"], "kind": "changed", "left": 2, "right": 3},
                {"path": ["x"], "kind": "removed", "left": True, "right": None},
                {"path": ["y"], "kind": "added", "left": None, "right": True},
            ],
        })
        diff = asyncio.run(client.diff_states("fix-a", "fix-b"))
        assert diff["left_channel_id"] == "fix-a"
        assert diff["right_channel_id"] == "fix-b"
        assert diff["common"]["channel_id"] == "root"
        assert diff["left_diverged_at_cursor"] == 1
        assert diff["right_diverged_at_cursor"] == 1
        assert diff["truncated"] is False
        assert diff["entry_count_total"] == 3
        assert diff["entries"][0]["path"] == ["step"]
        assert diff["entries"][0]["kind"] == "changed"
        assert diff["entries"][0]["left"] == 2
        assert diff["entries"][0]["right"] == 3
        # The request must pass left/right as query params.
        assert mock.call_args[1]["params"] == {"left": "fix-a", "right": "fix-b"}

    def test_diff_states_unwraps_state_blobs(self):
        client = _make_client()
        _mock_request(client, return_value={
            "left_channel_id": "a", "right_channel_id": "b",
            "left": {"channel_id": "a", "cursor": 1, "state": {"n": 1}},
            "right": {"channel_id": "b", "cursor": 1, "state": {"n": 2}},
            "common": None, "left_diverged_at_cursor": None, "right_diverged_at_cursor": None, "truncated": False, "entry_count_total": 0, "max_entries": 500, "entries": [],
        })
        diff = asyncio.run(client.diff_states("a", "b"))
        assert diff["left"]["state"] == {"n": 1}
        assert diff["common"] is None

    def test_diff_states_sync(self):
        client = _make_client()
        _mock_request(client, return_value={
            "left_channel_id": "a", "right_channel_id": "b",
            "left": {"channel_id": "a", "cursor": 1, "state": {}},
            "right": {"channel_id": "b", "cursor": 1, "state": {}},
            "common": None, "left_diverged_at_cursor": None, "right_diverged_at_cursor": None, "truncated": False, "entry_count_total": 0, "max_entries": 500, "entries": [],
        })
        diff = client.diff_states_sync("a", "b")
        assert diff["left_channel_id"] == "a"

    def test_decision_trail(self):
        client = _make_client()
        mock = _mock_request(client, return_value={
            "channel_id": "fork-c",
            "origin_run_id": "root",
            "ancestry": [
                {"channel_id": "fork-c", "parent_channel_id": "fork-a",
                 "forked_at_cursor": 5, "display_name": "C", "reason": None},
                {"channel_id": "fork-a", "parent_channel_id": "root",
                 "forked_at_cursor": 3, "display_name": "A", "reason": "first"},
                {"channel_id": "root", "parent_channel_id": None,
                 "forked_at_cursor": 0, "display_name": None, "reason": None},
            ],
            "boundary": {"channel_id": "fork-a", "cursor": 3, "state": {"step": 3}},
            "executions": [
                {"id": "ex-1", "channel_id": "fork-c", "key_name": "calc",
                 "tool_name": "calculator", "status": "completed",
                 "params": {"expr": "2+2"}, "result": {"ok": True}, "error": None,
                 "started_cursor": 6, "completed_cursor": 7},
            ],
        })
        trail = asyncio.run(client.decision_trail("fork-c"))
        assert trail["channel_id"] == "fork-c"
        assert trail["origin_run_id"] == "root"
        assert [h["channel_id"] for h in trail["ancestry"]] == ["fork-c", "fork-a", "root"]
        assert trail["boundary"]["channel_id"] == "fork-a"
        assert trail["boundary"]["state"] == {"step": 3}
        assert trail["executions"][0]["key_name"] == "calc"
        assert trail["executions"][0]["result"] == {"ok": True}
        assert mock.call_args[0][0] == "GET"
        assert mock.call_args[0][1] == "/api/v1/channels/fork-c/trail"

    def test_decision_trail_sync(self):
        client = _make_client()
        _mock_request(client, return_value={
            "channel_id": "root", "origin_run_id": "root",
            "ancestry": [{"channel_id": "root", "parent_channel_id": None,
                          "forked_at_cursor": 0, "display_name": None, "reason": None}],
            "boundary": None, "executions": [],
        })
        trail = client.decision_trail_sync("root")
        assert trail["origin_run_id"] == "root"


class TestConsumerGroups:
    def test_create_group(self):
        client = _make_client()
        mock = _mock_request(client, return_value={
            "group_id": "g1", "channel_id": "ch-1",
            "created_at": "2026-01-01T00:00:00Z", "metadata": None,
        })
        group = asyncio.run(client.create_group("g1", "ch-1", metadata={"team": "a"}))
        assert group.group_id == "g1"
        assert mock.call_args[1]["json"]["metadata"] == {"team": "a"}

    def test_list_groups_filter(self):
        client = _make_client()
        mock = _mock_request(client, return_value={"groups": [
            {"group_id": "g1", "channel_id": "ch-1", "created_at": "t", "metadata": None},
        ], "count": 1})
        groups = asyncio.run(client.list_groups(channel_id="ch-1"))
        assert len(groups) == 1
        assert mock.call_args[1]["params"] == {"channel_id": "ch-1"}

    def test_join_and_claim_and_ack(self):
        client = _make_client()
        _mock_request(client, return_value={"group_id": "g1", "consumer_id": "c1",
                                            "last_cursor": 0, "claimed_cursor": 0,
                                            "updated_at": "t"})
        offset = asyncio.run(client.join_group("g1", "c1", lease_seconds=30))
        assert offset.last_cursor == 0

        _mock_request(client, return_value={
            "group_id": "g1", "consumer_id": "c1",
            "lease_until": "2026-01-01T00:01:00Z",
            "events": [{
                "id": "e1", "channel_id": "ch-1", "type": "work",
                "payload": {}, "actor": "bot", "cursor": 1,
                "channel_cursor": 1, "timestamp": "2026-01-01T00:00:00Z",
            }],
            "count": 1,
        })
        work = asyncio.run(client.claim_work("g1", "c1", limit=10))
        assert len(work.events) == 1
        assert work.events[0].channel_cursor == 1

        ack_mock = _mock_request(client, return_value={"status": "acked"})
        asyncio.run(client.ack_work("g1", "c1", 1))
        assert ack_mock.call_args[1]["json"] == {"consumer_id": "c1", "cursor": 1}

    def test_offsets(self):
        client = _make_client()
        _mock_request(client, return_value={"group_id": "g1", "offsets": [
            {"group_id": "g1", "consumer_id": "c1", "last_cursor": 4,
             "claimed_cursor": 6, "updated_at": "t"},
        ], "count": 1})
        offsets = asyncio.run(client.group_offsets("g1"))
        assert offsets[0].last_cursor == 4
        assert offsets[0].claimed_cursor == 6


class TestWakeups:
    def test_schedule_wakeup(self):
        client = _make_client()
        mock = _mock_request(client, return_value={
            "status": "scheduled", "id": "w-1", "channel_id": "ch-1",
            "run_at": "2026-02-01T00:00:00Z", "status": "pending",
            "payload": {"step": "resume"}, "created_at": "t",
        })
        wakeup = asyncio.run(client.schedule_wakeup(
            "ch-1", "2026-02-01T00:00:00Z", payload={"step": "resume"},
        ))
        assert wakeup.id == "w-1"
        assert wakeup.status == "pending"
        body = mock.call_args[1]["json"]
        assert body["run_at"] == "2026-02-01T00:00:00Z"

    def test_cancel_wakeup(self):
        client = _make_client()
        _mock_request(client, return_value={"status": "cancelled"})
        assert asyncio.run(client.cancel_wakeup("w-1")) is True

    def test_schedule_wakeup_with_datetime(self):
        from datetime import datetime, timezone

        client = _make_client()
        mock = _mock_request(client, return_value={
            "id": "w-1", "channel_id": "ch-1", "run_at": "2026-02-01T00:00:00+00:00",
            "status": "pending", "payload": None, "created_at": "t",
        })
        when = datetime(2026, 2, 1, 1, 0, 0, tzinfo=timezone.utc)  # 01:00Z
        asyncio.run(client.schedule_wakeup("ch-1", when))
        body = mock.call_args[1]["json"]
        assert body["run_at"] == "2026-02-01T01:00:00+00:00"

    def test_schedule_wakeup_datetime_converts_timezone_to_utc(self):
        from datetime import datetime, timedelta, timezone

        client = _make_client()
        mock = _mock_request(client, return_value={
            "id": "w-1", "channel_id": "ch-1", "run_at": "2026-02-01T01:00:00+00:00",
            "status": "pending", "payload": None, "created_at": "t",
        })
        when = datetime(2026, 2, 1, 2, 0, 0, tzinfo=timezone(timedelta(hours=1)))  # 02:00+01:00 == 01:00Z
        asyncio.run(client.schedule_wakeup("ch-1", when))
        body = mock.call_args[1]["json"]
        assert body["run_at"] == "2026-02-01T01:00:00+00:00"

    def test_schedule_wakeup_naive_datetime_rejected(self):
        from datetime import datetime

        client = _make_client()
        _mock_request(client, return_value={})
        with pytest.raises(ValueError, match="timezone-aware"):
            asyncio.run(client.schedule_wakeup("ch-1", datetime(2026, 2, 1)))

    def test_cancel_wakeup_not_pending(self):
        from actae_client.errors import APIError
        client = _make_client()
        _mock_request(client, side_effect=APIError(409, "wakeup_not_pending"))
        assert asyncio.run(client.cancel_wakeup("w-1")) is False

    def test_get_wakeup_missing(self):
        from actae_client.errors import APIError
        client = _make_client()
        _mock_request(client, side_effect=APIError(404, "wakeup_not_found"))
        assert asyncio.run(client.get_wakeup("w-missing")) is None


class TestToolExecutions:
    _EXEC = {
        "id": "exec-1", "channel_id": "ch-1", "key_name": "pay-123",
        "tool_name": "charge_customer", "status": "running", "attempts": 1,
        "lease_until": "2026-02-01T00:00:00Z", "started_cursor": 5,
        "completed_cursor": None, "started_event_id": "evt-1",
        "completed_event_id": None, "replay_emitted": False,
        "params": {"customer": "abc"}, "result": None, "error": None,
        "created_at": "2026-02-01T00:00:00Z",
        "updated_at": "2026-02-01T00:00:00Z", "completed_at": None,
    }

    def test_claim_new_execution(self):
        from actae_client import ExecutionClaim
        client = _make_client()
        mock = _mock_request(client, return_value={
            "status": "claimed", "execution": self._EXEC,
            "result": None, "claim_token": "tok-1",
        })
        claim = asyncio.run(client.claim_execution(
            "ch-1", "pay-123", "charge_customer", {"customer": "abc"},
            lease_seconds=120, emit_replay_event=True, actor="bot",
        ))
        assert isinstance(claim, ExecutionClaim)
        assert claim.status == "claimed"
        assert claim.claim_token == "tok-1"
        assert claim.execution.id == "exec-1"
        assert claim.execution.attempts == 1
        body = mock.call_args[1]["json"]
        assert body["channel_id"] == "ch-1"
        assert body["key_name"] == "pay-123"
        assert body["lease_seconds"] == 120
        assert body["emit_replay_event"] is True
        assert body["actor"] == "bot"
        assert body["params"] == {"customer": "abc"}

    def test_claim_replayed(self):
        client = _make_client()
        _mock_request(client, return_value={
            "status": "replayed", "execution": self._EXEC,
            "result": {"ok": True}, "claim_token": None,
        })
        claim = asyncio.run(client.claim_execution(
            "ch-1", "pay-123", "charge_customer", params=None,
        ))
        assert claim.status == "replayed"
        assert claim.result == {"ok": True}
        assert claim.claim_token is None

    def test_claim_sends_dedup_fields(self):
        client = _make_client()
        mock = _mock_request(client, return_value={
            "status": "claimed", "execution": self._EXEC,
            "result": None, "claim_token": "tok-1",
        })
        asyncio.run(client.claim_execution(
            "ch-1", "k", "t", {"a": 1, "ts": "now"},
            dedup_fields=["a"],
        ))
        body = mock.call_args[1]["json"]
        assert body["dedup_fields"] == ["a"]

    def test_complete_execution(self):
        client = _make_client()
        mock = _mock_request(client, return_value={
            "status": "completed", "execution": {**self._EXEC, "status": "completed", "result": {"ok": True}},
        })
        info = asyncio.run(client.complete_execution(
            "exec-1", claim_token="tok-1", result={"ok": True},
        ))
        assert info.status == "completed"
        assert info.result == {"ok": True}
        body = mock.call_args[1]["json"]
        assert body["claim_token"] == "tok-1"
        assert body["result"] == {"ok": True}

    def test_fail_execution(self):
        client = _make_client()
        mock = _mock_request(client, return_value={
            "status": "failed", "execution": {**self._EXEC, "status": "failed", "error": {"message": "boom"}},
        })
        info = asyncio.run(client.fail_execution(
            "exec-1", "boom", claim_token="tok-1",
            error_type="ValueError", stack="trace",
        ))
        assert info.status == "failed"
        assert info.error["message"] == "boom"
        body = mock.call_args[1]["json"]
        assert body["message"] == "boom"
        assert body["error_type"] == "ValueError"
        assert body["stack"] == "trace"

    def test_heartbeat_execution(self):
        client = _make_client()
        mock = _mock_request(client, return_value={
            "status": "ok", "execution": self._EXEC,
        })
        info = asyncio.run(client.heartbeat_execution(
            "exec-1", claim_token="tok-1", lease_seconds=30,
        ))
        assert info.status == "running"
        body = mock.call_args[1]["json"]
        assert body["lease_seconds"] == 30

    def test_cancel_execution(self):
        client = _make_client()
        _mock_request(client, return_value={
            "status": "cancelled", "execution": {**self._EXEC, "status": "cancelled"},
        })
        info = asyncio.run(client.cancel_execution("exec-1", claim_token="tok-1"))
        assert info.status == "cancelled"

    def test_get_execution(self):
        client = _make_client()
        _mock_request(client, return_value={"status": "ok", "execution": self._EXEC})
        info = asyncio.run(client.get_execution("exec-1"))
        assert info.id == "exec-1"
        assert info.key_name == "pay-123"

    def test_list_executions(self):
        client = _make_client()
        mock = _mock_request(client, return_value={
            "status": "ok",
            "executions": [self._EXEC, {**self._EXEC, "id": "exec-2", "status": "completed"}],
            "count": 2,
        })
        execs = asyncio.run(client.list_executions("ch-1", limit=10))
        assert len(execs) == 2
        assert execs[0].id == "exec-1"
        assert execs[1].status == "completed"
        assert mock.call_args[1]["params"]["limit"] == 10

    def test_delete_execution(self):
        client = _make_client()
        mock = _mock_request(client, return_value={"status": "deleted"})
        assert asyncio.run(client.delete_execution("exec-1")) is True
        assert mock.call_args[0][0] == "DELETE"

    def test_get_execution_missing_raises(self):
        from actae_client import ExecutionNotFoundError
        client = _make_client()
        _mock_request(client, side_effect=ExecutionNotFoundError("execution_not_found"))
        with pytest.raises(ExecutionNotFoundError):
            asyncio.run(client.get_execution("nope"))

    def test_idempotency_mismatch_raises(self):
        from actae_client import IdempotencyKeyMismatchError
        client = _make_client()
        _mock_request(client, side_effect=IdempotencyKeyMismatchError("conflict"))
        with pytest.raises(IdempotencyKeyMismatchError):
            asyncio.run(client.claim_execution("ch-1", "k", "t"))

    def test_execution_not_owned_raises(self):
        from actae_client import ExecutionNotOwnedError
        client = _make_client()
        _mock_request(client, side_effect=ExecutionNotOwnedError("stale"))
        with pytest.raises(ExecutionNotOwnedError):
            asyncio.run(client.complete_execution("exec-1", claim_token="bad"))


class TestCapabilities:
    def test_parse_capabilities_scoping(self):
        from actae_client.client import Capabilities
        caps = Capabilities({
            "auth_method": "api_key",
            "key_source": "local",
            "permissions": {"read": ["project-a.*"], "write": ["*"], "delete": [], "admin": []},
        })
        assert caps.can_read("project-a.ch") is True
        assert caps.can_read("project-b.ch") is False
        assert caps.can_write("anything") is True
        assert caps.can_publish("anything") is True
        assert caps.can_fork("project-b.ch") is True  # write:["*"]
        assert caps.can("delete", "x") is False
        # Admin patterns grant every action.
        caps_admin = Capabilities({"permissions": {"admin": ["ops.*"]}})
        assert caps_admin.can("read", "ops.ch") is True
        assert caps_admin.can("read", "other.ch") is False

    def test_capabilities_endpoint(self):
        client = _make_client()
        client._request = AsyncMock(return_value={
            "auth_method": "api_key",
            "permissions": {"read": ["*"]},
        })
        caps = asyncio.run(client.capabilities())
        assert caps.can_read("x") is True
        assert caps.can_write("x") is False
