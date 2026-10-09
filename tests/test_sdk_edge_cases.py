"""Edge case and boundary condition tests for the SDK.

Covers Unicode, empty/null payloads, large payloads, and other
corner cases that could cause issues in production. No server required.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


def _make_client():
    from actae_client.client import ActaeClient

    return ActaeClient(api_key="sk-test", endpoint="http://localhost:8002")


class TestUnicodeAndSpecialCharacters:
    def test_record_with_unicode_payload(self):
        client = _make_client()
        payload = {
            "message": "Hello  ",
            "emoji": "",
            "accented": "éàüñç",
            "math": "∑∫√≈≠",
            "rtl": "مرحبا بالعالم",
        }
        mock = AsyncMock(return_value={"event": {
            "id": "e1", "channel_id": "ch-1", "type": "t",
            "payload": payload, "actor": "a", "cursor": 1,
            "channel_cursor": 1, "timestamp": "2026-01-01T00:00:00Z",
        }})
        client._request = mock

        event = asyncio.run(client.record("ch-1", "t", payload, actor="a"))
        assert event.payload["emoji"] == ""
        assert event.payload["rtl"] == "مرحبا بالعالم"

    def test_record_with_control_chars(self):
        client = _make_client()
        payload = {"data": "\x00\x01\x02\x1f\x7f"}
        mock = AsyncMock(return_value={"event": {
            "id": "e1", "channel_id": "ch-1", "type": "t",
            "payload": payload, "actor": "a", "cursor": 1,
            "channel_cursor": 1, "timestamp": "2026-01-01T00:00:00Z",
        }})
        client._request = mock

        event = asyncio.run(client.record("ch-1", "t", payload, actor="a"))
        assert event.payload["data"] == "\x00\x01\x02\x1f\x7f"


class TestEmptyAndNullPayloads:
    def test_record_empty_payload(self):
        client = _make_client()
        mock = AsyncMock(return_value={"event": {
            "id": "e1", "channel_id": "ch-1", "type": "t",
            "payload": {}, "actor": "a", "cursor": 1,
            "channel_cursor": 1, "timestamp": "",
        }})
        client._request = mock

        event = asyncio.run(client.record("ch-1", "t", {}, actor="a"))
        assert event.payload == {}

    def test_record_null_payload(self):
        client = _make_client()
        mock = AsyncMock(return_value={"event": {
            "id": "e1", "channel_id": "ch-1", "type": "t",
            "payload": None, "actor": "a", "cursor": 1,
            "channel_cursor": 1, "timestamp": "",
        }})
        client._request = mock

        event = asyncio.run(client.record("ch-1", "t", None, actor="a"))
        assert event.payload is None

    def test_record_empty_event_type(self):
        client = _make_client()
        mock = AsyncMock(return_value={"event": {
            "id": "e1", "channel_id": "ch-1", "type": "",
            "payload": {}, "actor": "a", "cursor": 1,
            "channel_cursor": 1, "timestamp": "",
        }})
        client._request = mock

        event = asyncio.run(client.record("ch-1", "", {}, actor="a"))
        assert event.event_type == ""

    def test_record_empty_channel_id(self):
        client = _make_client()
        mock = AsyncMock(return_value={"event": {
            "id": "e1", "channel_id": "", "type": "t",
            "payload": {}, "actor": "a", "cursor": 1,
            "channel_cursor": 1, "timestamp": "",
        }})
        client._request = mock

        # Empty channel ids are rejected by the shared URL-path-safe grammar.
        with pytest.raises(ValueError, match="channel_id"):
            asyncio.run(client.record("", "t", {}, actor="a"))
        mock.assert_not_awaited()

    def test_event_all_optional_fields_none(self):
        from actae_client import Event

        ev = Event(
            id="e1", channel_id="ch-1", event_type="t", payload={},
            actor="a", cursor=1, channel_cursor=1, timestamp="",
        )
        assert ev.agent_id is None
        assert ev.user_id is None
        assert ev.metadata is None


class TestLargePayloads:
    def test_large_nested_payload(self):
        client = _make_client()
        # Build a deeply nested dict
        payload = {"level0": {"level1": {"level2": {"level3": {"level4": "deep"}}}}}
        mock = AsyncMock(return_value={"event": {
            "id": "e1", "channel_id": "ch-1", "type": "t",
            "payload": payload, "actor": "a", "cursor": 1,
            "channel_cursor": 1, "timestamp": "",
        }})
        client._request = mock

        event = asyncio.run(client.record("ch-1", "t", payload, actor="a"))
        assert event.payload["level0"]["level1"]["level2"]["level3"]["level4"] == "deep"

    def test_large_list_payload(self):
        client = _make_client()
        payload = {"items": list(range(1000))}
        mock = AsyncMock(return_value={"event": {
            "id": "e1", "channel_id": "ch-1", "type": "t",
            "payload": payload, "actor": "a", "cursor": 1,
            "channel_cursor": 1, "timestamp": "",
        }})
        client._request = mock

        event = asyncio.run(client.record("ch-1", "t", payload, actor="a"))
        assert len(event.payload["items"]) == 1000
        assert event.payload["items"][0] == 0
        assert event.payload["items"][-1] == 999

    def test_mixed_types_in_payload(self):
        client = _make_client()
        payload = {
            "str": "text",
            "int": 42,
            "float": 3.14,
            "bool_true": True,
            "bool_false": False,
            "null": None,
            "list": [1, "two", 3.0, None],
            "nested": {"a": 1, "b": "two"},
        }
        mock = AsyncMock(return_value={"event": {
            "id": "e1", "channel_id": "ch-1", "type": "t",
            "payload": payload, "actor": "a", "cursor": 1,
            "channel_cursor": 1, "timestamp": "",
        }})
        client._request = mock

        event = asyncio.run(client.record("ch-1", "t", payload, actor="a"))
        assert event.payload["int"] == 42
        assert event.payload["float"] == 3.14
        assert event.payload["null"] is None


class TestChannelIdEdgeCases:
    def test_channel_id_with_special_chars(self):
        client = _make_client()
        channel_id = "channel/with/slashes?query=value&#fragment"
        mock = AsyncMock(return_value={"latest_cursor": 42})
        client._request = mock

        # Slashes/query/fragment are not addressable as a single URL path
        # segment → rejected by the shared grammar.
        with pytest.raises(ValueError, match="channel_id"):
            asyncio.run(client.get_cursor(channel_id))
        mock.assert_not_awaited()

    def test_channel_id_with_spaces(self):
        client = _make_client()
        mock = AsyncMock(return_value={"latest_cursor": 1})
        client._request = mock

        with pytest.raises(ValueError, match="channel_id"):
            asyncio.run(client.get_cursor("my channel"))
        mock.assert_not_awaited()

    def test_channel_id_very_long(self):
        client = _make_client()
        channel_id = "a" * 1000
        mock = AsyncMock(return_value={"latest_cursor": 42})
        client._request = mock

        # Longer than the 256-char cap → rejected.
        with pytest.raises(ValueError, match="channel_id"):
            asyncio.run(client.get_cursor(channel_id))
        mock.assert_not_awaited()

    def test_channel_id_valid_grammar_is_accepted(self):
        client = _make_client()
        mock = AsyncMock(return_value={"latest_cursor": 7})
        client._request = mock

        # The full allowed grammar [A-Za-z0-9._:-] up to 256 chars works.
        channel_id = "fork:exp_v1.3-2026"
        cursor = asyncio.run(client.get_cursor(channel_id))
        assert cursor == 7
        mock.assert_awaited_once()


class TestEventTypeEdgeCases:
    def test_event_type_with_dots_and_colons(self):
        from actae_client import Event

        ev = Event(
            id="e1", channel_id="ch-1", event_type="agent.tool:call.v1",
            payload={}, actor="a", cursor=1, channel_cursor=1, timestamp="",
        )
        assert ev.event_type == "agent.tool:call.v1"

    def test_event_type_with_integer_name(self):
        from actae_client import Event

        ev = Event(
            id="e1", channel_id="ch-1", event_type="123",
            payload={}, actor="a", cursor=1, channel_cursor=1, timestamp="",
        )
        assert ev.event_type == "123"


class TestMetadataEdgeCases:
    def test_metadata_with_none_values(self):
        client = _make_client()
        metadata = {"key1": None, "key2": "value", "key3": ""}
        mock = AsyncMock(return_value={"event": {
            "id": "e1", "channel_id": "ch-1", "type": "t",
            "payload": {}, "actor": "a", "cursor": 1,
            "channel_cursor": 1, "timestamp": "",
        }})
        client._request = mock

        asyncio.run(client.record("ch-1", "t", {}, actor="a", metadata=metadata))
        sent_meta = mock.call_args[1]["json"]["metadata"]["metadata"]
        assert sent_meta["key1"] is None
        assert sent_meta["key3"] == ""


class TestSessionEdgeCases:
    def test_session_name_with_special_chars(self):
        from actae_client import ActaeClient
        from actae_client.session import AgentSession

        actae = MagicMock(spec=ActaeClient)
        actae.record = AsyncMock(return_value=MagicMock(
            channel_id="session-", cursor=1,
            timestamp="2026-01-01T00:00:00Z",
        ))
        actae.save_state = AsyncMock(return_value=1)
        actae.update_metadata = AsyncMock()
        actae.get_channel_metadata = AsyncMock(return_value=None)

        session = AgentSession(actae, "session-")

        async def run():
            async with session:
                assert session.channel_id is not None
            assert session.status == "completed"

        asyncio.run(run())

    def test_session_without_state_fn(self):
        from actae_client import ActaeClient
        from actae_client.session import AgentSession

        actae = MagicMock(spec=ActaeClient)
        actae.record = AsyncMock(return_value=MagicMock(
            channel_id="ch-1", cursor=1, timestamp="",
        ))
        actae.update_metadata = AsyncMock()
        # Give spec'd async methods real AsyncMock returns so no auto-created
        # child AsyncMocks leak unawaited coroutines (e.g. dict() on a mock).
        actae.get_channel_metadata = AsyncMock(return_value=None)

        session = AgentSession(actae, "test", state_fn=None)

        async def run():
            async with session:
                await session.step("step", input="test", output="result")
            assert session.step_count == 1

        asyncio.run(run())

    def test_session_empty_name_raises(self):
        from actae_client import ActaeClient
        from actae_client.session import AgentSession

        actae = MagicMock(spec=ActaeClient)
        with pytest.raises(ValueError, match="name is required"):
            AgentSession(actae, "")

    def test_session_whitespace_name(self):
        from actae_client import ActaeClient
        from actae_client.session import AgentSession

        actae = MagicMock(spec=ActaeClient)
        # Whitespace-only names are allowed by the SDK (bool("   ") is True)
        session = AgentSession(actae, "   ")
        assert session.name == "   "


class TestCursorEdgeCases:
    def test_cursor_none_returns_none(self):
        client = _make_client()
        mock = AsyncMock(return_value={})
        client._request = mock

        cursor = asyncio.run(client.get_cursor("empty-channel"))
        assert cursor is None

    def test_cursor_zero(self):
        client = _make_client()
        mock = AsyncMock(return_value={"latest_cursor": 0})
        client._request = mock

        cursor = asyncio.run(client.get_cursor("ch-1"))
        assert cursor == 0

    def test_cursor_large_value(self):
        client = _make_client()
        mock = AsyncMock(return_value={"latest_cursor": 9223372036854775807})
        client._request = mock

        cursor = asyncio.run(client.get_cursor("ch-1"))
        assert cursor == 9223372036854775807


class TestReplayEdgeCases:
    def test_replay_limit_zero(self):
        client = _make_client()
        mock = AsyncMock(return_value={"events": []})
        client._request = mock

        events = asyncio.run(client.replay("ch-1", limit=0))
        assert events == []

    def test_replay_limit_negative(self):
        client = _make_client()
        mock = AsyncMock(return_value={"events": []})
        client._request = mock

        events = asyncio.run(client.replay("ch-1", limit=-1))
        assert events == []


class TestUnwrapSonicEdgeCases:
    def test_nested_sonic_in_list(self):
        from actae_client.types import _unwrap_sonic

        data = [{"$sonic_rs::private::JsonNumber": "42"}, "hello", 123]
        result = _unwrap_sonic(data)
        assert result[0] == 42
        assert result[1] == "hello"
        assert result[2] == 123

    def test_sonic_with_extra_keys(self):
        """If the sonic dict has extra keys, don't unwrap."""
        from actae_client.types import _unwrap_sonic

        data = {"$sonic_rs::private::JsonNumber": "42", "other": "field"}
        result = _unwrap_sonic(data)
        assert isinstance(result, dict)
        # Not unwrapped because the dict has more than one key
        assert result["$sonic_rs::private::JsonNumber"] == "42"

    def test_sonic_deeply_nested(self):
        from actae_client.types import _unwrap_sonic

        data = {
            "a": {"b": {"$sonic_rs::private::JsonNumber": "99"}},
            "c": [{"$sonic_rs::private::JsonNumber": "88"}],
        }
        result = _unwrap_sonic(data)
        assert result["a"]["b"] == 99
        assert result["c"][0] == 88

    def test_empty_dict_not_unwrapped(self):
        from actae_client.types import _unwrap_sonic

        result = _unwrap_sonic({})
        assert result == {}

    def test_empty_list_not_unwrapped(self):
        from actae_client.types import _unwrap_sonic

        result = _unwrap_sonic([])
        assert result == []


class TestNormalizeMsgEdgeCases:
    def test_normalize_empty_dict(self):
        from actae_client.client import ActaeClient

        result = ActaeClient._normalize_msg({})
        assert result == {}

    def test_normalize_unknown_tag(self):
        from actae_client.client import ActaeClient

        data = {"UnknownTag": {"field": "value"}}
        result = ActaeClient._normalize_msg(data)
        # Unknown tags are passed through unchanged — no TYPE_MAP entry
        assert result is data
        assert result == {"UnknownTag": {"field": "value"}}

    def test_normalize_case_sensitive(self):
        from actae_client.client import ActaeClient

        data = {"broadcast": {"topic": "ch-1"}}  # lowercase, doesn't match
        result = ActaeClient._normalize_msg(data)
        assert result is data  # unchanged


class TestWebSocketEdgeCases:
    def test_msg_with_missing_type(self):
        client = _make_client()
        data = {"topic": "ch-1", "payload": {"msg": "hello"}}
        asyncio.run(client._dispatch(data))  # Should not raise

    def test_publish_empty_payload(self):
        client = _make_client()
        client._connected = True
        client._ws = MagicMock()
        client._ws.send_json = AsyncMock()

        async def run():
            sent = asyncio.get_running_loop().create_future()

            async def capture(frame):
                sent.set_result(dict(frame))

            client._ws.send_json = AsyncMock(side_effect=capture)
            task = asyncio.create_task(client.publish("ch-1", {}))
            frame = await asyncio.wait_for(sent, timeout=1)
            await client._dispatch({
                "type": "ack", "id": "ack-1", "channel_id": "ch-1",
                "event_type": "broadcast", "payload": {}, "actor": "",
                "cursor": 0, "channel_cursor": None, "timestamp": "",
                "request_id": frame["request_id"],
            })
            event = await asyncio.wait_for(task, timeout=1)
            assert event.payload == {}

        asyncio.run(run())

    def test_dispatch_subscription_with_payload_based_topic(self):
        client = _make_client()
        data = {"type": "subscription", "event": "subscribed", "payload": {"topic": "ch-1", "cursor": 5}}
        asyncio.run(client._dispatch(data))
        assert client._subscribed.get("ch-1") == 5


class TestHealthEdgeCases:
    def test_readiness_all_degraded(self):
        client = _make_client()
        mock = AsyncMock(return_value={
            "status": "degraded", "timestamp": "", "instance_id": "i-1",
            "check_duration_ms": 0,
            "readiness_checks": {
                "database_ready": False, "capacity_available": False,
                "uptime_seconds": 0, "connection_utilization": 0.0,
                "active_connections": 0, "max_connections": 0,
            },
        })
        client._request = mock
        result = asyncio.run(client.readiness_check())
        assert result.status == "degraded"
        assert result.database_ready is False

    def test_health_no_components(self):
        client = _make_client()
        mock = AsyncMock(return_value={
            "status": "ok", "timestamp": "", "instance_id": "i-1",
            "check_duration_ms": 0, "components": {},
        })
        client._request = mock
        result = asyncio.run(client.health_check())
        assert result.components == {}


class TestChannelIDGrammar:
    def test_valid_ids_accepted(self):
        from actae_client.client import validate_channel_id
        for id_ in ("a", "channel-1", "ch_1", "project.a:step-2", "x" * 256):
            validate_channel_id(id_)  # must not raise

    def test_invalid_ids_rejected(self):
        from actae_client.client import validate_channel_id
        for id_ in ("", "a/b", "a b", "a\tb", 'a"b', "a\\b", "a%", "x" * 257):
            with pytest.raises(ValueError):
                validate_channel_id(id_)

    def test_record_rejects_slash_channel(self):
        client = _make_client()
        with pytest.raises(ValueError):
            asyncio.run(client.record("bad/channel", "e", {}, actor="a"))
