"""Tests for the onboarding-focused SDK surface: namespace facades,
the sync facade, multi-callback WebSocket delivery, stream(), and the
AgentSession.resume() naming contract."""

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from actae_client import Event, ChannelMetadata, ForkReceipt


def test_from_env_cloud_configuration(monkeypatch):
    from actae_client import ActaeClient

    monkeypatch.setenv("ACTAE_URL", "https://i-example.eu1.cloud.actae.dev/")
    monkeypatch.setenv("ACTAE_API_KEY", "diviga_test")
    client = ActaeClient.from_env(timeout=12)
    assert client._endpoint == "https://i-example.eu1.cloud.actae.dev"
    assert client._ws_endpoint == "wss://i-example.eu1.cloud.actae.dev/ws"
    assert client._timeout == 12


def test_from_env_requires_credentials(monkeypatch):
    from actae_client import ActaeClient

    monkeypatch.delenv("ACTAE_API_KEY", raising=False)
    monkeypatch.setenv("ACTAE_URL", "https://example.test")
    with pytest.raises(ValueError, match="ACTAE_API_KEY"):
        ActaeClient.from_env()


def _make_client():
    from actae_client.client import ActaeClient

    return ActaeClient(api_key="sk-test", endpoint="http://localhost:8002")


def _mock_request(client, return_value):
    client._request = AsyncMock(return_value=return_value)
    return client._request


class TestNamespaceFacades:
    def test_facades_exist_and_share_client(self):
        client = _make_client()
        for name in (
            "events", "state", "channels", "executions",
            "groups", "wakeups", "health", "auth", "ws",
        ):
            assert hasattr(client, name)
        assert client.events._client is client
        assert client.ws._client is client

    def test_facade_delegates_method(self):
        client = _make_client()
        _mock_request(client, return_value={
            "event": {
                "id": "e1", "type": "agent.step", "payload": {"ok": 1},
                "actor": "a", "cursor": 1, "channel_cursor": 1,
                "timestamp": "2026-01-01T00:00:00Z",
            }
        })
        ev = asyncio.run(client.events.record("ch-1", "agent.step", {"ok": 1}, actor="a"))
        assert isinstance(ev, Event)
        assert ev.event_type == "agent.step"

    def test_facade_covers_all_group_members(self):
        client = _make_client()
        expected = {
            "events": {"record", "replay", "query", "transition", "get_cursor", "latest_cursor"},
            "state": {"save_state", "latest_state", "list_states", "get_state", "delete_state"},
            "channels": {"list_channels", "fork", "get_channel_metadata", "list_forks", "get_fork_tree", "update_metadata"},
            "executions": {"claim_execution", "complete_execution", "fail_execution", "heartbeat_execution", "cancel_execution", "get_execution", "list_executions", "delete_execution"},
            "groups": {"create_group", "list_groups", "delete_group", "join_group", "claim_work", "ack_work", "heartbeat", "group_offsets"},
            "wakeups": {"schedule_wakeup", "list_wakeups", "get_wakeup", "cancel_wakeup"},
            "health": {"health_check", "readiness_check", "get_metrics_text", "get_metrics_json"},
            "auth": {"signup", "login", "logout", "get_me"},
            "ws": {"connect", "disconnect", "subscribe", "unsubscribe", "publish", "stream", "on_message", "on_error", "on_subscribed", "on_disconnected", "on_reconnect"},
        }
        for name, members in expected.items():
            facade = getattr(client, name)
            for member in members:
                assert hasattr(facade, member), f"{name}.{member} missing"
                assert getattr(facade, member) == getattr(client, member)
            with pytest.raises(AttributeError):
                getattr(facade, "does_not_exist")

    def test_facade_dir_lists_members(self):
        client = _make_client()
        assert "record" in dir(client.events)
        assert "save_state" in dir(client.state)


class TestMultiCallback:
    def test_multiple_message_callbacks_all_invoked(self):
        from actae_client.client import ActaeClient

        client = _make_client()
        seen = []

        client.on_message(lambda topic, ev: seen.append((topic, ev.event_type, "cb1")))
        client.on_message(lambda topic, ev: seen.append((topic, ev.event_type, "cb2")))

        asyncio.run(client._dispatch({
            "type": "broadcast", "topic": "ch-1",
            "payload": {"event": {"id": "e1", "type": "agent.step", "payload": {}, "actor": "a", "cursor": 1}},
        }))
        assert len(seen) == 2
        assert seen[0][2] == "cb1"
        assert seen[1][2] == "cb2"

    def test_callback_exception_does_not_block_others(self):
        client = _make_client()
        seen = []

        def failing(topic, ev):
            raise RuntimeError("boom")

        client.on_message(failing)
        client.on_message(lambda topic, ev: seen.append(ev.event_type))

        asyncio.run(client._dispatch({
            "type": "broadcast", "topic": "ch-1",
            "payload": {"event": {"id": "e1", "type": "agent.step", "payload": {}, "actor": "a", "cursor": 1}},
        }))
        assert seen == ["agent.step"]


class TestSubscribeWaitDefault:
    def test_wait_defaults_to_true(self):
        import inspect
        from actae_client.client import ActaeClient

        sig = inspect.signature(ActaeClient.subscribe)
        assert sig.parameters["wait"].default is True

    def test_wait_false_returns_immediately(self):
        client = _make_client()
        client._connected = True
        client._ws = MagicMock()
        client._ws.send_json = AsyncMock()
        asyncio.run(client.subscribe("ch-1", wait=False))
        client._ws.send_json.assert_awaited_once()

    def test_wait_true_waits_for_confirmation(self):
        client = _make_client()
        client._connected = True
        client._ws = MagicMock()
        client._ws.send_json = AsyncMock()

        async def run():
            task = asyncio.create_task(client.subscribe("ch-1", wait=True))
            await asyncio.sleep(0)
            await client._dispatch({
                "type": "subscription", "event": "subscribed",
                "topic": "ch-1", "cursor": 7,
            })
            await task

        asyncio.run(run())
        assert client._subscribed["ch-1"] == 7


class TestPublishSerialization:
    def test_concurrent_publishes_each_get_their_own_ack(self):
        client = _make_client()
        client._connected = True
        client._ws = MagicMock()
        client._ws.send_json = AsyncMock()
        client._timeout = 5.0

        async def fake_reader():
            # Simulate the server acking each publish with the matching
            # request_id echoed from the client's broadcast frame. Publishes
            # are serialized, so ack each frame as it arrives.
            dispatched = 0
            while dispatched < 2:
                calls = list(client._ws.send_json.await_args_list)
                if len(calls) > dispatched:
                    frame = calls[dispatched].args[0]
                    n = frame["payload"]["n"]
                    await client._dispatch({
                        "type": "ack",
                        "id": f"e{n}", "channel_id": "ch-1", "event_type": "broadcast",
                        "payload": {"n": n}, "actor": "a", "cursor": n, "channel_cursor": n,
                        "timestamp": "2026-01-01T00:00:00Z",
                        "request_id": frame["request_id"],
                    })
                    dispatched += 1
                else:
                    await asyncio.sleep(0)

        async def run():
            reader = asyncio.create_task(fake_reader())
            results = await asyncio.gather(
                client.publish("ch-1", {"n": 1}),
                client.publish("ch-1", {"n": 2}),
            )
            await reader
            return results

        ev1, ev2 = asyncio.run(run())
        assert ev1.payload["n"] == 1
        assert ev2.payload["n"] == 2
        assert ev1.cursor == 1
        assert ev2.cursor == 2


class TestStream:
    def test_stream_yields_broadcasts(self):
        client = _make_client()
        client._connected = True
        client._ws = MagicMock()
        client._ws.send_json = AsyncMock()

        async def run():
            events = []

            async def consume():
                async for ev in client.stream("ch-1"):
                    events.append(ev.event_type)
                    if len(events) == 2:
                        break

            consumer = asyncio.create_task(consume())
            await asyncio.sleep(0)
            await client._dispatch({
                "type": "subscription", "event": "subscribed",
                "topic": "ch-1", "cursor": 1,
            })
            await client._dispatch({
                "type": "broadcast", "topic": "ch-1",
                "payload": {"event": {"id": "e1", "type": "a.step", "payload": {}, "actor": "a", "cursor": 1}},
            })
            await client._dispatch({
                "type": "broadcast", "topic": "ch-1",
                "payload": {"event": {"id": "e2", "type": "b.step", "payload": {}, "actor": "a", "cursor": 2}},
            })
            await consumer
            return events

        assert asyncio.run(run()) == ["a.step", "b.step"]
        assert client._stream_queues == {}

    def test_stream_ends_on_disconnect(self):
        client = _make_client()
        client._connected = True
        client._ws = MagicMock()
        client._ws.send_json = AsyncMock()

        async def run():
            got = []

            async def consume():
                async for ev in client.stream("ch-1"):
                    got.append(ev.event_type)

            consumer = asyncio.create_task(consume())
            await asyncio.sleep(0)
            await client._dispatch({
                "type": "subscription", "event": "subscribed",
                "topic": "ch-1", "cursor": 1,
            })
            await client._dispatch({
                "type": "broadcast", "topic": "ch-1",
                "payload": {"event": {"id": "e1", "type": "x", "payload": {}, "actor": "a", "cursor": 1}},
            })
            client._connected = False
            await client._disconnect_ws()
            await consumer
            return got

        assert asyncio.run(run()) == ["x"]
        assert client._stream_queues == {}

    def test_stream_requires_connection(self):
        client = _make_client()
        with pytest.raises(Exception) as exc:
            asyncio.run(client.stream("ch-1").__anext__())
        from actae_client import ActaeConnectionError
        assert isinstance(exc.value, ActaeConnectionError)


class TestSyncFacade:
    def test_record_sync(self):
        client = _make_client()
        mock = _mock_request(client, return_value={
            "event": {
                "id": "e1", "type": "agent.step", "payload": {"ok": 1},
                "actor": "a", "cursor": 1, "channel_cursor": 1,
                "timestamp": "2026-01-01T00:00:00Z",
            }
        })
        ev = client.record_sync("ch-1", "agent.step", {"ok": 1}, actor="a")
        assert isinstance(ev, Event)
        assert ev.cursor == 1

        # operation_id must be forwarded by the sync facade.
        client.record_sync("ch-1", "agent.step", {"ok": 1}, actor="a", operation_id="op-9")
        body = mock.call_args[1]["json"]
        assert body["operation_id"] == "op-9"

    def test_replay_sync(self):
        client = _make_client()
        _mock_request(client, return_value={"events": [{
            "id": "e1", "type": "agent.step", "payload": {}, "actor": "a",
            "cursor": 1, "channel_cursor": 1, "timestamp": "2026-01-01T00:00:00Z",
        }]})
        events = client.replay_sync("ch-1", cursor=0)
        assert len(events) == 1
        assert events[0].cursor == 1

    def test_query_sync(self):
        client = _make_client()
        _mock_request(client, return_value={"events": [{
            "id": "e1", "type": "agent.step", "payload": {}, "actor": "a",
            "cursor": 1, "channel_cursor": 1, "timestamp": "2026-01-01T00:00:00Z",
        }]})
        events = client.query_sync(event_type="agent.step")
        assert len(events) == 1

    def test_get_cursor_sync(self):
        client = _make_client()
        _mock_request(client, return_value={"latest_cursor": 42})
        assert client.get_cursor_sync("ch-1") == 42

    def test_latest_state_sync(self):
        client = _make_client()
        _mock_request(client, return_value={"cursor": 7, "state": {"mem": [1]}})
        state = client.latest_state_sync("ch-1")
        assert state == {"cursor": 7, "state": {"mem": [1]}}

    def test_save_state_sync(self):
        client = _make_client()
        _mock_request(client, return_value={"version": 3})
        assert client.save_state_sync("ch-1", 7, {"mem": 1}) == 3

    def test_transition_sync(self):
        from actae_client import TransitionResult

        client = _make_client()
        _mock_request(client, return_value={
            "event": {
                "id": "e1", "type": "agent.step", "payload": {}, "actor": "a",
                "cursor": 1, "channel_cursor": 1, "timestamp": "2026-01-01T00:00:00Z",
            },
            "state_version": 2,
        })
        result = client.transition_sync("ch-1", "agent.step", {}, {"mem": 1})
        assert isinstance(result, TransitionResult)
        assert result.event.cursor == 1
        assert result.state_version == 2

    def test_fork_sync(self):
        client = _make_client()
        _mock_request(client, return_value={
            "channel_id": "exp-v2", "parent_channel_id": "exp-v1",
            "source_channel_id": "exp-v1", "child_channel_id": "exp-v2",
            "origin_run_id": "exp-v1", "forked_at_cursor": 7,
            "forked_at": "2026-01-01T00:00:00Z", "display_name": None,
            "reason": None,
            "requested_cursor": 7, "resolved_cursor": 7, "restorable": True,
        })
        meta = client.fork_sync("exp-v1", "exp-v2", 7)
        assert isinstance(meta, ForkReceipt)
        assert meta.fork_id == "exp-v2"
        assert meta.resolved_cursor == 7

    def test_disconnect_sync(self):
        """Sync-only users must be able to release the WS + HTTP sessions
        (avoid leaking aiohttp connectors at shutdown)."""
        from unittest.mock import patch

        client = _make_client()
        with patch.object(client, "disconnect", new=AsyncMock()) as mock_disconnect:
            client.disconnect_sync()
            mock_disconnect.assert_awaited_once()


class TestResumeNaming:
    def test_resume_uses_channel_id_as_name(self):
        from actae_client.types import ChannelMetadata
        from actae_client.session import AgentSession

        actae = _make_client()
        actae.get_channel_metadata = AsyncMock(return_value=ChannelMetadata(
            channel_id="exp-v1",
            parent_channel_id=None,
            origin_run_id="exp-v1",
            forked_at_cursor=0,
            forked_at="2026-01-01T00:00:00Z",
            display_name="My Experiment",
            reason=None,
            experiment_metadata={
                "status": "crashed",
                "step_count": 3,
                "cursors": [1, 2, 3],
                "params": {"model": "gpt-4"},
            },
        ))

        session = asyncio.run(AgentSession.resume(actae, "exp-v1"))
        assert session.name == "exp-v1"
        assert session._display_name == "My Experiment"
        assert session.step_count == 3
        assert session.cursors == [1, 2, 3]


class TestCertAutoDiscovery:
    """The SDK auto-loads the default mTLS identity from the config dir."""

    _CA_PEM = """-----BEGIN CERTIFICATE-----
MIIC/zCCAeegAwIBAgIUV5CnQI93ubIbgHOjQWTj3Y6OIhswDQYJKoZIhvcNAQEL
BQAwDzENMAsGA1UEAwwEdGVzdDAeFw0yNjA4MDUwODA1MzBaFw0yNjA5MDQwODA1
MzBaMA8xDTALBgNVBAMMBHRlc3QwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEK
AoIBAQDMW4AaDdiV8Pwn035OjOdAIR6tMJchoyTWWNoJwLKsHr1jQbNUnAoqcfvO
6MJAoJ2+OaorTpIXGv/M4Bc1XxwPuB/NkGtdcxZgdBKuoQCZij59Scex/4aiyUO+
8cD6dpFrOuz2xr5wwZ2IamUo+s0qgTVzEYuAOpaJrb1Le7yMZuUTgqhNxHdWZU3Y
I/1G/J6ZXv79fIt6YkCiMnQAp61n4fS+9mBtlFkHkD+pMRAbicrOEvMqlx0/yDbC
rUMzgo4Gs28b/SwgkTIve0clYYPXqTarFzzRXu34aPU+16axPzUt2+MwAwkXyIc1
ERUzbhFX+lcYDzOwUsMtzJu5EtQfAgMBAAGjUzBRMB0GA1UdDgQWBBTsWg6r77KD
8cklqbJENKBLt2saHTAfBgNVHSMEGDAWgBTsWg6r77KD8cklqbJENKBLt2saHTAP
BgNVHRMBAf8EBTADAQH/MA0GCSqGSIb3DQEBCwUAA4IBAQATt3sf1UdKnsDj7KQI
a+HGDkhuY5+qFv4ZPHb63mZ2kXNXf9zXKQVxuxTY7LZT2yJjNfT25ZoZbp93+EmM
zWkvSseUArvBs5SbN4yKR8jsSK81paFwaVgXNwSff/bY01/ALBbHbjfZrtWE0/T1
k6DqAx6djo3qYR7VGSiEG9q2PR0lgEtQ4IQzX1kTaFaprGziygcc/G3tkZ+LxhKG
k30XCBgZ8nKoWVEKySyQZRhmFK3Ma1fIbGH39WOpgFmuEzT2eo+xTWJXMWG36Wot
zojW09+3cscGhLPHxyrdB8ZIQbnRdo/ZQdE1T0W4x/M63G9F77sqAfBJEltADlqU
zq7G
-----END CERTIFICATE-----
"""
    _ID_PEM = """-----BEGIN CERTIFICATE-----
MIIC/zCCAeegAwIBAgIUV5CnQI93ubIbgHOjQWTj3Y6OIhswDQYJKoZIhvcNAQEL
BQAwDzENMAsGA1UEAwwEdGVzdDAeFw0yNjA4MDUwODA1MzBaFw0yNjA5MDQwODA1
MzBaMA8xDTALBgNVBAMMBHRlc3QwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEK
AoIBAQDMW4AaDdiV8Pwn035OjOdAIR6tMJchoyTWWNoJwLKsHr1jQbNUnAoqcfvO
6MJAoJ2+OaorTpIXGv/M4Bc1XxwPuB/NkGtdcxZgdBKuoQCZij59Scex/4aiyUO+
8cD6dpFrOuz2xr5wwZ2IamUo+s0qgTVzEYuAOpaJrb1Le7yMZuUTgqhNxHdWZU3Y
I/1G/J6ZXv79fIt6YkCiMnQAp61n4fS+9mBtlFkHkD+pMRAbicrOEvMqlx0/yDbC
rUMzgo4Gs28b/SwgkTIve0clYYPXqTarFzzRXu34aPU+16axPzUt2+MwAwkXyIc1
ERUzbhFX+lcYDzOwUsMtzJu5EtQfAgMBAAGjUzBRMB0GA1UdDgQWBBTsWg6r77KD
8cklqbJENKBLt2saHTAfBgNVHSMEGDAWgBTsWg6r77KD8cklqbJENKBLt2saHTAP
BgNVHRMBAf8EBTADAQH/MA0GCSqGSIb3DQEBCwUAA4IBAQATt3sf1UdKnsDj7KQI
a+HGDkhuY5+qFv4ZPHb63mZ2kXNXf9zXKQVxuxTY7LZT2yJjNfT25ZoZbp93+EmM
zWkvSseUArvBs5SbN4yKR8jsSK81paFwaVgXNwSff/bY01/ALBbHbjfZrtWE0/T1
k6DqAx6djo3qYR7VGSiEG9q2PR0lgEtQ4IQzX1kTaFaprGziygcc/G3tkZ+LxhKG
k30XCBgZ8nKoWVEKySyQZRhmFK3Ma1fIbGH39WOpgFmuEzT2eo+xTWJXMWG36Wot
zojW09+3cscGhLPHxyrdB8ZIQbnRdo/ZQdE1T0W4x/M63G9F77sqAfBJEltADlqU
zq7G
-----END CERTIFICATE-----
"""
    _KEY_PEM = """-----BEGIN PRIVATE KEY-----
MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDMW4AaDdiV8Pwn
035OjOdAIR6tMJchoyTWWNoJwLKsHr1jQbNUnAoqcfvO6MJAoJ2+OaorTpIXGv/M
4Bc1XxwPuB/NkGtdcxZgdBKuoQCZij59Scex/4aiyUO+8cD6dpFrOuz2xr5wwZ2I
amUo+s0qgTVzEYuAOpaJrb1Le7yMZuUTgqhNxHdWZU3YI/1G/J6ZXv79fIt6YkCi
MnQAp61n4fS+9mBtlFkHkD+pMRAbicrOEvMqlx0/yDbCrUMzgo4Gs28b/SwgkTIv
e0clYYPXqTarFzzRXu34aPU+16axPzUt2+MwAwkXyIc1ERUzbhFX+lcYDzOwUsMt
zJu5EtQfAgMBAAECggEADPsm+QC2KFglfFYn6M90hRNVgoTapM1bMq50MzhlYR+W
wi5TOOWsk6On7i3E4RwSyRmaoKOeDg+t/hKiBsbi3nDAvGsXFtmPq1LUOPmLMzWf
4I+GOt1TbRXB0uhCbOaJODmHAeoAAOOboSW5BVBhJfkNLyEHLn8KPvalVp0mjfiC
JbmJTWljkFrn66y+Lqww9YnCqbAdQoZ038jt0KX5lBgHgF6jQ5ngy3+k7cWwaIkp
HRRGaBU3LZN0UDvlaiTF67YZw+mJUteyPLgTnMT5cait2MgeZ8OJK+3GkdNG8DxW
OnwCQqUkSMYwAJk3/H0aEXdzfs961xN13iQ3T6jRsQKBgQD1MhJc9v2JPv8e1ryQ
ZpCggDGIinbh93CWVaocyfXZyio1eTnwkR6QIBWdK/qaDNIB8/edMaCjPWXVvWWf
RLwsNujEGt0F0Vl3MChAmAqq6803qrDPfo3g4wZIN6WX7KC3YefaQkBsDGbImeNz
RQRVUgZQCMXYfPSBUxGzuYd0NwKBgQDVXMD5ECOqJ/lCtZo6bQkxtWPPplJo9wA2
+46TgP4QvKxlSC/yB48I1b3iDGUASE2LV9ad9cnQ2hEXL3OxFwrCc11dFy70iZML
WDm7K3dNmSRnspy3ACwQ/ZcpRVBjiXHhLqMAV0JnDsvlqwe3WWhJLcT8pq/D89OH
rLoztIN7WQKBgQDVwdc8cJ7Lfb4P9ojhImlHYzrLnFrT2FGw3fG1s2O/gH2XrJ2U
Wg9Y+n+dS+/nSPH0feoKgm9WoHodAkaLuPKLYTs/a2PwZHgobjVJSsNSCswXkZke
62do/MJHRyv37HSYKqRkJInhKFaa333oyexjLWUPdPZ2K0lFTVQLaNzrtQKBgFnc
cOH1LDA0GcVA2y4UUjT/YoRIVpkivpJprIjvYRIHhMw7dQYIrPNZolmcQsW1rgMs
AZYRuOgfj+cl8yH4xG1VTVMxunL/plC23cm46sxh3XVXQq3Igsa9J3cYXF0vvCjN
DZXNKohhMPsP53YPT97SSg7m3Uw4WzTfSKUSN/YxAoGBAPSQa4cDEAE2FosldpOI
3Sbeq3Fp4nrsQ8kv+zN890iQdz+zIkQIloqUYexnB+oD4BIEtqdHD4LaG27RX+Aj
8XAJBE5w0NxY4zH1BFL6IJJV0iNpqVgs4hY2h3UzSe4JoP3OxjQx9R7nm+zxWuBZ
6mGXJYU/EM0SaJiobdNDLFs/
-----END PRIVATE KEY-----
"""

    def _write_identity(self, tmp_dir):
        (tmp_dir / "ca.crt").write_text(self._CA_PEM)
        (tmp_dir / "client.crt").write_text(self._ID_PEM)
        (tmp_dir / "client.key").write_text(self._KEY_PEM)
        return tmp_dir

    def test_discovers_mtls_from_config_dir(self, tmp_path, monkeypatch):
        cfg = self._write_identity(tmp_path)
        monkeypatch.setenv("ACTAE_CONFIG_DIR", str(cfg))
        from actae_client.client import ActaeClient

        client = ActaeClient(api_key="sk-test", endpoint="https://localhost:8002")
        assert client._ssl is not None
        assert client._mtls_auto_discovered is True
        assert client._mtls_hint_enabled is False

    def test_explicit_args_win_over_discovery(self, tmp_path, monkeypatch):
        cfg = self._write_identity(tmp_path)
        monkeypatch.setenv("ACTAE_CONFIG_DIR", str(cfg))
        import ssl as ssl_mod
        from actae_client.client import ActaeClient

        ctx = ssl_mod.create_default_context()
        client = ActaeClient(api_key="sk-test", endpoint="https://localhost:8002", ssl_context=ctx)
        assert client._ssl is ctx
        assert client._mtls_auto_discovered is False

    def test_no_files_no_auto_ssl(self, tmp_path, monkeypatch):
        monkeypatch.setenv("ACTAE_CONFIG_DIR", str(tmp_path))
        from actae_client.client import ActaeClient

        client = ActaeClient(api_key="sk-test", endpoint="https://localhost:8002")
        assert client._ssl is None
        assert client._mtls_hint_enabled is True

    def test_http_endpoint_skips_discovery(self, tmp_path, monkeypatch):
        cfg = self._write_identity(tmp_path)
        monkeypatch.setenv("ACTAE_CONFIG_DIR", str(cfg))
        from actae_client.client import ActaeClient

        client = ActaeClient(api_key="sk-test", endpoint="http://localhost:8002")
        assert client._ssl is None
        assert client._mtls_hint_enabled is False

    def test_connect_error_includes_mtls_hint(self, tmp_path, monkeypatch):
        monkeypatch.setenv("ACTAE_CONFIG_DIR", str(tmp_path))
        from actae_client.client import ActaeClient

        client = ActaeClient(api_key="sk-test", endpoint="https://localhost:8002")
        client._mtls_hint_enabled = True
        from actae_client import ActaeConnectionError

        with pytest.raises(ActaeConnectionError) as exc:
            asyncio.run(client.connect())
        assert "issue-client-cert.sh" in str(exc.value)


class TestEchoSelf:
    """echo_self=True delivers a client's own publishes to its local consumers."""

    def _publish_once(self, client):
        client._connected = True
        client._ws = MagicMock()
        client._ws.send_json = AsyncMock()
        client._timeout = 5.0

        async def fake_reader():
            while not client._ws.send_json.await_args_list:
                await asyncio.sleep(0)
            frame = client._ws.send_json.await_args_list[0].args[0]
            await client._dispatch({
                "type": "ack",
                "id": "e1", "channel_id": "ch-1", "event_type": "broadcast",
                "payload": {"n": 1}, "actor": "a", "cursor": 1,
                "channel_cursor": 1, "timestamp": "2026-01-01T00:00:00Z",
                "request_id": frame["request_id"],
            })

        async def run():
            reader = asyncio.create_task(fake_reader())
            await client.publish("ch-1", {"n": 1})
            await reader

        asyncio.run(run())

    def test_echo_self_true_delivers_to_callbacks(self):
        from actae_client.client import ActaeClient

        client = ActaeClient(api_key="sk-test", endpoint="http://localhost:8002", echo_self=True)
        seen = []
        client.on_message(lambda topic, ev: seen.append((topic, ev.cursor)))
        self._publish_once(client)
        assert seen == [("ch-1", 1)]

    def test_echo_self_true_delivers_to_streams(self):
        from actae_client.client import ActaeClient

        client = ActaeClient(api_key="sk-test", endpoint="http://localhost:8002", echo_self=True)
        queue = asyncio.Queue()
        client._stream_queues["ch-1"] = [queue]
        self._publish_once(client)
        ev = asyncio.run(queue.get())
        assert ev.cursor == 1

    def test_echo_self_false_no_local_delivery(self):
        from actae_client.client import ActaeClient

        client = ActaeClient(api_key="sk-test", endpoint="http://localhost:8002")
        seen = []
        client.on_message(lambda topic, ev: seen.append(ev.cursor))
        self._publish_once(client)
        assert seen == []
