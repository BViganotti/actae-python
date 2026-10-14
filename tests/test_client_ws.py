import asyncio
import sys
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest

from actae_client import Event
from actae_client.errors import APIError
from actae_client.client import _jittered_delay


def _make_client():
    from actae_client.client import ActaeClient

    return ActaeClient(api_key="sk-test", ws_endpoint="ws://localhost:8002")


class TestNormalizeMsg:
    def test_connection_message(self):
        from actae_client.client import ActaeClient

        raw = {"Connection": {"event": "connected", "payload": {"connection_id": "cid-1"}}}
        result = ActaeClient._normalize_msg(raw)
        assert result["type"] == "connection"
        assert result["event"] == "connected"
        assert result["payload"]["connection_id"] == "cid-1"

    def test_broadcast_message(self):
        from actae_client.client import ActaeClient

        raw = {"Broadcast": {"topic": "ch-1", "payload": {"msg": "hello"}}}
        result = ActaeClient._normalize_msg(raw)
        assert result["type"] == "broadcast"
        assert result["topic"] == "ch-1"

    def test_subscription_message(self):
        from actae_client.client import ActaeClient

        raw = {"Subscription": {"event": "subscribed", "topic": "ch-1", "cursor": 42}}
        result = ActaeClient._normalize_msg(raw)
        assert result["type"] == "subscription"
        assert result["cursor"] == 42

    def test_ack_message(self):
        from actae_client.client import ActaeClient

        raw = {"Ack": {"id": "ack-1", "cursor": 10}}
        result = ActaeClient._normalize_msg(raw)
        assert result["type"] == "ack"
        assert result["id"] == "ack-1"

    def test_cursor_sync_message(self):
        from actae_client.client import ActaeClient

        raw = {"CursorSync": {"topic": "ch-1", "cursor": 15}}
        result = ActaeClient._normalize_msg(raw)
        assert result["type"] == "cursor_sync"

    def test_error_message(self):
        from actae_client.client import ActaeClient

        raw = {"Error": {"message": "Something went wrong"}}
        result = ActaeClient._normalize_msg(raw)
        assert result["type"] == "error"
        assert result["message"] == "Something went wrong"

    def test_nack_message(self):
        from actae_client.client import ActaeClient

        raw = {"Nack": {"message": "Not authorized"}}
        result = ActaeClient._normalize_msg(raw)
        assert result["type"] == "nack"

    def test_already_flat(self):
        from actae_client.client import ActaeClient

        raw = {"type": "broadcast", "topic": "ch-1", "payload": {}}
        result = ActaeClient._normalize_msg(raw)
        assert result is raw

    def test_unknown_type_passthrough(self):
        from actae_client.client import ActaeClient

        raw = {"type": "unknown", "data": "val"}
        result = ActaeClient._normalize_msg(raw)
        assert result is raw

    def test_empty_dict(self):
        from actae_client.client import ActaeClient

        result = ActaeClient._normalize_msg({})
        assert result == {}


class TestDispatch:
    def test_dispatch_authenticated(self):
        client = _make_client()
        client._auth_event = asyncio.Event()
        data = {"type": "connection", "event": "authenticated", "payload": {"connection_id": "cid-1"}}
        asyncio.run(client._dispatch(data))
        assert client._authenticated is True
        assert client._connection_id == "cid-1"
        assert client._auth_event.is_set()

    def test_dispatch_connected(self):
        client = _make_client()
        data = {"type": "connection", "event": "connected", "payload": {"connection_id": "cid-1"}}
        asyncio.run(client._dispatch(data))
        assert client._connection_id == "cid-1"

    def test_dispatch_pong(self):
        client = _make_client()
        data = {"type": "connection", "event": "pong", "payload": {}}
        asyncio.run(client._dispatch(data))  # Should not raise

    def test_dispatch_broadcast_with_event_trigger_callback(self):
        client = _make_client()
        received = []
        client.on_message(lambda topic, event: received.append((topic, event)))
        data = {
            "type": "broadcast", "topic": "ch-1",
            "payload": {
                "event": {
                    "id": "e1", "channel_id": "ch-1", "type": "broadcast",
                    "payload": {"msg": "hello"}, "actor": "sys",
                    "cursor": 5, "timestamp": "",
                }
            },
        }
        asyncio.run(client._dispatch(data))
        assert len(received) == 1
        topic, event = received[0]
        assert topic == "ch-1"
        assert event.id == "e1"
        assert event.cursor == 5

    def test_dispatch_broadcast_without_event(self):
        client = _make_client()
        received = []
        client.on_message(lambda topic, event: received.append((topic, event)))
        data = {"type": "broadcast", "topic": "ch-1", "payload": {"msg": "direct"}}
        asyncio.run(client._dispatch(data))
        assert len(received) == 1
        assert received[0][1].payload == {"msg": "direct"}

    def test_dispatch_broadcast_updates_topic_cursor_for_reconnect(self):
        """The last-seen cursor per topic must advance on broadcast so an
        auto-reconnect resubscribes at the watermark instead of re-delivering
        every event since the first subscribe."""
        client = _make_client()
        client._subscribed["ch-1"] = 3  # subscription-time cursor
        data = {
            "type": "broadcast", "topic": "ch-1",
            "payload": {
                "event": {
                    "id": "e9", "channel_id": "ch-1", "type": "broadcast",
                    "payload": {}, "actor": "sys", "cursor": 9, "timestamp": "",
                }
            },
        }
        asyncio.run(client._dispatch(data))
        assert client._subscribed["ch-1"] == 9

    def test_dispatch_broadcast_without_cursor_keeps_watermark(self):
        client = _make_client()
        client._subscribed["ch-1"] = 3
        data = {"type": "broadcast", "topic": "ch-1", "payload": {"msg": "direct"}}
        asyncio.run(client._dispatch(data))
        assert client._subscribed["ch-1"] == 3

    def test_dispatch_broadcast_after_unsubscribe_does_not_resurrect_topic(self):
        """An in-flight broadcast delivered after unsubscribe must not
        re-add the topic to the reconnect resubscribe set."""
        client = _make_client()
        client._subscribed.pop("ch-1", None)
        data = {
            "type": "broadcast", "topic": "ch-1",
            "payload": {
                "event": {
                    "id": "e9", "channel_id": "ch-1", "type": "broadcast",
                    "payload": {}, "actor": "sys", "cursor": 9, "timestamp": "",
                }
            },
        }
        asyncio.run(client._dispatch(data))
        assert "ch-1" not in client._subscribed

    def test_dispatch_drops_catch_up_duplicate(self):
        """An event the server delivers both in a subscribe replay and live
        (the live-registration-before-replay race) must be delivered once."""
        client = _make_client()
        received = []
        client.on_message(lambda topic, ev: received.append(ev.cursor))
        client._delivered_cursor["ch-1"] = 5  # replay delivered up to 5

        dup = {
            "type": "broadcast", "topic": "ch-1",
            "payload": {
                "event": {
                    "id": "e4", "channel_id": "ch-1", "type": "broadcast",
                    "payload": {}, "actor": "sys", "cursor": 4, "timestamp": "",
                }
            },
        }
        asyncio.run(client._dispatch(dup))
        assert received == [], f"duplicate delivered: {received}"

        fresh = {
            "type": "broadcast", "topic": "ch-1",
            "payload": {
                "event": {
                    "id": "e6", "channel_id": "ch-1", "type": "broadcast",
                    "payload": {}, "actor": "sys", "cursor": 6, "timestamp": "",
                }
            },
        }
        asyncio.run(client._dispatch(fresh))
        assert received == [6]

    def test_subscribe_resets_delivery_window(self):
        """A fresh subscribe() starts a new delivery window, so an explicit
        re-subscribe legitimately re-delivers old events."""
        client = _make_client()
        client._connected = True
        client._ws = MagicMock()
        client._ws.send_json = AsyncMock()
        client._delivered_cursor["ch-1"] = 99
        asyncio.run(client.subscribe("ch-1", cursor=0, wait=False))
        assert "ch-1" not in client._delivered_cursor

    def test_dispatch_broadcast_callback_exception(self):
        client = _make_client()
        def failing_cb(topic, event):
            raise ValueError("simulated")
        client.on_message(failing_cb)
        data = {"type": "broadcast", "topic": "ch-1", "payload": {"event": {"id": "", "channel_id": "", "type": "t", "payload": {}, "actor": "", "cursor": 0, "timestamp": ""}}}
        asyncio.run(client._dispatch(data))  # Should not raise

    def test_dispatch_subscribed(self):
        client = _make_client()
        received = []
        client._on_subscribed_cb = lambda topic, cursor: received.append((topic, cursor))
        data = {"type": "subscription", "event": "subscribed", "topic": "ch-1", "cursor": 42}
        asyncio.run(client._dispatch(data))
        assert client._subscribed.get("ch-1") == 42
        assert received[0] == ("ch-1", 42)

    def test_dispatch_subscribed_wait_event(self):
        client = _make_client()
        ev = asyncio.Event()
        client._subscribed_events["ch-1"] = [ev]
        data = {"type": "subscription", "event": "subscribed", "topic": "ch-1", "cursor": 42}
        asyncio.run(client._dispatch(data))
        assert ev.is_set()
        assert "ch-1" not in client._subscribed_events

    def test_dispatch_subscribed_wakes_all_concurrent_waiters(self):
        """stream() + an explicit subscribe() on the same topic must both
        be released by a single server confirmation."""
        client = _make_client()
        ev1, ev2 = asyncio.Event(), asyncio.Event()
        client._subscribed_events["ch-1"] = [ev1, ev2]
        data = {"type": "subscription", "event": "subscribed", "topic": "ch-1", "cursor": 42}
        asyncio.run(client._dispatch(data))
        assert ev1.is_set()
        assert ev2.is_set()
        assert "ch-1" not in client._subscribed_events

    def test_dispatch_unsubscribed(self):
        client = _make_client()
        client._subscribed["ch-1"] = 42
        data = {"type": "subscription", "event": "unsubscribed", "payload": {"topic": "ch-1"}}
        asyncio.run(client._dispatch(data))
        assert "ch-1" not in client._subscribed

    def test_dispatch_subscription_error(self):
        client = _make_client()
        received = []
        client._on_error_cb = lambda msg: received.append(msg)
        data = {"type": "subscription", "event": "error", "payload": {"error": "Permission denied"}}
        asyncio.run(client._dispatch(data))
        assert received[0] == "Permission denied"

    def test_dispatch_cursor_sync(self):
        client = _make_client()
        data = {"type": "cursor_sync", "topic": "ch-1", "cursor": 100}
        asyncio.run(client._dispatch(data))
        assert client._subscribed["ch-1"] == 100

    def test_dispatch_ack(self):
        client = _make_client()
        async def run():
            data = {"type": "ack", "cursor": 5, "id": "ack-1"}
            await client._dispatch(data)
            # The ack should be in the queue for publish() to consume
            ack = await asyncio.wait_for(client._ack_queue.get(), timeout=1)
            assert ack["cursor"] == 5
            assert ack["id"] == "ack-1"
        asyncio.run(run())

    def test_dispatch_error_triggers_callback(self):
        client = _make_client()
        received = []
        client._on_error_cb = lambda msg: received.append(msg)
        data = {"type": "error", "message": "Server error"}
        asyncio.run(client._dispatch(data))
        assert received[0] == "Server error"

    def test_dispatch_error_callback_exception(self):
        client = _make_client()
        def failing_cb(msg):
            raise ValueError("cb failure")
        client._on_error_cb = failing_cb
        data = {"type": "error", "message": "Server error"}
        asyncio.run(client._dispatch(data))  # Should not raise


class TestConnectDisconnect:
    def test_connect_is_idempotent(self):
        client = _make_client()
        client._connected = True
        with patch.object(client, "_do_connect", new_callable=AsyncMock) as mock_do:
            asyncio.run(client.connect())
            mock_do.assert_not_called()

    def test_disconnect_sets_auto_reconnect_false(self):
        client = _make_client()
        asyncio.run(client.disconnect())
        assert client._auto_reconnect is False

    def test_disconnect_closes_foreign_loop_sessions_on_their_loop(self):
        """Sessions created by the sync facade live on background loops;
        closing them from the main loop would raise a cross-loop
        RuntimeError. They must be closed via their own loop."""
        from unittest.mock import AsyncMock, MagicMock, patch

        client = _make_client()
        foreign_loop = asyncio.new_event_loop()
        foreign_session = MagicMock()
        foreign_session.closed = False
        foreign_session._loop = foreign_loop
        foreign_session.close = AsyncMock()
        client._sessions_by_loop[12345] = foreign_session

        scheduled = []

        def fake_run_coroutine_threadsafe(coro, loop):
            coro.close()  # mock coroutine — nothing to run; avoid GC warning
            scheduled.append((coro, loop))
            fut = asyncio.Future()
            return fut

        with patch("asyncio.run_coroutine_threadsafe", side_effect=fake_run_coroutine_threadsafe):
            asyncio.run(client.disconnect())

        assert len(scheduled) == 1
        assert scheduled[0][1] is foreign_loop
        foreign_session.close.assert_not_awaited()  # never awaited on the main loop
        foreign_loop.close()


class TestSubscribeUnsubscribe:
    def test_subscribe_raises_if_not_connected(self):
        from actae_client.errors import ActaeConnectionError

        client = _make_client()
        with pytest.raises(ActaeConnectionError, match="Not connected"):
            asyncio.run(client.subscribe("ch-1", wait=False))

    def test_unsubscribe_raises_if_not_connected(self):
        from actae_client.errors import ActaeConnectionError

        client = _make_client()
        with pytest.raises(ActaeConnectionError, match="Not connected"):
            asyncio.run(client.unsubscribe("ch-1"))

    def test_publish_raises_if_not_connected(self):
        from actae_client.errors import ActaeConnectionError

        client = _make_client()
        with pytest.raises(ActaeConnectionError, match="Not connected"):
            asyncio.run(client.publish("ch-1", {}))

    def test_subscribe_sends_json(self):
        client = _make_client()
        client._connected = True
        client._ws = MagicMock()
        client._ws.send_json = AsyncMock()
        asyncio.run(client.subscribe("ch-1", cursor=42, wait=False))
        client._ws.send_json.assert_awaited_once_with({"type": "subscribe", "topic": "ch-1", "cursor": 42})

    def test_subscribe_with_wait(self):
        client = _make_client()
        client._connected = True
        client._ws = MagicMock()
        client._ws.send_json = AsyncMock()

        async def run():
            async def confirm():
                await client._dispatch({"type": "subscription", "event": "subscribed", "topic": "ch-1", "cursor": 42})
            async def do_subscribe():
                await client.subscribe("ch-1", wait=True)
            t = asyncio.create_task(do_subscribe())
            await asyncio.sleep(0.01)
            await confirm()
            await asyncio.wait_for(t, timeout=1)
            assert "ch-1" in client._subscribed
        asyncio.run(run())

    def test_unsubscribe_sends_json(self):
        client = _make_client()
        client._connected = True
        client._ws = MagicMock()
        client._ws.send_json = AsyncMock()
        asyncio.run(client.unsubscribe("ch-1"))
        client._ws.send_json.assert_awaited_once_with({"type": "unsubscribe", "topic": "ch-1"})

    def test_publish_sends_broadcast_and_waits_for_ack(self):
        client = _make_client()
        client._connected = True
        client._ws = MagicMock()

        async def run():
            sent = asyncio.get_running_loop().create_future()

            async def capture(frame):
                sent.set_result(dict(frame))

            client._ws.send_json = AsyncMock(side_effect=capture)
            task = asyncio.create_task(client.publish("ch-1", {"msg": "hello"}))
            frame = await asyncio.wait_for(sent, timeout=1)
            assert frame["type"] == "broadcast"
            assert "request_id" in frame
            # Server echoes the request_id in the Ack.
            await client._dispatch({
                "type": "ack", "id": "ack-1", "channel_id": "ch-1",
                "event_type": "broadcast", "payload": {"msg": "hello"},
                "actor": "", "cursor": 5, "channel_cursor": 2,
                "timestamp": "2026-01-01T00:00:00Z",
                "request_id": frame["request_id"],
            })
            event = await asyncio.wait_for(task, timeout=1)
            assert event.cursor == 5
            assert event.channel_cursor == 2
            client._ws.send_json.assert_awaited_once()
        asyncio.run(run())

    def test_publish_forwards_operation_id(self):
        client = _make_client()
        client._connected = True
        client._ws = MagicMock()

        async def run():
            sent = asyncio.get_running_loop().create_future()

            async def capture(frame):
                sent.set_result(dict(frame))

            client._ws.send_json = AsyncMock(side_effect=capture)
            task = asyncio.create_task(client.publish("ch-1", {}, operation_id="op-9"))
            frame = await asyncio.wait_for(sent, timeout=1)
            assert frame["operation_id"] == "op-9"
            await client._dispatch({
                "type": "ack", "id": "ack-1", "channel_id": "ch-1",
                "event_type": "broadcast", "payload": {}, "actor": "",
                "cursor": 1, "channel_cursor": 1,
                "timestamp": "2026-01-01T00:00:00Z",
                "request_id": frame["request_id"],
            })
            await asyncio.wait_for(task, timeout=1)

            # Omitted operation_id must not appear in the frame.
            sent2 = asyncio.get_running_loop().create_future()

            async def capture2(frame):
                sent2.set_result(dict(frame))

            client._ws.send_json = AsyncMock(side_effect=capture2)
            task2 = asyncio.create_task(client.publish("ch-1", {}))
            frame2 = await asyncio.wait_for(sent2, timeout=1)
            assert "operation_id" not in frame2
            await client._dispatch({
                "type": "ack", "id": "ack-2", "channel_id": "ch-1",
                "event_type": "broadcast", "payload": {}, "actor": "",
                "cursor": 2, "channel_cursor": 2,
                "timestamp": "2026-01-01T00:00:00Z",
                "request_id": frame2["request_id"],
            })
            await asyncio.wait_for(task2, timeout=1)
        asyncio.run(run())

    def test_publish_stale_ack_not_consumed(self):
        """A delayed ack for a timed-out publish must be discarded, not
        consumed by the next publish (audit item 8)."""
        client = _make_client()
        client._connected = True
        client._timeout = 0.2
        client._ws = MagicMock()

        async def run():
            sent = asyncio.get_running_loop().create_future()

            async def capture(frame):
                if not sent.done():
                    sent.set_result(dict(frame))

            client._ws.send_json = AsyncMock(side_effect=capture)

            # Publish A gets no ack → times out.
            with pytest.raises(APIError):
                await client.publish("ch-a", {"pub": "A"})
            ridA = (await asyncio.wait_for(sent, timeout=1))["request_id"]

            # A's stale ack arrives late — no pending future → discarded.
            await client._dispatch({
                "type": "ack", "id": "ack-A", "channel_id": "ch-a",
                "event_type": "broadcast", "payload": {"pub": "A"}, "actor": "",
                "cursor": 1, "channel_cursor": 1,
                "timestamp": "2026-01-01T00:00:00Z",
                "request_id": ridA,
            })
            assert client._ack_queue.empty()

            # Publish B must receive its own ack, not A's stale one.
            sentB = asyncio.get_running_loop().create_future()

            async def captureB(frame):
                sentB.set_result(dict(frame))

            client._ws.send_json = AsyncMock(side_effect=captureB)
            taskB = asyncio.create_task(client.publish("ch-b", {"pub": "B"}))
            frameB = await asyncio.wait_for(sentB, timeout=1)
            await client._dispatch({
                "type": "ack", "id": "ack-B", "channel_id": "ch-b",
                "event_type": "broadcast", "payload": {"pub": "B"}, "actor": "",
                "cursor": 2, "channel_cursor": 2,
                "timestamp": "2026-01-01T00:00:00Z",
                "request_id": frameB["request_id"],
            })
            ev = await asyncio.wait_for(taskB, timeout=1)
            assert ev.channel_id == "ch-b"
            assert ev.id == "ack-B"
        asyncio.run(run())


class TestReconnectLoop:
    def test_reconnect_loop_returns_if_connected(self):
        client = _make_client()
        client._connected = True
        client._auto_reconnect = True
        client._close_event = asyncio.Event()
        asyncio.run(client._reconnect_loop())  # Should return immediately

    def test_reconnect_loop_returns_if_no_auto_reconnect(self):
        client = _make_client()
        client._connected = False
        client._auto_reconnect = False
        client._close_event = asyncio.Event()
        asyncio.run(client._reconnect_loop())  # Should return immediately

    def test_reconnect_loop_gives_up_after_max_failures(self):
        client = _make_client()
        client._connected = False
        client._auto_reconnect = True
        client._close_event = asyncio.Event()
        client._max_reconnect_failures = 1
        client._reconnect_failures = 1  # Already at max

        async def run():
            await asyncio.wait_for(client._reconnect_loop(), timeout=5)
            assert client._auto_reconnect is False
        asyncio.run(run())


class TestJitteredDelay:
    def test_stays_within_plus_minus_fifty_percent(self):
        for delay in (0.5, 1.0, 5.0, 30.0):
            for _ in range(50):
                got = _jittered_delay(delay)
                assert delay * 0.5 <= got <= delay * 1.5, (delay, got)

    def test_never_drops_below_floor(self):
        for _ in range(100):
            assert _jittered_delay(0.05) >= 0.025
            assert _jittered_delay(0.01) > 0.0

    def test_produces_variation(self):
        values = {_jittered_delay(1.0) for _ in range(50)}
        assert len(values) > 1, "jitter must vary the delay"


class TestReader:
    def test_reader_cancelled_cleanly(self):
        client = _make_client()
        client._auto_reconnect = False
        client._close_event = asyncio.Event()

        class CancelledWs:
            def __aiter__(self):
                return self._gen()

            async def _gen(self):
                if False:
                    yield  # pragma: no cover — make this a real async generator
                raise asyncio.CancelledError()

        client._ws = CancelledWs()
        asyncio.run(client._reader())  # Should not raise

    def test_reader_disconnect_triggers_reconnect(self):
        from actae_client.errors import ActaeConnectionError

        client = _make_client()
        client._ws = AsyncMsgIterator([_close_msg()])
        client._auto_reconnect = True
        client._close_event = asyncio.Event()
        client._max_reconnect_failures = 1
        client._reconnect_failures = 1  # Already at max, so reconnect gives up immediately
        asyncio.run(client._reader())
        assert client._connected is False


class TestWebSocketLifecycle:
    def test_multiple_subscriptions(self):
        client = _make_client()
        client._connected = True
        client._ws = MagicMock()
        client._ws.send_json = AsyncMock()
        asyncio.run(client.subscribe("ch-1", wait=False))
        asyncio.run(client.subscribe("ch-2", wait=False))
        assert client._ws.send_json.await_count == 2

    def test_subscribe_and_unsubscribe(self):
        client = _make_client()
        client._connected = True
        client._ws = MagicMock()
        client._ws.send_json = AsyncMock()
        asyncio.run(client.subscribe("ch-1", wait=False))
        asyncio.run(client.unsubscribe("ch-1"))
        assert client._ws.send_json.await_count == 2

    def test_subscribe_with_wait_timeout(self):
        from actae_client.errors import ActaeConnectionError

        client = _make_client()
        client._connected = True
        client._ws = MagicMock()
        client._ws.send_json = AsyncMock()

        async def run():
            with pytest.raises(ActaeConnectionError, match="timed out"):
                def _timed_out(coro, **kwargs):
                    coro.close()  # the patched wait_for never awaits it
                    raise asyncio.TimeoutError()
                with patch("actae_client.client.asyncio.wait_for", side_effect=_timed_out):
                    await client.subscribe("ch-1", wait=True)
        asyncio.run(run())


class TestPublishAck:
    def test_publish_ack_timeout(self):
        from actae_client.errors import APIError

        client = _make_client()
        client._connected = True
        client._ws = MagicMock()
        client._ws.send_json = AsyncMock()
        client._timeout = 0.05

        async def run():
            with pytest.raises(APIError, match="No Ack"):
                await client.publish("ch-1", {})
        asyncio.run(run())


class MockWsWithIter:
    """A mock WebSocket replacement with a real __aiter__ that yields CLOSED."""

    def __init__(self):
        self.send_json = AsyncMock()
        self.close = AsyncMock()

    def __aiter__(self):
        return self._gen()

    async def _gen(self):
        yield MagicMock(type=aiohttp.WSMsgType.CLOSED)


class _NoopReader:
    """Patched _reader that does nothing — prevents reader task issues in tests."""

    async def noop_reader(self):
        pass


class TestDoConnect:
    def test_do_connect_sends_auth(self):
        client = _make_client()
        client._session = MagicMock()
        client._session.closed = False
        mock_ws = MagicMock()
        mock_ws.send_json = AsyncMock()
        mock_ws.close = AsyncMock()
        client._session.ws_connect = AsyncMock(return_value=mock_ws)

        async def run():
            with patch.object(client, "_reader", _NoopReader.noop_reader):
                # After _do_connect creates _auth_event, set it immediately
                async def patched_connect():
                    await client._do_connect()
                    # _do_connect creates new events, so we set them from here
                # Actually _do_connect sets _auth_event and _close_event internally
                # We need to set them AFTER _do_connect creates them
                # The easiest way: have the noop reader set the event
                orig_reader = client._reader
                async def reader_with_auth():
                    client._auth_event.set()
                with patch.object(client, "_reader", reader_with_auth):
                    await client._do_connect()
            mock_ws.send_json.assert_awaited_once()
            sent = mock_ws.send_json.call_args[0][0]
            assert sent["type"] == "auth"
            assert sent["api_key"] == "sk-test"

        asyncio.run(run())

    def test_do_connect_sets_connection_id(self):
        client = _make_client()
        client._session = MagicMock()
        client._session.closed = False
        mock_ws = MagicMock()
        mock_ws.send_json = AsyncMock()
        mock_ws.close = AsyncMock()
        client._session.ws_connect = AsyncMock(return_value=mock_ws)

        async def run():
            async def reader_with_auth():
                client._auth_event.set()
            with patch.object(client, "_reader", reader_with_auth):
                await client._do_connect()
            assert client._connected is True

        asyncio.run(run())

    def test_do_connect_creates_session_if_needed(self):
        client = _make_client()
        client._session = None

        mock_ws = MagicMock()
        mock_ws.send_json = AsyncMock()
        mock_ws.close = AsyncMock()

        with patch("aiohttp.ClientSession") as mock_cls:
            mock_session = MagicMock()
            mock_session.closed = False
            mock_session.ws_connect = AsyncMock(return_value=mock_ws)
            mock_cls.return_value = mock_session

            async def run():
                async def reader_with_auth():
                    client._auth_event.set()
                with patch.object(client, "_reader", reader_with_auth):
                    await client._do_connect()
                mock_cls.assert_called_once()

            asyncio.run(run())

    def test_do_connect_heartbeat_shorter_than_receive_timeout(self):
        """heartbeat must stay strictly below the per-receive timeout, or a
        quiet topic kills the connection every `timeout` seconds when the
        deadline races the pong (reconnect churn)."""
        client = _make_client()
        client._session = None
        mock_ws = MagicMock()
        mock_ws.send_json = AsyncMock()
        mock_ws.close = AsyncMock()

        with patch("aiohttp.ClientSession") as mock_cls:
            mock_session = MagicMock()
            mock_session.closed = False
            mock_session.ws_connect = AsyncMock(return_value=mock_ws)
            mock_cls.return_value = mock_session

            async def run():
                async def reader_with_auth():
                    client._auth_event.set()
                with patch.object(client, "_reader", reader_with_auth):
                    await client._do_connect()
                kwargs = mock_session.ws_connect.await_args.kwargs
                assert kwargs["heartbeat"] < kwargs["timeout"].ws_receive
                assert kwargs["heartbeat"] == 15.0  # default 30s timeout / 2

            asyncio.run(run())

    def test_do_connect_heartbeat_follows_short_timeout(self):
        client = _make_client()
        client._session = None
        client._timeout = 8.0
        mock_ws = MagicMock()
        mock_ws.send_json = AsyncMock()
        mock_ws.close = AsyncMock()

        with patch("aiohttp.ClientSession") as mock_cls:
            mock_session = MagicMock()
            mock_session.closed = False
            mock_session.ws_connect = AsyncMock(return_value=mock_ws)
            mock_cls.return_value = mock_session

            async def run():
                async def reader_with_auth():
                    client._auth_event.set()
                with patch.object(client, "_reader", reader_with_auth):
                    await client._do_connect()
                kwargs = mock_session.ws_connect.await_args.kwargs
                assert kwargs["heartbeat"] == 5.0  # floor clamp: 8/2=4 -> 5
                assert kwargs["heartbeat"] < kwargs["timeout"].ws_receive

            asyncio.run(run())

    def test_do_connect_auth_timeout(self):
        from actae_client.errors import AuthError

        client = _make_client()
        client._session = MagicMock()
        client._session.closed = False
        mock_ws = MagicMock()
        mock_ws.send_json = AsyncMock()
        mock_ws.close = AsyncMock()
        # Give the mock WS an aiter that yields CLOSED after a pause
        # so the reader task exits cleanly instead of hanging
        async def ws_iter():
            yield MagicMock(type=aiohttp.WSMsgType.CLOSED)
        mock_ws.__aiter__.return_value = ws_iter()
        client._session.ws_connect = AsyncMock(return_value=mock_ws)
        client._auth_event = asyncio.Event()
        client._timeout = 0.05

        async def run():
            with pytest.raises(AuthError, match="timed out"):
                await client._do_connect()

        asyncio.run(run())


class AsyncMsgIterator:
    """Helper: creates an async iterator from a list of message factories.

    Avoids MagicMock special-method interference with __aiter__.
    """

    def __init__(self, msgs):
        self._msgs = list(msgs)

    def __aiter__(self):
        return self._gen()

    async def _gen(self):
        for m in self._msgs:
            yield m


def _text_msg(data: str):
    msg = MagicMock()
    msg.type = aiohttp.WSMsgType.TEXT
    msg.data = data
    return msg


def _close_msg():
    msg = MagicMock()
    msg.type = aiohttp.WSMsgType.CLOSED
    return msg


def _error_msg():
    msg = MagicMock()
    msg.type = aiohttp.WSMsgType.ERROR
    return msg


class TestReaderMessageProcessing:
    def test_reader_processes_json_message(self):
        import json

        client = _make_client()
        client._auto_reconnect = False
        client._close_event = asyncio.Event()
        client._ws = AsyncMsgIterator([
            _text_msg(json.dumps({
                "type": "subscription",
                "event": "subscribed",
                "topic": "ch-1",
                "cursor": 42,
            })),
            _close_msg(),
        ])
        asyncio.run(client._reader())
        assert client._subscribed.get("ch-1") == 42

    def test_reader_handles_invalid_json(self):
        client = _make_client()
        client._auto_reconnect = False
        client._close_event = asyncio.Event()
        client._ws = AsyncMsgIterator([
            _text_msg("not valid json {{{"),
            _close_msg(),
        ])
        asyncio.run(client._reader())  # Should not raise

    def test_reader_processes_externally_tagged(self):
        import json

        client = _make_client()
        client._auto_reconnect = False
        client._close_event = asyncio.Event()
        client._ws = AsyncMsgIterator([
            _text_msg(json.dumps({
                "Broadcast": {"topic": "ch-1", "payload": {"event": {"id": "e1", "channel_id": "ch-1", "type": "broadcast", "payload": {}, "actor": "", "cursor": 5, "timestamp": ""}}}
            })),
            _close_msg(),
        ])

        received = []
        client.on_message(lambda topic, event: received.append((topic, event)))
        asyncio.run(client._reader())
        assert len(received) == 1
        assert received[0][1].cursor == 5

    def test_reader_disconnect_triggers_callback(self):
        client = _make_client()
        client._auto_reconnect = False
        client._close_event = asyncio.Event()
        client._connected = True  # Reader only fires callback if was connected
        client._ws = AsyncMsgIterator([_close_msg()])

        disconnected = []
        client._on_disconnected_cb = lambda: disconnected.append(True)
        asyncio.run(client._reader())
        assert len(disconnected) == 1

    def test_reader_error_sets_disconnected(self):
        client = _make_client()
        client._auto_reconnect = False
        client._close_event = asyncio.Event()
        client._ws = AsyncMsgIterator([_error_msg()])
        asyncio.run(client._reader())
        assert client._connected is False


class TestConnectionId:
    def test_connected_sets_connection_id(self):
        client = _make_client()
        data = {"type": "connection", "event": "connected", "payload": {"connection_id": "cid-1"}}
        asyncio.run(client._dispatch(data))
        assert client._connection_id == "cid-1"

    def test_authenticated_preserves_connection_id(self):
        client = _make_client()
        client._connection_id = "existing-cid"
        client._auth_event = asyncio.Event()
        data = {"type": "connection", "event": "authenticated", "payload": {"connection_id": "new-cid"}}
        asyncio.run(client._dispatch(data))
        assert client._connection_id == "new-cid"

    def test_pong_noop(self):
        client = _make_client()
        data = {"type": "connection", "event": "pong", "payload": {}}
        asyncio.run(client._dispatch(data))  # Should not raise or change state
