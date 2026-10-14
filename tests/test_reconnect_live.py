"""Live reconnect-watermark tests — no duplicate/gap delivery after a drop.

Requires a running Actae server; skipped otherwise:

    ACTAE_URL=http://localhost:8002 ACTAE_API_KEY=sk-dev-0000000000000000000000 \
        python3 -m pytest sdks/python/tests/test_reconnect_live.py -v

`ACTAE_ENDPOINT` is accepted as an alias for `ACTAE_URL`. Uses TWO client
connections: the server does not echo broadcasts back to the publishing
connection, so delivery is verified on a separate subscriber.
"""

import asyncio
import os
import uuid

import pytest

from actae_client import ActaeClient

_ENDPOINT = os.environ.get("ACTAE_URL") or os.environ.get("ACTAE_ENDPOINT")
_API_KEY = os.environ.get("ACTAE_API_KEY")

pytestmark = pytest.mark.skipif(
    not (_ENDPOINT and _API_KEY), reason="ACTAE_URL/ACTAE_API_KEY not set"
)


def _make_client():
    return ActaeClient(api_key=_API_KEY, endpoint=_ENDPOINT)


def _channel(prefix):
    return f"reconnect-live-{prefix}-{uuid.uuid4().hex[:8]}"


async def _wait_for(predicate, timeout=10.0, interval=0.1):
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if predicate():
            return True
        await asyncio.sleep(interval)
    return False


def test_no_duplicate_or_gap_delivery_after_reconnect():
    """Drop the subscriber's WebSocket mid-stream; after auto-reconnect it
    must receive exactly the events it missed (cursor > last seen) — no
    redelivery of already-seen events, no gaps."""
    async def run():
        sub = _make_client()
        pub = _make_client()
        async with sub, pub:
            channel = _channel("dedup")
            received = []
            sub.on_message(lambda topic, ev: received.append(ev.cursor))
            await sub.subscribe(channel, wait=True)

            # Publisher is a separate connection, so these broadcasts reach sub.
            for i in range(3):
                await pub.publish(channel, {"i": i})
            await _wait_for(lambda: len(received) == 3, timeout=10.0)
            assert len(received) == 3, f"expected 3 events, got {received}"

            # Force-drop the subscriber connection; auto-reconnect kicks in.
            await sub._ws.close()
            reconnected = await _wait_for(lambda: sub._connected, timeout=15.0)
            assert reconnected, "subscriber did not reconnect"

            for i in range(3, 6):
                await pub.publish(channel, {"i": i})
            await _wait_for(lambda: len(received) == 6, timeout=10.0)

            # Exactly the 6 published events, no duplicates (global cursors
            # are shared across channels, so they are not 0..5).
            assert len(received) == 6, f"expected 6 events, got {received}"
            assert len(set(received)) == 6, f"duplicates: {received}"
            assert received == sorted(received), f"out of order: {received}"
            # The post-reconnect batch overlaps nothing from the pre-drop batch.
            assert set(received[:3]).isdisjoint(set(received[3:])), f"got {received}"

    asyncio.run(run())


def test_replay_catch_up_on_subscribe_with_cursor():
    """subscribe(topic, cursor=N) replays events with cursor > N only
    (exclusive semantics) — no re-delivery of the event at cursor N."""
    async def run():
        pub = _make_client()
        sub = _make_client()
        async with pub, sub:
            channel = _channel("cursor")
            for i in range(4):
                await pub.publish(channel, {"i": i})
            await asyncio.sleep(0.3)  # let them land

            received = []
            sub.on_message(lambda topic, ev: received.append(ev.cursor))
            latest = await sub.get_cursor(channel)
            assert latest is not None and latest >= 3
            # Replay from the latest seen cursor: nothing should be delivered
            # (all events are <= latest, and replay is exclusive).
            await sub.subscribe(channel, cursor=latest, wait=True)
            await asyncio.sleep(0.5)
            assert received == [], f"expected no redelivery, got {received}"

            await pub.publish(channel, {"i": "new"})
            await _wait_for(lambda: len(received) == 1, timeout=10.0)
            assert len(received) == 1, f"expected 1 event, got {received}"

    asyncio.run(run())
