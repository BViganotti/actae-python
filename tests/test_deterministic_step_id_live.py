"""Live deterministic step operation-id integration test.

Proves, against a real Actae server, the crash-recovery promise of the
deterministic per-step operation id (Python `_step_operation_id`, Go
`DeterministicOperationKey` + `canonicalStepContent`):

  - record step 1 on a channel, then "crash" and re-drive the SAME step
    (same step_number, same content) from a fresh session on the same
    channel: the derived id is identical, so the server REPLAYS the
    original event — replay() shows exactly ONE step event, and the
    returned Event is the original one,
  - re-drive with CHANGED content derives a different id and records a
    NEW event (replay() then shows both),
  - metadata key insertion order does NOT break idempotency (the canonical
    serialization sorts keys, so the same logical step with reordered
    metadata still derives the same id and replays).

    ACTAE_URL=http://localhost:8002 ACTAE_API_KEY=sk-dev-0000000000000000000000 \
        python3 -m pytest sdks/python/tests/test_deterministic_step_id_live.py -v
"""

import asyncio
import os
import uuid

import pytest

from actae_client import ActaeClient
from actae_client.session import AgentSession, _step_operation_id

_ENDPOINT = os.environ.get("ACTAE_URL") or os.environ.get("ACTAE_ENDPOINT")
_API_KEY = os.environ.get("ACTAE_API_KEY")

pytestmark = pytest.mark.skipif(
    not (_ENDPOINT and _API_KEY), reason="ACTAE_URL/ACTAE_API_KEY not set"
)


def _client():
    return ActaeClient(api_key=_API_KEY, endpoint=_ENDPOINT)


def _fresh_channel(prefix):
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


class TestDeterministicStepIDLive:
    def test_crash_recovery_redrive_replays(self):
        actae = _client()
        channel = _fresh_channel("det-redrive")

        async def run():
            # First run: step 1 with fixed content.
            s1 = AgentSession(actae, channel)
            async with s1:
                await s1.step("inference", input="q", output="a", metadata={"m": 1})

            # "Crash": a fresh session on the SAME channel re-drives the
            # same step 1 with identical content.
            s2 = AgentSession(actae, channel)
            async with s2:
                ev2 = await s2.step("inference", input="q", output="a", metadata={"m": 1})

            # Server-side replay: exactly ONE inference event on the channel,
            # and the returned Event IS the original (same id, same cursor).
            events = await actae.replay(channel, event_type="inference")
            assert len(events) == 1, [
                (e.id, e.cursor, e.event_type) for e in events
            ]
            assert ev2.id == events[0].id
            assert ev2.cursor == events[0].cursor

        asyncio.run(run())

    def test_changed_content_records_new_event(self):
        actae = _client()
        channel = _fresh_channel("det-changed")

        async def run():
            s1 = AgentSession(actae, channel)
            async with s1:
                await s1.step("inference", input="q", output="a")

            # Changed output -> distinct id -> new event, no replay.
            s2 = AgentSession(actae, channel)
            async with s2:
                await s2.step("inference", input="q", output="DIFFERENT")

            events = await actae.replay(channel, event_type="inference")
            assert len(events) == 2, [(e.id, e.cursor) for e in events]
            assert events[0].id != events[1].id

        asyncio.run(run())

    def test_key_insertion_order_does_not_break_idempotency(self):
        # The server replays by (channel_id, operation_id); the canonical
        # serialization sorts keys, so the SAME logical step recorded with
        # a different metadata insertion order must derive the SAME id and
        # replay — not duplicate. (The server does not echo operation ids
        # on GET /events; dedup is observable as "one event in replay".)
        actae = _client()
        channel = _fresh_channel("det-keyorder")

        async def run():
            s1 = AgentSession(actae, channel)
            async with s1:
                await s1.step("inference", input="q", output="a",
                              metadata={"m": 1, "n": 2})

            # Same logical step, metadata inserted in reverse order.
            s2 = AgentSession(actae, channel)
            async with s2:
                await s2.step("inference", input="q", output="a",
                              metadata={"n": 2, "m": 1})

            events = await actae.replay(channel, event_type="inference")
            assert len(events) == 1, [
                (e.id, e.cursor, e.metadata) for e in events
            ]

        asyncio.run(run())