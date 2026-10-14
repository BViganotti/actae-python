"""Live human-approval pattern tests — durable human-in-the-loop.

Proves, against a real Actae server, that an agent can pause for a human
decision without holding a process open:

  - ``request_approval`` records an ``approval.requested`` event and schedules
    a durable wake-up when a timeout is set;
  - ``decide_approval`` records an ``approval.decided`` event;
  - ``wait_for_approval`` observes the decision;
  - a timeout records ``approval.expired`` and returns ``None``;
  - ``AgentSession.require_approval`` packages the whole flow.

    ACTAE_URL=http://localhost:8002 ACTAE_API_KEY=sk-dev-0000000000000000000000 \
        python3 -m pytest sdks/python/tests/test_approval_pattern.py -v
"""

import asyncio
import os
import uuid

import pytest

from actae_client import ActaeClient
from actae_client.client import APPROVAL_DECIDED, APPROVAL_EXPIRED, APPROVAL_REQUESTED
from actae_client.session import AgentSession

_ENDPOINT = os.environ.get("ACTAE_URL") or os.environ.get("ACTAE_ENDPOINT")
_API_KEY = os.environ.get("ACTAE_API_KEY")

pytestmark = pytest.mark.skipif(
    not (_ENDPOINT and _API_KEY), reason="ACTAE_URL/ACTAE_API_KEY not set"
)


def _client() -> ActaeClient:
    return ActaeClient(api_key=_API_KEY, endpoint=_ENDPOINT)


def _chan(prefix: str) -> str:
    return f"approval-{prefix}-{uuid.uuid4().hex[:8]}"


def _events(client, channel, event_type):
    return asyncio.run(client.replay(channel, cursor=0, limit=500, event_type=event_type))


def test_request_decide_wait():
    async def run():
        client = _client()
        await client.connect()
        try:
            ch = _chan("decide")
            request_id = await client.request_approval(
                ch, summary="Send the contract to legal", details={"amount": 1200}
            )
            requested = await client.replay(
                ch, cursor=0, limit=50, event_type=APPROVAL_REQUESTED
            )
            assert len(requested) == 1
            assert requested[0].payload["request_id"] == request_id

            decision = await client.decide_approval(
                ch, request_id, decision="approved", actor="alice", reason="looks good"
            )
            assert decision.event_type == APPROVAL_DECIDED

            event = await client.wait_for_approval(ch, request_id, timeout_seconds=5)
            assert event is not None
            assert event.payload["decision"] == "approved"
            assert event.payload["reason"] == "looks good"
        finally:
            await client.disconnect()

    asyncio.run(run())


def test_decide_is_idempotent_per_decision():
    """A retried/duplicated decision replays; a changed decision is a new event."""

    async def run():
        client = _client()
        await client.connect()
        try:
            ch = _chan("idem")
            request_id = await client.request_approval(ch, summary="Approve invoice")
            first = await client.decide_approval(
                ch, request_id, decision="approved", actor="alice"
            )
            # Same decision again → the server replays the original event.
            retry = await client.decide_approval(
                ch, request_id, decision="approved", actor="alice"
            )
            assert retry.id == first.id
            decided = await client.replay(
                ch, cursor=0, limit=50, event_type=APPROVAL_DECIDED
            )
            assert len(decided) == 1

            # A different decision derives a different key and is recorded anew
            # (the application applies last-wins).
            await client.decide_approval(
                ch, request_id, decision="rejected", actor="bob"
            )
            decided = await client.replay(
                ch, cursor=0, limit=50, event_type=APPROVAL_DECIDED
            )
            assert len(decided) == 2
        finally:
            await client.disconnect()

    asyncio.run(run())


def test_timeout_records_expired_and_schedules_wakeup():
    async def run():
        client = _client()
        await client.connect()
        try:
            ch = _chan("timeout")
            request_id = await client.request_approval(
                ch, summary="Approve deployment", timeout_seconds=1
            )
            event = await client.wait_for_approval(
                ch, request_id, timeout_seconds=1, poll_interval=0.2
            )
            assert event is None
            expired = await client.replay(
                ch, cursor=0, limit=50, event_type=APPROVAL_EXPIRED
            )
            assert len(expired) == 1
            assert expired[0].payload["request_id"] == request_id

            wakeups = await client.list_wakeups(channel_id=ch, status="pending")
            assert any(
                w.payload and w.payload.get("approval_request_id") == request_id
                for w in wakeups
            )
            for w in wakeups:
                await client.cancel_wakeup(w.id)
        finally:
            await client.disconnect()

    asyncio.run(run())


def test_agent_session_require_approval():
    async def run():
        client = _client()
        await client.connect()
        try:
            ch = _chan("session")
            session = AgentSession(client, ch)
            await session.__aenter__()

            async def approver():
                # A human/service resolves the request it observes on the channel.
                for _ in range(50):
                    reqs = await client.replay(
                        ch, cursor=0, limit=50, event_type=APPROVAL_REQUESTED
                    )
                    if reqs:
                        request_id = reqs[0].payload["request_id"]
                        await client.decide_approval(
                            ch, request_id, decision="approved", actor="alice"
                        )
                        return
                    await asyncio.sleep(0.1)
                raise AssertionError("no approval.requested observed")

            try:
                result, _ = await asyncio.gather(
                    session.require_approval("Ship it?", timeout_seconds=5, poll_interval=0.1),
                    approver(),
                )
                assert result["approved"] is True
                assert result["decision"]["decision"] == "approved"
            finally:
                await session.__aexit__(None, None, None)
        finally:
            await client.disconnect()

    asyncio.run(run())
