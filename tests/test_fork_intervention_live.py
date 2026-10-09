"""Live fork-intervention tests — side-effect-safe counterfactuals.

Proves, against a real Actae server, the AgentSession ergonomics added for
intervention + side-effect-aware forking:

  - ``session.intervention`` survives ``fork(...)`` and is recorded on the
    child's experiment metadata (Actae stores it; the app applies it);
  - the fork receipt exposes ``tool_policies``;
  - ``auto`` (default) replays an inherited tool result on the child instead
    of re-firing the effect;
  - ``block`` refuses a new irreversible call (``ForkToolBlockedError``) and
    persists a ``tool.blocked`` event;
  - ``resume(fork_at_step=...)`` forwards the same intervention/policies.

    ACTAE_URL=http://localhost:8002 ACTAE_API_KEY=sk-dev-0000000000000000000000 \
        python3 -m pytest sdks/python/tests/test_fork_intervention_live.py -v
"""

import asyncio
import os
import uuid

import pytest

from actae_client import ActaeClient
from actae_client.errors import ForkToolBlockedError
from actae_client.session import AgentSession

_ENDPOINT = os.environ.get("ACTAE_URL") or os.environ.get("ACTAE_ENDPOINT")
_API_KEY = os.environ.get("ACTAE_API_KEY")

pytestmark = pytest.mark.skipif(
    not (_ENDPOINT and _API_KEY), reason="ACTAE_URL/ACTAE_API_KEY not set"
)


def _client() -> ActaeClient:
    return ActaeClient(api_key=_API_KEY, endpoint=_ENDPOINT)


def _chan(prefix: str) -> str:
    return f"fork-intervention-live-{prefix}-{uuid.uuid4().hex[:8]}"


def _state_fn(decisions):
    def _fn():
        return {"decisions": list(decisions)}

    return _fn


async def _baseline(client: ActaeClient, name: str) -> AgentSession:
    decisions = []
    session = AgentSession(
        client, name, params={"model": "base"}, state_fn=_state_fn(decisions), snapshot_interval=1
    )
    async with session:
        decisions.append({"step": 1, "action": "research"})
        await session.step("research", output={"sources": 3})
        decisions.append({"step": 2, "action": "send_email"})
        claim = await client.claim_execution(
            session.channel_id, "email-1", "email.send", {"to": "alice@example.com"}
        )
        assert claim.status == "claimed"
        await client.complete_execution(
            claim.execution.id, claim.claim_token, result={"message_id": "msg-aaa"}
        )
        await session.step("send_email", output={"message_id": "msg-aaa"})
    return session


def test_auto_replays_inherited_effect():
    async def run():
        client = _client()
        await client.connect()
        try:
            base = await _baseline(client, _chan("auto-base"))
            child = await base.fork(2, _chan("auto-child"))
            claim = await client.claim_execution(
                child.channel_id, "email-1", "email.send", {"to": "alice@example.com"}
            )
            assert claim.status == "replayed"
            assert claim.result == {"message_id": "msg-aaa"}
            started = await client.replay(
                child.channel_id, cursor=0, limit=200, event_type="tool.started"
            )
            assert started == []
        finally:
            await client.disconnect()

    asyncio.run(run())


def test_block_refuses_and_persists_boundary():
    async def run():
        client = _client()
        await client.connect()
        try:
            base = await _baseline(client, _chan("block-base"))
            child = await base.fork(
                2,
                _chan("block-child"),
                tool_policies={"email.send": "block"},
            )
            with pytest.raises(ForkToolBlockedError):
                await client.claim_execution(
                    child.channel_id, "email-2", "email.send", {"to": "bob@example.com"}
                )
            blocked = await client.replay(
                child.channel_id, cursor=0, limit=200, event_type="tool.blocked"
            )
            assert len(blocked) == 1
            assert blocked[0].payload["policy"] == "block"
        finally:
            await client.disconnect()

    asyncio.run(run())


def test_intervention_and_receipt_survive_fork_and_resume():
    async def run():
        client = _client()
        await client.connect()
        try:
            base = await _baseline(client, _chan("int-base"))
            child_name = _chan("int-child")
            intervention = {"model": "candidate-model", "temperature": 0.2}
            child = await base.fork(
                1,
                child_name,
                intervention=intervention,
                tool_policies={"email.send": "replay"},
            )
            assert child.intervention == intervention
            assert child.tool_policies == {"email.send": "replay"}
            receipt = await client.get_fork_receipt(child_name)
            assert receipt.tool_policies == {"email.send": "replay"}

            meta = await client.get_channel_metadata(child_name)
            assert (meta.experiment_metadata or {})["intervention"] == intervention

            # resume(fork_at_step=) forwards intervention + policies too.
            resumed = await AgentSession.resume(
                client,
                base.channel_id,
                fork_at_step=1,
                name=_chan("int-resume"),
                intervention={"model": "resumed-model"},
                tool_policies={"email.send": "block"},
            )
            assert resumed.intervention == {"model": "resumed-model"}
            assert resumed.tool_policies == {"email.send": "block"}
        finally:
            await client.disconnect()

    asyncio.run(run())
