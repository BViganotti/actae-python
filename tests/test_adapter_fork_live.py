"""Live fork tests for the Python SDK's framework adapters.

Covers the adapter-native fork entry points against a REAL ActaeClient:

  - CrewAIResumer.fork          (adapters/crewai.py)
  - ChainResumer.fork           (adapters/langchain.py)
  - ActaeTracingProcessor.fork_trace  (adapters/openai_agents.py)

These adapters' ``fork`` methods only need the ActaeClient — they do NOT
require the actual framework SDK at runtime (crewai.py imports only
``ActaeClient``; langchain.py falls back to ``object`` for the callback
base class; only ActaeTracingProcessor raises ImportError without
openai-agents). Env-gated live suite, same pattern as test_fork_resume_deep.py.

    ACTAE_URL=http://localhost:8002 ACTAE_API_KEY=sk-dev-0000000000000000000000 \
        python3 -m pytest sdks/python/tests/test_adapter_fork_live.py -v
"""

import asyncio
import os
import uuid
from types import SimpleNamespace

import pytest

from actae_client import ActaeClient

_ENDPOINT = os.environ.get("ACTAE_URL") or os.environ.get("ACTAE_ENDPOINT")
_API_KEY = os.environ.get("ACTAE_API_KEY")

pytestmark = pytest.mark.skipif(
    not (_ENDPOINT and _API_KEY), reason="ACTAE_URL/ACTAE_API_KEY not set"
)


def _client():
    return ActaeClient(api_key=_API_KEY, endpoint=_ENDPOINT)


def _chan(prefix):
    return f"adapter-fork-{prefix}-{uuid.uuid4().hex[:8]}"


# ---------------------------------------------------------------------------
# CrewAI
# ---------------------------------------------------------------------------

def test_crewai_resumer_fork_live():
    """CrewAIResumer.fork copies the completed-tasks state to a new channel
    and the fork evolves independently. No crewai needed: the adapter imports
    only ActaeClient and get_remaining_tasks matches plain strings."""
    asyncio.run(_test_crewai_fork("crewai"))


async def _test_crewai_fork(prefix):
    from actae_client.adapters.crewai import ActaeCrewStateHook, CrewAIResumer

    actae = _client()
    await actae.connect()
    try:
        base = _chan(prefix)
        fork_ch = f"{base}-fork"

        # A real event gives the snapshot a cursor to align to.
        await actae.record(base, "crew.task.completed", {"task": "task1"}, actor="crewai")

        hook = ActaeCrewStateHook(actae, base)
        state = {
            "completed_tasks": ["task1", "task2"],
            "task_outputs": [
                {"task": "task1", "output": "output-1"},
                {"task": "task2", "output": "output-2"},
            ],
            "crew_id": "crew-x",
        }
        await hook.save_state(state)
        assert await hook.load_state() == state

        resumer = CrewAIResumer(actae, base)
        forked = await resumer.fork(fork_ch, reason="refine")
        assert forked.channel == fork_ch

        # The fork inherits the completed-tasks dict verbatim.
        inherited = await actae.latest_state(fork_ch)
        assert inherited is not None
        assert inherited["state"]["completed_tasks"] == ["task1", "task2"]
        assert inherited["state"]["task_outputs"] == state["task_outputs"]
        assert inherited["state"]["crew_id"] == "crew-x"

        # get_remaining_tasks on the fork reflects the same completed set.
        remaining = await forked.get_remaining_tasks(["task1", "task2", "task3"])
        assert remaining == ["task3"]

        # The fork evolves independently: source state is unaffected.
        ev = await actae.record(fork_ch, "crew.task.completed", {"task": "task3"}, actor="crewai")
        await actae.save_state(fork_ch, ev.cursor, {
            "completed_tasks": ["task1", "task2", "task3"],
            "task_outputs": [
                {"task": "task1", "output": "output-1"},
                {"task": "task2", "output": "output-2"},
                {"task": "task3", "output": "output-3"},
            ],
            "crew_id": "crew-x",
        })
        assert await hook.load_state() == state, "source must be unaffected by fork writes"
        fork_latest = await actae.latest_state(fork_ch)
        assert fork_latest["state"]["completed_tasks"] == ["task1", "task2", "task3"]
    finally:
        await actae.disconnect()


# ---------------------------------------------------------------------------
# LangChain
# ---------------------------------------------------------------------------

def test_langchain_resumer_fork_live():
    """ChainResumer.fork copies the full context dict to a new channel. The
    adapter works without langchain installed (callback base falls back to
    object), so no importorskip is needed."""
    asyncio.run(_test_langchain_fork("langchain"))


async def _test_langchain_fork(prefix):
    from actae_client.adapters.langchain import ActaeContextSaver, ChainResumer

    actae = _client()
    await actae.connect()
    try:
        base = _chan(prefix)
        fork_ch = f"{base}-fork"

        ev = await actae.record(base, "chain.run.end", {"step": 1}, actor="langchain")
        context = {
            "messages": [
                {"role": "user", "content": "hi"},
                {"role": "assistant", "content": "hello"},
            ],
            "tool_outputs": [
                {"output": {"result": 42}, "run_id": "tool-1"},
            ],
            "chain_steps": [
                {"outputs": {"final": "out"}, "run_id": "chain-1"},
            ],
        }
        # Schema ActaeContextSaver._do_save persists: {"context": {...}}.
        await actae.save_state(base, ev.cursor, {"context": context})

        resumer = ChainResumer(actae, base)
        forked = await resumer.fork(fork_ch)
        assert forked.channel == fork_ch

        # The fork inherits the full context dict verbatim.
        inherited = await actae.latest_state(fork_ch)
        assert inherited is not None
        assert inherited["state"]["context"] == context

        # And the saver-side load_context surfaces it.
        saver = ActaeContextSaver(actae, fork_ch)
        assert await saver.load_context() == context

        # Independent evolution: source unaffected by fork writes.
        ev = await actae.record(fork_ch, "chain.run.end", {"step": 2}, actor="langchain")
        new_context = dict(context)
        new_context["messages"] = context["messages"] + [
            {"role": "user", "content": "more"}
        ]
        await actae.save_state(fork_ch, ev.cursor, {"context": new_context})
        assert (await actae.latest_state(base))["state"]["context"] == context
        fork_latest = await actae.latest_state(fork_ch)
        assert fork_latest["state"]["context"]["messages"][-1]["content"] == "more"
    finally:
        await actae.disconnect()


# ---------------------------------------------------------------------------
# OpenAI Agents SDK
# ---------------------------------------------------------------------------

def test_openai_agents_fork_trace_live():
    """ActaeTracingProcessor.fork_trace picks the snapshot of the requested
    run (not the newest) and raises ValueError for an unknown trace_id. The
    constructor raises ImportError without openai-agents, hence importorskip."""
    pytest.importorskip("agents")
    asyncio.run(_test_openai_fork("oa"))


async def _test_openai_fork(prefix):
    from actae_client.adapters.openai_agents import ActaeTracingProcessor

    actae = _client()
    await actae.connect()
    try:
        ch = _chan(prefix)
        fork_ch = f"{ch}-fork"
        proc = ActaeTracingProcessor(actae, channel=ch)
        try:
            trace_b = f"trace-b-{uuid.uuid4().hex[:8]}"
            trace_a = f"trace-a-{uuid.uuid4().hex[:8]}"

            # Run B first (older snapshot), run A second (the NEWEST).
            ev = await actae.record(
                ch, "openai.trace.end", {"trace_id": trace_b}, actor="openai-agents"
            )
            await actae.save_state(ch, ev.cursor, {
                "trace_id": trace_b,
                "spans": [{"type": "AgentSpanData", "name": "research", "span_id": "s1"}],
            })

            ev = await actae.record(
                ch, "openai.trace.end", {"trace_id": trace_a}, actor="openai-agents"
            )
            await actae.save_state(ch, ev.cursor, {
                "trace_id": trace_a,
                "spans": [{"type": "AgentSpanData", "name": "writer", "span_id": "s2"}],
            })

            # fork_trace must resolve to B's snapshot, NOT the newest (A).
            await proc.fork_trace(trace_b, fork_ch, reason="compare")
            inherited = await actae.latest_state(fork_ch)
            assert inherited is not None
            assert inherited["state"]["trace_id"] == trace_b
            assert inherited["state"]["spans"][0]["name"] == "research"

            # Unknown trace_id raises a clear ValueError.
            with pytest.raises(ValueError, match="no Actae snapshot"):
                await proc.fork_trace("trace-unknown", f"{fork_ch}-nope")
        finally:
            proc.shutdown()
    finally:
        await actae.disconnect()
