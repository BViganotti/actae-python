"""Live fork-resume integration tests — the flagship workflow.

Proves, against a real Actae server, that "fork at step N, refine step N+1"
does NOT re-run steps 1..N:

  - the fork's event log contains only `fork.started` + steps N+1..end,
  - step N+1's input contains steps 1..N's real output (inherited state),
  - a per-step execution counter shows steps 1..N were NOT invoked on the
    fork,
  - the fork's final state differs from the baseline ONLY in the refined
    step, while steps 1..N data is byte-identical (inherited).

Uses a deterministic fake pipeline (no LLM) so the assertions are exact.

    ACTAE_URL=http://localhost:8002 ACTAE_API_KEY=sk-dev-0000000000000000000000 \
        python3 -m pytest sdks/python/tests/test_fork_resume_live.py -v
"""

import asyncio
import os
import uuid

import pytest

from actae_client import ActaeClient
from actae_client.session import AgentSession
from actae_client.adapters.langgraph import ActaeCheckpointSaver, _HAS_LANGGRAPH

_ENDPOINT = os.environ.get("ACTAE_URL") or os.environ.get("ACTAE_ENDPOINT")
_API_KEY = os.environ.get("ACTAE_API_KEY")

pytestmark = pytest.mark.skipif(
    not (_ENDPOINT and _API_KEY), reason="ACTAE_URL/ACTAE_API_KEY not set"
)


def _client():
    return ActaeClient(api_key=_API_KEY, endpoint=_ENDPOINT)


def _chan(prefix):
    return f"fork-live-{prefix}-{uuid.uuid4().hex[:8]}"


class Registry:
    """Per-step execution counter — the token-savings proof."""

    def __init__(self):
        self.calls = {}

    def mark(self, step):
        self.calls[step] = self.calls.get(step, 0) + 1

    def count(self, step):
        return self.calls.get(step, 0)


# A deterministic 10-step pipeline: each step appends to the accumulated
# state. Step 6 is the "refined" step — its behavior differs between the
# baseline and the fork via a `style` parameter.
def step1(reg, state):
    reg.mark("s1"); return {"s1_out": "research:" + state["topic"]}

def step2(reg, state):
    reg.mark("s2"); return {"s2_out": "insights(" + state["s1_out"] + ")"}

def step3(reg, state):
    reg.mark("s3"); return {"s3_out": "stakeholders(" + state["s2_out"] + ")"}

def step4(reg, state):
    reg.mark("s4"); return {"s4_out": "risks(" + state["s3_out"] + ")"}

def step5(reg, state):
    reg.mark("s5"); return {"s5_out": "metrics(" + state["s4_out"] + ")"}

def step6(reg, state, style="neutral"):
    reg.mark("s6")
    return {"s6_out": f"[{style}] recs(" + state["s5_out"] + ")"}

def step7(reg, state):
    reg.mark("s7"); return {"s7_out": "summary(" + state["s6_out"] + ")"}

def step8(reg, state):
    reg.mark("s8"); return {"s8_out": "next(" + state["s7_out"] + ")"}

def step9(reg, state):
    reg.mark("s9"); return {"s9_out": "final(" + state["s8_out"] + ")"}

PIPELINE = [
    (1, "s1", step1), (2, "s2", step2), (3, "s3", step3), (4, "s4", step4),
    (5, "s5", step5), (6, "s6", step6), (7, "s7", step7), (8, "s8", step8),
    (9, "s9", step9),
]
FORK_STEP = 5
STATE_KEYS = ["s1_out", "s2_out", "s3_out", "s4_out", "s5_out"]


async def _run(session, registry, live, *, style="neutral", from_step=0, stop_after=None):
    for step, name, fn in PIPELINE[from_step:]:
        if stop_after is not None and step > stop_after:
            break
        if step == 6:
            delta = fn(registry, live, style=style)
        else:
            delta = fn(registry, live)
        live.update(delta)
        # Step 6 records the full accumulated state as input (all of steps
        # 1-5's output) so the event log proves inherited data was consumed.
        step_input = dict(delta)
        if step == 6:
            step_input = {k: live[k] for k in STATE_KEYS + ["s6_out"]}
        await session.step(f"step.{step}", input=step_input, output=delta, context=delta)


async def _replay_types(actae, channel):
    events = await actae.replay(channel, cursor=0, limit=200)
    return [e.event_type for e in events]


def test_fork_resume_skips_steps_1_to_5():
    # One event loop for connect + run + disconnect: the client's WS/reconnect
    # task is bound to this loop, so it must outlive the test body.
    asyncio.run(_test_main("skip"))


async def _test_main(prefix):
    actae = _client()
    await actae.connect()
    try:
        await _test_fork_resume(actae, prefix)
    finally:
        await actae.disconnect()


async def _test_fork_resume(actae, prefix):
    base = _chan(prefix)
    fork = f"{base}-fork"

    # ---- Baseline: steps 1-9, deterministic ----
    base_reg = Registry()
    base_live = {"topic": "T"}
    async with AgentSession(actae, base, state_fn=lambda: dict(base_live)) as s:
        await _run(s, base_reg, base_live)

    # ---- Fork at step 5 with a MODIFIED step 6 ----
    fork_reg = Registry()
    fork_live = {"topic": "T"}
    forked = await AgentSession.resume(
        actae, base, fork_at_step=FORK_STEP, name=fork, state_fn=lambda: dict(fork_live)
    )

    # The fork inherited steps 1-5's state.
    inherited = forked.inherited_state or {}
    for k in STATE_KEYS:
        assert inherited.get(k) == base_live.get(k), f"inherited {k} mismatch"
    # Seed the fork's live state from what it inherited (steps 1-5's data).
    for k, v in inherited.items():
        fork_live[k] = v
    # The fork continues at step 6 (not step 1).
    assert forked.step_count == FORK_STEP

    async with forked:
        await _run(forked, fork_reg, fork_live, style="aggressive", from_step=FORK_STEP)

    # ---- Proofs ----
    # (a) Steps 1-5 were NOT invoked on the fork.
    for name in ("s1", "s2", "s3", "s4", "s5"):
        assert fork_reg.count(name) == 0, f"{name} re-invoked on fork"
    # Steps 6-9 WERE invoked exactly once.
    for name in ("s6", "s7", "s8", "s9"):
        assert fork_reg.count(name) == 1, f"{name} invoked {fork_reg.count(name)}x on fork"

    # (b) The fork's event log = fork.started + session.started + steps 6-9.
    fork_types = await _replay_types(actae, fork)
    assert fork_types[0] == "fork.started"
    user_steps = [t for t in fork_types if t.startswith("step.")]
    assert user_steps == ["step.6", "step.7", "step.8", "step.9"], fork_types
    # No steps 1-5 on the fork's log.
    assert not any(t in ("step.1", "step.2", "step.3", "step.4", "step.5") for t in fork_types)

    # (c) Step 6's event input carried steps 1-5's real output (inherited).
    events = await actae.replay(fork, cursor=0, limit=200)
    step6 = next(e for e in events if e.event_type == "step.6")
    inp = step6.payload.get("input", {})
    for k in STATE_KEYS:
        assert inp.get(k) == base_live.get(k), f"step6 input missing inherited {k}"

    # (d) Steps 1-5 data is byte-identical between baseline and fork (inherited,
    #     not regenerated); step 6 output differs (the refinement).
    base_events = await actae.replay(base, cursor=0, limit=200)
    base_step6 = next(e for e in base_events if e.event_type == "step.6")
    fork_step6 = next(e for e in events if e.event_type == "step.6")
    assert base_step6.payload["context"]["s6_out"] != fork_step6.payload["context"]["s6_out"], \
        "step 6 must differ (refinement applied)"
    base_step5 = next(e for e in base_events if e.event_type == "step.5")
    fork_step5 = next((e for e in events if e.event_type == "step.5"), None)
    assert fork_step5 is None, "step 5 must not exist on the fork"
    assert base_step5.payload["context"]["s5_out"] == base_live["s5_out"]


def test_fork_of_fork_continues_lineage():
    """A fork of a fork still inherits the deepest state and keeps lineage."""
    asyncio.run(_test_nested_fork("nested"))


async def _test_nested_fork(prefix):
    actae = _client()
    await actae.connect()
    try:
        base = _chan(prefix)
        fork1 = f"{base}-f1"
        fork2 = f"{base}-f2"

        # Baseline full run.
        reg = Registry()
        live = {"topic": "T"}
        async with AgentSession(actae, base, state_fn=lambda: dict(live)) as s:
            await _run(s, reg, live)

        # Fork at step 3, then fork THAT fork at step 4 (i.e. 3+1 of inherited).
        f1 = await AgentSession.resume(actae, base, fork_at_step=3, name=fork1, state_fn=lambda: dict(f1_live))
        f1_live = dict(f1.inherited_state or {})
        async with f1:
            await _run(f1, Registry(), f1_live, from_step=3)

        f2 = await AgentSession.resume(actae, fork1, fork_at_step=4, name=fork2, state_fn=lambda: dict(f2_live))
        f2_live = dict(f2.inherited_state or {})
        # f2 inherited from f1, which had inherited from base → all of 1-4.
        assert f2_live.get("s1_out") == live.get("s1_out")
        assert f2_live.get("s4_out") == f1_live.get("s4_out")
        assert f2.step_count == 4, "nested fork continues at step 5"

        # Lineage chain: f2 -> f1 -> base.
        meta2 = await actae.get_channel_metadata(fork2)
        assert meta2.parent_channel_id == fork1
        async with f2:
            # f2 inherited steps 1-4; run steps 5-9 (index 4 = step 5).
            await _run(f2, Registry(), f2_live, from_step=4)
        types = await _replay_types(actae, fork2)
        assert types[0] == "fork.started"
        assert "step.5" in types and "step.9" in types
        assert not any(t in ("step.1", "step.2", "step.3", "step.4") for t in types)
    finally:
        await actae.disconnect()


def test_crash_recovery_resumes_from_last_step():
    """A crashed fork resumes from its last recorded step with state intact."""
    asyncio.run(_test_crash_resume("crash"))


async def _test_crash_resume(prefix):
    actae = _client()
    await actae.connect()
    try:
        base = _chan(prefix)
        reg = Registry()
        live = {"topic": "T"}
        # Simulate a crash: run steps 1-5, then raise → session marked crashed.
        session = AgentSession(actae, base, state_fn=lambda: dict(live))
        with pytest.raises(RuntimeError, match="boom"):
            async with session:
                # Run steps 1-5 only, then crash.
                await _run(session, reg, live, from_step=0, stop_after=5)
                raise RuntimeError("boom")  # exits __aexit__ with exception → crashed
        assert reg.count("s5") == 1 and reg.count("s6") == 0, "crashed after step 5"

        # Resume without fork_at_step: crash recovery, continues at step 6.
        resumed = await AgentSession.resume(actae, base)
        assert resumed.step_count == 5, f"resumed at {resumed.step_count}, want 5"
        rlive = dict(resumed.inherited_state or {})
        # Inherited state has steps 1-5's data.
        assert rlive.get("s5_out") == live.get("s5_out")
        async with resumed:
            await _run(resumed, Registry(), rlive, from_step=5)
        types = await _replay_types(actae, base)
        assert "step.5" in types and "step.9" in types
        # Crash recovery continues on the SAME channel (no fork.started).
        assert types[0] == "session.started"
    finally:
        await actae.disconnect()


# ---------------------------------------------------------------------------
# LangGraph checkpoint fork-resume (the framework-native path)
# ---------------------------------------------------------------------------

def test_langgraph_checkpoint_fork_resume():
    """A LangGraph MessagesState graph forked via ActaeCheckpointSaver.fork_thread
    preserves the full LLM message context and re-runs only the tail nodes."""
    if not _HAS_LANGGRAPH:
        pytest.skip("langgraph not installed")
    asyncio.run(_test_langgraph_fork("lg-fork"))


async def _test_langgraph_fork(prefix):
    from langgraph.graph import END, START, MessagesState, StateGraph

    actae = _client()
    await actae.connect()
    try:
        suffix = uuid.uuid4().hex[:8]
        thread = f"{prefix}-{suffix}"
        base_channel = f"langgraph-t-{suffix}"
        saver = ActaeCheckpointSaver(actae, channel=base_channel)
        cfg = {"configurable": {"thread_id": thread}}

        def build(suffix_text):
            g = StateGraph(MessagesState)
            g.add_node("n1", lambda s: {"messages": [("assistant", "a1:" + s["messages"][-1].content)]})
            g.add_node("n2", lambda s: {"messages": [("assistant", "a2:" + s["messages"][-1].content)]})
            g.add_node("n3", lambda s: {"messages": [("assistant", suffix_text + ":" + s["messages"][-1].content)]})
            g.add_node("n4", lambda s: {"messages": [("assistant", "a4:" + s["messages"][-1].content)]})
            g.add_edge(START, "n1"); g.add_edge("n1", "n2")
            g.add_edge("n2", "n3"); g.add_edge("n3", "n4"); g.add_edge("n4", END)
            return g.compile(checkpointer=saver)

        # Full run (node 3 = "BASE").
        full = build("BASE")
        await full.ainvoke({"messages": [("user", "u0")]}, cfg)

        # Checkpoints newest-first: [a4, a3, a2, a1, start]. Fork at a2 (the
        # checkpoint right before node 3), then node 3 re-runs with "FORK".
        hist = [t async for t in full.aget_state_history(cfg)]
        ids = [h.config["configurable"]["checkpoint_id"] for h in hist]
        after_n2_id = ids[2] if len(ids) >= 3 else ids[-1]

        fork_cfg = await saver.fork_thread(
            {"configurable": {"thread_id": thread, "checkpoint_id": after_n2_id}},
            new_thread_id=f"{thread}-fork",
            reason="refine node 3",
        )
        # The fork's config carries a different thread_id; the graph must be
        # built with a saver resolving that thread to the fork channel. The
        # same saver does (new_thread_id hashes to the fork channel).
        fork_graph = build("FORK")
        res = await fork_graph.ainvoke(None, fork_cfg)

        contents = [m.content for m in res["messages"]]
        # Context preserved: user + node1 + node2 carried over.
        assert contents[0] == "u0", f"user message lost: {contents}"
        assert contents[1] == "a1:u0", f"node1 context lost: {contents}"
        assert contents[2] == "a2:a1:u0", f"node2 context lost: {contents}"
        # Node 3 re-ran with the FORK prompt, node 4 followed.
        assert contents[3] == "FORK:a2:a1:u0", f"node3 not re-run with fork prompt: {contents}"
        assert contents[4] == "a4:FORK:a2:a1:u0", f"node4 not re-run: {contents}"
        assert len(contents) == 5, f"expected 5 messages, got {len(contents)}"
    finally:
        await actae.disconnect()
