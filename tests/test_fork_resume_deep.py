"""Deep fork-resume comparison & edge-case suite (live).

The core question this answers:

  - A FULL run (steps 1-9, fresh state) vs a FORK run (fork at step 5, run
    steps 6-9): are the results identical for the shared steps?
  - A FULL run vs a FORK with a MODIFIED prompt at step 6: does the result
    differ ONLY in the refined step, while steps 1-5 stay byte-identical?
  - Every edge case: fork at step 1 / last / beyond range, fork-of-fork,
    fork with no saved state, concurrent forks, idempotent forks,
    non-monotonic-cursor detection, crash recovery.

Uses a deterministic pipeline (no LLM) so assertions are exact.

    ACTAE_URL=http://localhost:8002 ACTAE_API_KEY=sk-dev-0000000000000000000000 \
        python3 -m pytest sdks/python/tests/test_fork_resume_deep.py -v
"""

import asyncio
import os
import uuid

import pytest

from actae_client import ActaeClient
from actae_client.errors import NoRestorableCheckpointError
from actae_client.session import AgentSession, SessionError, SessionCompletedError

_ENDPOINT = os.environ.get("ACTAE_URL") or os.environ.get("ACTAE_ENDPOINT")
_API_KEY = os.environ.get("ACTAE_API_KEY")

pytestmark = pytest.mark.skipif(
    not (_ENDPOINT and _API_KEY), reason="ACTAE_URL/ACTAE_API_KEY not set"
)


def _client():
    return ActaeClient(api_key=_API_KEY, endpoint=_ENDPOINT)


def _chan(prefix):
    return f"fork-deep-{prefix}-{uuid.uuid4().hex[:8]}"


class Registry:
    def __init__(self):
        self.calls = {}

    def mark(self, step):
        self.calls[step] = self.calls.get(step, 0) + 1

    def count(self, step):
        return self.calls.get(step, 0)


# Deterministic 9-step pipeline. Each step produces a value that depends on
# all previous steps' values (so any deviation is caught). Step 6 is the
# "refined" step — its output depends on a `style` parameter.
def _prev(state, n):
    return "|".join(state[f"s{i}_out"] for i in range(1, n))


def step1(reg, state):
    reg.mark("s1"); return {"s1_out": "r:" + state["topic"]}

def step2(reg, state):
    reg.mark("s2"); return {"s2_out": "i(" + _prev(state, 2) + ")"}

def step3(reg, state):
    reg.mark("s3"); return {"s3_out": "sh(" + _prev(state, 3) + ")"}

def step4(reg, state):
    reg.mark("s4"); return {"s4_out": "rk(" + _prev(state, 4) + ")"}

def step5(reg, state):
    reg.mark("s5"); return {"s5_out": "m(" + _prev(state, 5) + ")"}

def step6(reg, state, style="neutral"):
    reg.mark("s6")
    return {"s6_out": f"[{style}] rec(" + _prev(state, 6) + ")"}

def step7(reg, state):
    reg.mark("s7"); return {"s7_out": "sum(" + _prev(state, 7) + ")"}

def step8(reg, state):
    reg.mark("s8"); return {"s8_out": "nxt(" + _prev(state, 8) + ")"}

def step9(reg, state):
    reg.mark("s9"); return {"s9_out": "fin(" + _prev(state, 9) + ")"}

PIPELINE = [
    (1, step1), (2, step2), (3, step3), (4, step4), (5, step5),
    (6, step6), (7, step7), (8, step8), (9, step9),
]
ALL_STATE_KEYS = [f"s{i}_out" for i in range(1, 10)]
FORK_STEP = 5


async def _run(session, registry, live, *, style="neutral", from_step=1, stop_after=9):
    for step, fn in PIPELINE[from_step - 1:]:
        if step > stop_after:
            break
        if step == 6:
            delta = fn(registry, live, style=style)
        else:
            delta = fn(registry, live)
        live.update(delta)
        step_input = dict(delta)
        if step == 6:
            step_input = {k: live[k] for k in ALL_STATE_KEYS + ["s6_out"] if k in live}
        await session.step(f"step.{step}", input=step_input, output=delta, context=delta)


async def _state_of(actae, channel):
    snap = await actae.latest_state(channel)
    return (snap or {}).get("state", {})


async def _types(actae, channel):
    return [e.event_type for e in await actae.replay(channel, cursor=0, limit=500)]


async def _main(client):
    await client.connect()
    try:
        await _full_vs_fork(client, "cmp")
        await _full_vs_fork_modified(client, "mod")
        await _fork_edge_cases(client, "edge")
    finally:
        await client.disconnect()


# ---------------------------------------------------------------------------
# 1. FULL run vs FORK run (same prompt): identical results for shared steps.
# ---------------------------------------------------------------------------
async def _full_vs_fork(actae, prefix):
    base = _chan(prefix)
    fork = f"{base}-fork"

    # FULL: run all 9 steps from scratch.
    full_reg = Registry()
    full_live = {"topic": "T"}
    async with AgentSession(actae, base, state_fn=lambda: dict(full_live)) as s:
        await _run(s, full_reg, full_live)
    full_state = await _state_of(actae, base)

    # FORK at step 5, run steps 6-9 with the SAME step 6 prompt.
    fork_reg = Registry()
    fork_live = {"topic": "T"}
    fork_sess = await AgentSession.resume(
        actae, base, fork_at_step=FORK_STEP, name=fork, state_fn=lambda: dict(fork_live)
    )
    inherited = fork_sess.inherited_state or {}
    fork_live.update(inherited)
    async with fork_sess:
        await _run(fork_sess, fork_reg, fork_live, from_step=FORK_STEP + 1)
    fork_state = await _state_of(actae, fork)

    # (a) Steps 1-5 identical (inherited, not regenerated).
    for k in [f"s{i}_out" for i in range(1, 6)]:
        assert fork_state.get(k) == full_state.get(k), f"{k}: fork {fork_state.get(k)!r} != full {full_state.get(k)!r}"
    # (b) Steps 6-9 identical too (same prompt, same inputs → same outputs).
    for k in [f"s{i}_out" for i in range(6, 10)]:
        assert fork_state.get(k) == full_state.get(k), f"{k}: fork {fork_state.get(k)!r} != full {full_state.get(k)!r}"
    # (c) Full determinism: the whole state matches.
    assert fork_state == full_state, "fork state != full state (same prompt)"
    # (d) Event logs: fork has fork.started + session.started + steps 6-9.
    fork_types = await _types(actae, fork)
    assert fork_types[0] == "fork.started"
    assert "step.6" in fork_types and "step.9" in fork_types
    assert not any(t in ("step.1", "step.2", "step.3", "step.4", "step.5") for t in fork_types)
    # (e) Execution counters: steps 1-5 not invoked on fork.
    for i in range(1, 6):
        assert fork_reg.count(f"s{i}") == 0, f"s{i} re-invoked on fork"


# ---------------------------------------------------------------------------
# 2. FULL vs FORK with MODIFIED prompt at step 6: differs ONLY in step 6.
# ---------------------------------------------------------------------------
async def _full_vs_fork_modified(actae, prefix):
    base = _chan(prefix)
    fork = f"{base}-forkmod"

    full_reg = Registry()
    full_live = {"topic": "T"}
    async with AgentSession(actae, base, state_fn=lambda: dict(full_live)) as s:
        await _run(s, full_reg, full_live)
    full_state = await _state_of(actae, base)

    fork_reg = Registry()
    fork_live = {"topic": "T"}
    fork_sess = await AgentSession.resume(
        actae, base, fork_at_step=FORK_STEP, name=fork, state_fn=lambda: dict(fork_live)
    )
    fork_live.update(fork_sess.inherited_state or {})
    async with fork_sess:
        await _run(fork_sess, fork_reg, fork_live, from_step=FORK_STEP + 1, style="aggressive")
    fork_state = await _state_of(actae, fork)

    # Steps 1-5 identical (inherited).
    for k in [f"s{i}_out" for i in range(1, 6)]:
        assert fork_state.get(k) == full_state.get(k), f"{k} should be inherited"
    # Step 6 DIFFERS (the modified prompt).
    assert fork_state.get("s6_out") != full_state.get("s6_out"), "step 6 must differ"
    # Steps 7-9 differ transitively (they depend on s6).
    assert fork_state.get("s7_out") != full_state.get("s7_out")
    assert fork_state.get("s9_out") != full_state.get("s9_out")
    # But step 6's INPUT (the inherited context) is identical to the full run.
    base_events = await actae.replay(base, cursor=0, limit=500)
    fork_events = await actae.replay(fork, cursor=0, limit=500)
    full_s6 = next(e for e in base_events if e.event_type == "step.6")
    fork_s6 = next(e for e in fork_events if e.event_type == "step.6")
    full_inp = full_s6.payload.get("input", {})
    fork_inp = fork_s6.payload.get("input", {})
    for k in [f"s{i}_out" for i in range(1, 6)]:
        assert fork_inp.get(k) == full_inp.get(k), f"step6 input {k} differs"


# ---------------------------------------------------------------------------
# 3. Edge cases.
# ---------------------------------------------------------------------------
async def _fork_edge_cases(actae, prefix):
    await _fork_at_every_step(actae, prefix + "-a")
    await _fork_of_fork(actae, prefix + "-b")
    await _fork_without_state_snapshot(actae, prefix + "-c")
    await _concurrent_forks(actae, prefix + "-d")
    await _idempotent_fork(actae, prefix + "-e")
    await _resume_completed_raises(actae, prefix + "-f")
    await _resume_unknown_channel_raises(actae, prefix + "-g")
    await _nested_state_consistency(actae, prefix + "-h")
    await _fork_raw_channel_outside_session(actae, prefix + "-i")
    await _fork_step_beyond_pipeline_raises(actae, prefix + "-j")
    await _snapshot_interval_gt_one_fork_falls_back(actae, prefix + "-k")
    await _fork_lineage_only(actae, prefix + "-m")
    await _fork_of_fork_at_inherited_step(actae, prefix + "-l")


async def _fork_at_every_step(actae, prefix):
    """Fork at step 1..9; each fork must inherit the right prefix of state."""
    base = _chan(prefix)
    reg = Registry()
    live = {"topic": "T"}
    async with AgentSession(actae, base, state_fn=lambda: dict(live)) as s:
        await _run(s, reg, live)
    full = await _state_of(actae, base)

    for at in range(1, 10):
        fname = f"{base}-f{at}"
        fs = await AgentSession.resume(actae, base, fork_at_step=at, name=fname, state_fn=lambda: {})
        fstate = fs.inherited_state or {}
        # Fork at step N inherits steps 1..N.
        for i in range(1, at + 1):
            assert fstate.get(f"s{i}_out") == full.get(f"s{i}_out"), \
                f"fork@{at} missing inherited s{i}"
        # It does NOT inherit steps after N.
        for i in range(at + 1, 10):
            assert fstate.get(f"s{i}_out") is None, f"fork@{at} should not have s{i}"
        assert fs.step_count == at, f"fork@{at} step_count {fs.step_count}"


async def _fork_of_fork(actae, prefix):
    """Fork of a fork: the second fork inherits the deepest state."""
    base = _chan(prefix)
    f1 = f"{base}-f1"
    f2 = f"{base}-f2"
    reg = Registry()
    live = {"topic": "T"}
    async with AgentSession(actae, base, state_fn=lambda: dict(live)) as s:
        await _run(s, reg, live)

    # f1: fork at 3, run steps 4-9.
    s1 = await AgentSession.resume(actae, base, fork_at_step=3, name=f1, state_fn=lambda: dict(l1))
    l1 = dict(s1.inherited_state or {})
    async with s1:
        await _run(s1, Registry(), l1, from_step=4)
    # f2: fork f1 at 6, run steps 7-9.
    s2 = await AgentSession.resume(actae, f1, fork_at_step=6, name=f2, state_fn=lambda: dict(l2))
    l2 = dict(s2.inherited_state or {})
    assert l2.get("s1_out") == live.get("s1_out")
    assert l2.get("s6_out") == l1.get("s6_out"), f"s6 not inherited: {l2.get('s6_out')!r}"
    assert s2.step_count == 6
    async with s2:
        await _run(s2, Registry(), l2, from_step=7)
    types = await _types(actae, f2)
    assert types[0] == "fork.started"
    assert not any(t in ("step.1", "step.2", "step.3", "step.4", "step.5", "step.6") for t in types)


async def _fork_without_state_snapshot(actae, prefix):
    """Forking an event-only channel (no state_fn) is strict by default: it
    raises NoRestorableCheckpointError; with boundary_mode='approximate' it
    falls back to the latest state."""
    base = _chan(prefix)
    reg = Registry()
    live = {"topic": "T"}
    # No state_fn → no snapshots saved.
    async with AgentSession(actae, base) as s:
        await _run(s, reg, live)

    # Strict default: no snapshot at the boundary → clear error.
    with pytest.raises(NoRestorableCheckpointError):
        await AgentSession.resume(actae, base, fork_at_step=3, name=f"{base}-f", state_fn=lambda: {})

    # Approximate mode: falls back to the latest state instead of erroring.
    fs = await AgentSession.resume(
        actae, base, fork_at_step=3, name=f"{base}-f-approx",
        state_fn=lambda: {}, boundary_mode="approximate",
    )
    assert fs.step_count == 3


async def _concurrent_forks(actae, prefix):
    """Two forks created concurrently from the same base are independent."""
    base = _chan(prefix)
    reg = Registry()
    live = {"topic": "T"}
    async with AgentSession(actae, base, state_fn=lambda: dict(live)) as s:
        await _run(s, reg, live)

    f1 = await AgentSession.resume(actae, base, fork_at_step=5, name=f"{base}-fa", state_fn=lambda: dict(a))
    a = dict(f1.inherited_state or {})
    f2 = await AgentSession.resume(actae, base, fork_at_step=5, name=f"{base}-fb", state_fn=lambda: dict(b))
    b = dict(f2.inherited_state or {})

    # Both inherited the same step-5 state independently.
    for k in [f"s{i}_out" for i in range(1, 6)]:
        assert a.get(k) == b.get(k), f"{k}: {a.get(k)!r} != {b.get(k)!r}"

    # Different step-6 styles → independent evolution.
    async with f1:
        await _run(f1, Registry(), a, from_step=6, style="aggressive")
    async with f2:
        await _run(f2, Registry(), b, from_step=6, style="neutral")
    fa = await _state_of(actae, f"{base}-fa")
    fb = await _state_of(actae, f"{base}-fb")
    assert fa["s6_out"] != fb["s6_out"], "concurrent forks must evolve independently"
    assert fa["s1_out"] == fb["s1_out"], "inherited prefix must match"


async def _idempotent_fork(actae, prefix):
    """Re-forking the same name is a no-op (returns the original fork)."""
    base = _chan(prefix)
    reg = Registry()
    live = {"topic": "T"}
    async with AgentSession(actae, base, state_fn=lambda: dict(live)) as s:
        await _run(s, reg, live)
    name = f"{base}-idem"
    f1 = await AgentSession.resume(actae, base, fork_at_step=5, name=name, state_fn=lambda: {})
    f2 = await AgentSession.resume(actae, base, fork_at_step=5, name=name, state_fn=lambda: {})
    # Both resolve to the same channel; the server is idempotent.
    assert f1.name == f2.name == name


async def _resume_completed_raises(actae, prefix):
    base = _chan(prefix)
    reg = Registry()
    live = {"topic": "T"}
    async with AgentSession(actae, base, state_fn=lambda: dict(live)) as s:
        await _run(s, reg, live)
    with pytest.raises(SessionCompletedError):
        await AgentSession.resume(actae, base)


async def _resume_unknown_channel_raises(actae, prefix):
    with pytest.raises(SessionError, match="not found"):
        await AgentSession.resume(actae, f"{prefix}-does-not-exist")


async def _nested_state_consistency(actae, prefix):
    """Deep state comparison: fork's inherited state == parent's snapshot at
    the fork cursor (byte-for-byte)."""
    base = _chan(prefix)
    reg = Registry()
    live = {"topic": "T"}
    async with AgentSession(actae, base, state_fn=lambda: dict(live)) as s:
        await _run(s, reg, live)
    full = await _state_of(actae, base)

    fs = await AgentSession.resume(actae, base, fork_at_step=7, name=f"{base}-deep", state_fn=lambda: {})
    inherited = fs.inherited_state or {}
    # Fork at step 7 inherits steps 1..7 exactly; steps 8-9 do not exist yet.
    for i in range(1, 8):
        k = f"s{i}_out"
        assert inherited.get(k) == full.get(k), f"{k}: {inherited.get(k)!r} != {full.get(k)!r}"
    for i in range(8, 10):
        k = f"s{i}_out"
        assert inherited.get(k) is None, f"{k} should not exist at step-7 fork"
    assert inherited.get("s7_out") == full.get("s7_out")


async def _fork_raw_channel_outside_session(actae, prefix):
    """Forking a channel created outside AgentSession (raw record calls, no
    metadata, single state snapshot at a later cursor) — exercises the
    replay-fallback path and the strict/approximate boundary policy."""
    base = _chan(prefix)
    live = {"topic": "T"}
    for i in range(5):
        ev = await actae.record(base, f"ev.{i}", {"i": i}, actor="raw")
    await actae.save_state(base, ev.cursor, live)

    # The only snapshot is at cursor 5; forking at step 3 (cursor 3) has no
    # boundary snapshot → strict default raises.
    with pytest.raises(NoRestorableCheckpointError):
        await AgentSession.resume(actae, base, fork_at_step=3, name=f"{base}-fork", state_fn=lambda: {})

    # Approximate mode: falls back to the latest state (cursor 5's snapshot).
    # The fallback resolves BEYOND the requested boundary, so the SDK refuses
    # to prime with the contaminated state — inherited_state is None.
    fs = await AgentSession.resume(
        actae, base, fork_at_step=3, name=f"{base}-fork-approx",
        state_fn=lambda: {}, boundary_mode="approximate",
    )
    assert fs.step_count == 3, f"raw-channel fork step_count {fs.step_count}"
    assert fs.inherited_state is None, (
        "raw-channel approximate fork inherited contaminated state"
    )


async def _fork_step_beyond_pipeline_raises(actae, prefix):
    """Forking at a step beyond the recorded steps must raise a clear error."""
    base = _chan(prefix)
    reg = Registry()
    live = {"topic": "T"}
    async with AgentSession(actae, base, state_fn=lambda: dict(live)) as s:
        await _run(s, reg, live)  # 9 steps
    with pytest.raises(SessionError, match="exceeds|no recorded|steps"):
        await AgentSession.resume(actae, base, fork_at_step=10, name=f"{base}-f10", state_fn=lambda: {})


async def _snapshot_interval_gt_one_fork_falls_back(actae, prefix):
    """With snapshot_interval > 1, a fork at a step with no snapshot is strict
    by default (raises); boundary_mode='approximate' falls back to the latest
    state (drift surfaced, contaminated state refused)."""
    base = _chan(prefix)
    reg = Registry()
    live = {"topic": "T"}
    # Save snapshots every 3 steps: steps 3, 6, 9 have snapshots.
    async with AgentSession(actae, base, snapshot_interval=3, state_fn=lambda: dict(live)) as s:
        await _run(s, reg, live)
    # Fork at step 2 (no snapshot at/before cursor 2) → strict raises.
    with pytest.raises(NoRestorableCheckpointError):
        await AgentSession.resume(actae, base, fork_at_step=2, name=f"{base}-f2", state_fn=lambda: {})
    # Approximate mode: falls back to the latest state. The fallback resolves
    # BEYOND the requested boundary → the SDK refuses the contaminated state.
    fs = await AgentSession.resume(
        actae, base, fork_at_step=2, name=f"{base}-f2-approx",
        state_fn=lambda: {}, boundary_mode="approximate",
    )
    assert fs.step_count == 2
    assert fs.inherited_state is None, "interval>1 approximate fork inherited contaminated state"


async def _fork_lineage_only(actae, prefix):
    """boundary_mode='lineage_only' creates the fork with NO state copy —
    the escape hatch for event-only channels. Must succeed, report
    restorable=False, expose no inherited state, and record the parent
    lineage in the child's channel metadata."""
    base = _chan(prefix)
    reg = Registry()
    live = {"topic": "T"}
    # Event-only channel: no state_fn → zero snapshots saved.
    async with AgentSession(actae, base) as s:
        await _run(s, reg, live)

    fs = await AgentSession.resume(
        actae, base, fork_at_step=5, name=f"{base}-lo",
        state_fn=lambda: {}, boundary_mode="lineage_only",
    )
    assert fs.step_count == 5
    assert fs.boundary_restorable is False, "lineage_only fork must report restorable=False"
    assert fs.resolved_boundary_cursor == 0, "lineage_only fork must not copy state (cursor 0)"
    assert fs.inherited_state is None, "lineage_only fork must expose no inherited state"

    # The fork channel exists, is replayable, and its metadata records the
    # lineage to the parent.
    types = await _types(actae, fs.name)
    assert types, "lineage_only fork channel must exist and be replayable"
    meta = await actae.get_channel_metadata(fs.name)
    assert meta is not None, "lineage_only fork must have channel metadata"
    assert meta.parent_channel_id == base, (
        f"lineage_only fork parent = {meta.parent_channel_id!r}, want {base!r}"
    )
    # Stepping the fork works and continues at step 6 (no inherited prefix).
    async with fs:
        await fs.step(f"step.{6}", input={}, output={"s6_out": "refined"}, context={})
    post = await _types(actae, fs.name)
    assert "step.6" in post, "lineage_only fork must record steps after the boundary"


async def _fork_of_fork_at_inherited_step(actae, prefix):
    """Forking a fork at an INHERITED step must resolve against the ancestor
    that owns that step, not silently map to the fork's own first event."""
    base = _chan(prefix)
    f1 = f"{base}-f1"
    f2 = f"{base}-f2"
    reg = Registry()
    live = {"topic": "T"}
    async with AgentSession(actae, base, state_fn=lambda: dict(live)) as s:
        await _run(s, reg, live)
    full = await _state_of(actae, base)

    # f1: fork at step 3 (inherits steps 1-3), then run steps 4-9.
    s1 = await AgentSession.resume(actae, base, fork_at_step=3, name=f1, state_fn=lambda: dict(l1))
    l1 = dict(s1.inherited_state or {})
    async with s1:
        await _run(s1, Registry(), l1, from_step=4)

    # f2: fork f1 at step 2 — an INHERITED step. The correct state is on the
    # root (base), and the fork boundary must be the root's step-2 cursor.
    s2 = await AgentSession.resume(actae, f1, fork_at_step=2, name=f2, state_fn=lambda: dict(l2))
    l2 = dict(s2.inherited_state or {})
    # f2 must inherit steps 1-2 from the ROOT, not f1's own first events.
    assert l2.get("s1_out") == full.get("s1_out"), f"s1: {l2.get('s1_out')!r} != {full.get('s1_out')!r}"
    assert l2.get("s2_out") == full.get("s2_out"), f"s2: {l2.get('s2_out')!r} != {full.get('s2_out')!r}"
    # It must NOT accidentally carry f1's later steps (4+) or even step 3.
    assert l2.get("s3_out") is None, f"f2 should not inherit s3: {l2.get('s3_out')!r}"
    assert l2.get("s4_out") is None, f"f2 should not inherit s4: {l2.get('s4_out')!r}"
    assert s2.step_count == 2, f"f2 step_count {s2.step_count}"


def test_fork_deep_comparison():
    asyncio.run(_main(_client()))


# ---------------------------------------------------------------------------
# Framework-agnostic fork helpers (StateManager + Claude store) — live proof.
# ---------------------------------------------------------------------------

def test_state_manager_fork_is_framework_agnostic():
    """StateManager.fork copies the opaque state dict to a new channel —
    the same primitive works for any framework because the state is opaque."""
    asyncio.run(_test_state_manager_fork("sm-fork"))


async def _test_state_manager_fork(prefix):
    from actae_client.adapters.base import StateManager

    actae = _client()
    await actae.connect()
    try:
        base = _chan(prefix)
        fork_ch = f"{base}-fork"
        mgr = StateManager(actae, base)

        # A framework-agnostic "context": any dict (messages, memory, ...).
        await mgr.save({"messages": ["u1", "a1", "u2"], "step": 3})

        forked = await mgr.fork(fork_ch, reason="refine from here")
        inherited = await forked.load()
        assert inherited == {"messages": ["u1", "a1", "u2"], "step": 3}, \
            "fork must inherit the opaque state dict"
        # The fork evolves independently.
        await forked.save({"messages": ["u1", "a1", "u2", "a2"], "step": 4})
        assert (await mgr.load())["step"] == 3, "source unaffected by fork writes"
        assert (await forked.load())["step"] == 4
    finally:
        await actae.disconnect()


def test_claude_store_fork_session_live():
    """ActaeClaudeSessionStore.fork_session copies the full transcript into a
    new session — the Claude-SDK native fork path."""
    pytest.importorskip("claude_agent_sdk")
    asyncio.run(_test_claude_fork("claude-fork"))


async def _test_claude_fork(prefix):
    from actae_client.adapters.claude import ActaeClaudeSessionStore

    actae = _client()
    await actae.connect()
    try:
        proj = _chan(prefix)
        store = ActaeClaudeSessionStore(actae)
        sess = "conversation-1"
        key = {"project_key": proj, "session_id": sess}

        # Build a 3-turn transcript.
        for i, t in enumerate(["user_message", "assistant_message", "user_message"]):
            await store.append(key, [{
                "type": t, "uuid": f"msg-{i}",
                "content": [{"type": "text", "text": f"turn {i}"}],
            }])

        # Fork into a new session; the full transcript must carry over.
        new_key = await store.fork_session(proj, sess, "conversation-1-fix")
        forked = await store.load(new_key)
        assert forked is not None
        assert len(forked) == 3, f"fork must inherit all 3 turns, got {len(forked)}"
        assert [e["uuid"] for e in forked] == ["msg-0", "msg-1", "msg-2"]

        # The fork is discoverable as a session.
        sessions = await store.list_sessions(proj)
        assert "conversation-1-fix" in {s["session_id"] for s in sessions}
    finally:
        await actae.disconnect()


# ---------------------------------------------------------------------------
# Codex CLI observability mirror (OTLP receiver) — live proof.
# ---------------------------------------------------------------------------

def test_codex_otlp_receiver_mirrors_to_actae_live():
    """The Codex OTLP receiver mirrors codex.* events into Actae channels and
    persists token/tool snapshots so diff & trail work on Codex sessions."""
    asyncio.run(_test_codex_receiver("codex-live"))


async def _test_codex_receiver(prefix):
    from actae_client.adapters.codex import CodexOTLPReceiver, _channel_for_conversation
    import aiohttp

    actae = _client()
    await actae.connect()
    try:
        conversation = f"codex-conv-{uuid.uuid4().hex[:8]}"
        ch = _channel_for_conversation(conversation)

        # Start a receiver, send two codex.* events, stop.
        receiver = CodexOTLPReceiver(actae, host="127.0.0.1", port=0)
        await receiver.start()
        try:
            port = receiver._site._server.sockets[0].getsockname()[1]
            async with aiohttp.ClientSession() as sess:
                body = {
                    "resourceLogs": [{"scopeLogs": [{"logRecords": [
                        {"timeUnixNano": "1", "body": {"stringValue": "codex.conversation_starts"},
                         "attributes": [{"key": "conversation.id", "value": {"stringValue": conversation}}]},
                        {"timeUnixNano": "2", "body": {"stringValue": "codex.sse_event"},
                         "attributes": [
                             {"key": "conversation.id", "value": {"stringValue": conversation}},
                             {"key": "input_token_count", "value": {"intValue": 120}},
                             {"key": "output_token_count", "value": {"intValue": 60}},
                         ]},
                        {"timeUnixNano": "3", "body": {"stringValue": "codex.tool_result"},
                         "attributes": [
                             {"key": "conversation.id", "value": {"stringValue": conversation}},
                             {"key": "tool_name", "value": {"stringValue": "run_shell"}},
                             {"key": "success", "value": {"stringValue": "true"}},
                         ]},
                    ]}]}]}
                async with sess.post(f"http://127.0.0.1:{port}/v1/logs", json=body) as resp:
                    assert resp.status == 200
        finally:
            await receiver.stop()

        # The conversation channel has the 3 mirrored events + a snapshot.
        events = await actae.replay(ch, cursor=0, limit=100)
        types = [e.event_type for e in events]
        assert "codex.conversation_starts" in types
        assert "codex.sse_event" in types
        assert "codex.tool_result" in types

        snap = await actae.latest_state(ch)
        assert snap is not None
        state = snap["state"]
        assert state["tokens"]["input_token_count"] == 120
        assert state["tokens"]["output_token_count"] == 60
        assert state["tools"][0]["tool_name"] == "run_shell"

        # The channel is forkable / diffable like any other.
        fork_ch = f"{ch}-fork"
        await actae.fork(ch, fork_ch, snap["cursor"], display_name="codex-fork", reason="compare")
        inherited = await actae.latest_state(fork_ch)
        assert inherited["state"]["tokens"]["input_token_count"] == 120, \
            "fork must inherit the codex token snapshot"
    finally:
        await actae.disconnect()
