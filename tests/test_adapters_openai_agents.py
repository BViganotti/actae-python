import asyncio
import copy
import subprocess
import sys
import textwrap
import threading
from types import SimpleNamespace
from typing import Any, Dict, List, Optional
from unittest.mock import patch

import pytest

pytest.importorskip("agents")

from actae_client.adapters.openai_agents import (
    ActaeTracingProcessor,
    _span_summary,
    install_actae_tracing,
    uninstall_actae_tracing,
)


class FakeActae:
    """In-memory Actae client implementing the primitives the adapters use."""

    def __init__(self):
        self.states: Dict[str, List[Dict[str, Any]]] = {}
        self.events: List[Dict[str, Any]] = []

    async def latest_cursor(self, channel_id: str) -> Optional[int]:
        return sum(1 for e in self.events if e["channel_id"] == channel_id) or None

    async def transition(self, channel_id, event_type, payload, state, *, actor="system", **kw):
        cursor = (await self.latest_cursor(channel_id) or 0) + 1
        self.events.append({"channel_id": channel_id, "type": event_type,
                            "payload": payload, "actor": actor, "cursor": cursor})
        self.states.setdefault(channel_id, []).append(
            {"version": len(self.states.get(channel_id, [])) + 1,
             "cursor": cursor, "state": state})
        return cursor

    async def save_state(self, channel_id, cursor, state) -> int:
        self.states.setdefault(channel_id, []).append(
            {"version": len(self.states.get(channel_id, [])) + 1,
             "cursor": cursor, "state": state})
        return len(self.states[channel_id])

    async def latest_state(self, channel_id):
        versions = self.states.get(channel_id)
        if not versions:
            return None
        latest = versions[-1]
        return {"cursor": latest["cursor"], "state": copy.deepcopy(latest["state"])}

    async def record(self, channel_id, event_type, payload, **kw):
        cursor = (await self.latest_cursor(channel_id) or 0) + 1
        self.events.append({"channel_id": channel_id, "type": event_type,
                            "payload": payload, "actor": kw.get("actor"), "cursor": cursor})

    async def list_states(self, channel_id, limit=100, offset=0):
        return [
            {"version": i + 1, "cursor": s["cursor"]}
            for i, s in enumerate(self.states.get(channel_id, []))
        ][offset:offset + limit]

    async def get_state(self, channel_id, version):
        versions = self.states.get(channel_id, [])
        if not versions:
            return None
        return versions[version - 1]

    async def fork(self, source_channel_id, new_channel_id, at_cursor, *,
                   display_name=None, reason=None, **kw):
        src = self.states.get(source_channel_id)
        if not src:
            raise ValueError("no state")
        for s in src:
            if s["cursor"] == at_cursor:
                self.states.setdefault(new_channel_id, []).append(
                    {"version": 1, "cursor": at_cursor, "state": copy.deepcopy(s["state"])})
                return {"channel_id": new_channel_id}
        raise ValueError("cursor not found")

    def state_versions(self, channel_id):
        return list(self.states.get(channel_id, []))


def _data(name="agent", **fields):
    return SimpleNamespace(name=name, **fields)


def _span(data=None, trace_id="t1", span_id="s1", parent_id=None,
          started_at="2026-01-01T00:00:00Z", ended_at="2026-01-01T00:00:02Z",
          error=None):
    return SimpleNamespace(
        trace_id=trace_id, span_id=span_id, parent_id=parent_id,
        started_at=started_at, ended_at=ended_at, error=error,
        span_data=data,
    )


def _trace(trace_id="t1", name="Agent workflow", group_id=None, metadata=None):
    return SimpleNamespace(trace_id=trace_id, name=name, group_id=group_id, metadata=metadata)


def _agent_span(**kw):
    return _span(_data("research", handoffs=["writer"], tools=["web_search"]), **kw)


class TestSpanSummary:
    def test_agent_span(self):
        span = _agent_span()
        out = _span_summary(span, include_inputs_outputs=True)
        assert out["type"] == "SimpleNamespace"
        assert out["name"] == "research"
        assert out["handoffs"] == ["writer"]
        assert out["tools"] == ["web_search"]

    def test_generation_span(self):
        data = SimpleNamespace(model="gpt-4o", input=[{"role": "user", "content": "hi"}],
                               output=[{"role": "assistant", "content": "yo"}],
                               usage={"total_tokens": 10})
        span = _span(data)
        out = _span_summary(span, include_inputs_outputs=True)
        assert out["model"] == "gpt-4o"
        assert out["usage"] == {"total_tokens": 10}
        assert out["input"] == [{"role": "user", "content": "hi"}]

    def test_function_and_guardrail_spans(self):
        fn = _span(SimpleNamespace(name="lookup", input='{"q": 1}', output="result"))
        guard = _span(SimpleNamespace(name="pii", triggered=True))
        out_fn = _span_summary(fn, include_inputs_outputs=True)
        out_guard = _span_summary(guard, include_inputs_outputs=True)
        assert out_fn["input"] == '{"q": 1}'
        assert out_fn["output"] == "result"
        assert out_guard["triggered"] is True

    def test_handoff_span(self):
        data = SimpleNamespace(from_agent="research", to_agent="writer")
        out = _span_summary(_span(data), include_inputs_outputs=True)
        assert out["from_agent"] == "research"
        assert out["to_agent"] == "writer"

    def test_inputs_outputs_can_be_stripped(self):
        data = SimpleNamespace(name="lookup", input="secret-input", output="secret-output")
        out = _span_summary(_span(data), include_inputs_outputs=False)
        assert "input" not in out
        assert "output" not in out
        assert out["name"] == "lookup"

    def test_error_and_duration(self):
        span = _span(_data("agent"), error={"message": "boom", "data": {"k": 1}})
        out = _span_summary(span, include_inputs_outputs=True)
        assert out["error"]["message"] == "boom"
        assert out["error"]["data"] == {"k": 1}
        assert out["duration_ms"] == 2000.0

    def test_no_duration_when_unstarted(self):
        span = _span(_data("agent"), started_at=None, ended_at=None)
        out = _span_summary(span, include_inputs_outputs=True)
        assert "duration_ms" not in out


class TestTracingProcessor:
    def test_protocol_methods_complete(self):
        proc = ActaeTracingProcessor(FakeActae())
        try:
            for method in ("on_trace_start", "on_trace_end",
                           "on_span_start", "on_span_end",
                           "force_flush", "shutdown"):
                assert callable(getattr(proc, method)), method
        finally:
            proc.shutdown()

    def _run(self, actae, processor, *steps):
        for step in steps:
            step()
        try:
            processor.force_flush()
        finally:
            processor.shutdown()

    def test_full_lifecycle_records_events_and_snapshot(self):
        actae = FakeActae()
        proc = ActaeTracingProcessor(actae)
        self._run(
            actae, proc,
            lambda: proc.on_trace_start(_trace()),
            lambda: proc.on_span_end(_agent_span(span_id="s1")),
            lambda: proc.on_span_end(_span(SimpleNamespace(name="lookup"), span_id="s2")),
            lambda: proc.on_trace_end(_trace()),
        )
        types = [e["type"] for e in actae.events]
        assert types == ["openai.trace.start", "openai.span.end", "openai.span.end", "openai.trace.end"]
        assert all(e["actor"] == "openai-agents" for e in actae.events)
        versions = actae.state_versions("openai_agents")
        assert len(versions) == 1
        state = versions[0]["state"]
        assert state["trace_id"] == "t1"
        assert state["workflow_name"] == "Agent workflow"
        assert len(state["spans"]) == 2
        assert state["spans"][0]["span_id"] == "s1"
        assert state["spans"][1]["name"] == "lookup"

    def test_trace_metadata_and_group_id_in_snapshot(self):
        actae = FakeActae()
        proc = ActaeTracingProcessor(actae)
        self._run(
            actae, proc,
            lambda: proc.on_trace_start(_trace(group_id="g-1", metadata={"env": "prod"})),
            lambda: proc.on_trace_end(_trace(group_id="g-1", metadata={"env": "prod"})),
        )
        state = actae.state_versions("openai_agents")[0]["state"]
        assert state["group_id"] == "g-1"
        assert state["metadata"] == {"env": "prod"}
        start_ev = actae.events[0]
        assert start_ev["payload"]["group_id"] == "g-1"

    def test_trace_end_without_start_still_works(self):
        actae = FakeActae()
        proc = ActaeTracingProcessor(actae)
        self._run(actae, proc, lambda: proc.on_trace_end(_trace()))
        assert len(actae.state_versions("openai_agents")) == 0
        assert [e["type"] for e in actae.events] == []

    def test_multiple_runs_version_snapshots(self):
        actae = FakeActae()
        proc = ActaeTracingProcessor(actae)
        self._run(
            actae, proc,
            lambda: proc.on_trace_start(_trace(trace_id="t1")),
            lambda: proc.on_span_end(_agent_span(trace_id="t1")),
            lambda: proc.on_trace_end(_trace(trace_id="t1")),
            lambda: proc.on_trace_start(_trace(trace_id="t2")),
            lambda: proc.on_trace_end(_trace(trace_id="t2")),
        )
        versions = actae.state_versions("openai_agents")
        assert len(versions) == 2
        assert [v["state"]["trace_id"] for v in versions] == ["t1", "t2"]

    def test_fork_trace_copies_run_snapshot_to_new_channel(self):
        actae = FakeActae()
        proc = ActaeTracingProcessor(actae)
        self._run(
            actae, proc,
            lambda: proc.on_trace_start(_trace(trace_id="t1")),
            lambda: proc.on_span_end(_agent_span(trace_id="t1")),
            lambda: proc.on_trace_end(_trace(trace_id="t1")),
        )
        asyncio.run(proc.fork_trace("t1", "run-1-fork", reason="compare"))
        # The fork channel inherited the t1 snapshot.
        fork_versions = actae.state_versions("run-1-fork")
        assert len(fork_versions) == 1
        assert fork_versions[0]["state"]["trace_id"] == "t1"
        assert fork_versions[0]["state"]["spans"][0]["name"] == "research"

    def test_fork_trace_missing_run_raises(self):
        actae = FakeActae()
        proc = ActaeTracingProcessor(actae)
        self._run(actae, proc)
        with pytest.raises(ValueError, match="no Actae snapshot"):
            asyncio.run(proc.fork_trace("nope", "run-fork"))

    def test_include_inputs_outputs_false(self):
        actae = FakeActae()
        proc = ActaeTracingProcessor(actae, include_inputs_outputs=False)
        self._run(
            actae, proc,
            lambda: proc.on_trace_start(_trace()),
            lambda: proc.on_span_end(_span(SimpleNamespace(name="lookup", input="i", output="o"))),
            lambda: proc.on_trace_end(_trace()),
        )
        span = actae.state_versions("openai_agents")[0]["state"]["spans"][0]
        assert "input" not in span
        assert "output" not in span

    def test_works_from_sync_context_without_running_loop(self):
        actae = FakeActae()
        proc = ActaeTracingProcessor(actae)
        try:
            try:
                asyncio.get_running_loop()
                has_loop = True
            except RuntimeError:
                has_loop = False
            assert has_loop is False
            proc.on_trace_start(_trace())
            proc.on_span_end(_agent_span())
            proc.on_trace_end(_trace())
            proc.force_flush()
            assert len(actae.state_versions("openai_agents")) == 1
            assert len(actae.events) == 3
        finally:
            proc.shutdown()

    def test_callbacks_never_raise_on_actae_failure(self):
        class BoomActae(FakeActae):
            async def record(self, *args, **kwargs):
                raise RuntimeError("server down")

            async def transition(self, *args, **kwargs):
                raise RuntimeError("server down")

        proc = ActaeTracingProcessor(BoomActae())
        try:
            proc.on_trace_start(_trace())
            proc.on_span_end(_agent_span())
            proc.on_trace_end(_trace())
            proc.force_flush()
        finally:
            proc.shutdown()

    def test_flush_async(self):
        actae = FakeActae()
        proc = ActaeTracingProcessor(actae)
        try:
            proc.on_trace_start(_trace())
            proc.on_trace_end(_trace())
            asyncio.run(proc.flush())
            assert len(actae.state_versions("openai_agents")) == 1
        finally:
            proc.shutdown()

    def test_requires_openai_agents(self):
        code = textwrap.dedent(
            """
            import sys

            class _Blocker:
                def find_spec(self, fullname, path=None, target=None):
                    if fullname == "agents" or fullname.startswith("agents."):
                        raise ImportError(f"blocked: {fullname}")
                    return None

            sys.meta_path.insert(0, _Blocker())
            import actae_client.adapters.openai_agents as m
            print("has_sdk:", m._HAS_OPENAI_AGENTS)
            try:
                m.ActaeTracingProcessor(None)
                print("UNEXPECTED")
            except ImportError as e:
                print("ok:", str(e)[:50])
            try:
                m.install_actae_tracing(None)
                print("UNEXPECTED2")
            except ImportError as e:
                print("ok2:", str(e)[:50])
            """
        )
        result = subprocess.run(
            [sys.executable, "-c", code], capture_output=True, text=True
        )
        assert result.returncode == 0, result.stderr
        assert "has_sdk: False" in result.stdout
        assert "ok: ActaeTracingProcessor requires openai-agents" in result.stdout
        assert "ok2: install_actae_tracing requires openai-agents" in result.stdout


class TestInstallUninstall:
    def test_install_registers_processor(self):
        with patch("actae_client.adapters.openai_agents.add_trace_processor") as add:
            proc = install_actae_tracing(FakeActae(), channel="my-ch")
        add.assert_called_once()
        assert isinstance(proc, ActaeTracingProcessor)
        assert proc.channel == "my-ch"
        proc.shutdown()

    def test_uninstall_reaches_into_provider(self):
        class FakeMulti:
            def __init__(self):
                self._lock = threading.Lock()
                self._processors = ("a", "b")

        class FakeProvider:
            _multi_processor = FakeMulti()

        proc = ActaeTracingProcessor(FakeActae())
        try:
            with patch("agents.tracing.get_trace_provider",
                       return_value=FakeProvider()):
                assert uninstall_actae_tracing(proc) is True
        finally:
            proc.shutdown()

    def test_uninstall_best_effort_when_internals_missing(self):
        class BareProvider:
            pass

        proc = ActaeTracingProcessor(FakeActae())
        try:
            with patch("agents.tracing.get_trace_provider",
                       return_value=BareProvider()):
                assert uninstall_actae_tracing(proc) is False
        finally:
            proc.shutdown()
