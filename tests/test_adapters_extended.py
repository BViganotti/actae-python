"""Extended adapter tests — LangGraph, CrewAI, LangChain.

LangGraph tests use an in-memory fake implementing the Actae state/event API
and real compiled LangGraph graphs. No server required.
"""

import asyncio
import subprocess
import sys
import textwrap
from unittest.mock import AsyncMock, MagicMock

import pytest

from actae_client.adapters import langgraph as _lg
from actae_client.adapters import langchain as _lc

_SKIP_LC = _lc._HAS_LANGCHAIN
_REQUIRES_LG = pytest.mark.skipif(
    not _lg._HAS_LANGGRAPH, reason="langgraph not installed"
)


def _mock_actae():
    actae = MagicMock()
    actae.latest_cursor = AsyncMock(return_value=42)
    actae.save_state = AsyncMock(return_value=1)
    actae.latest_state = AsyncMock(return_value={
        "cursor": 42,
        "state": {"key": "val"},
    })
    actae.fork = AsyncMock(return_value={"channel_id": "forked"})
    actae.list_states = AsyncMock(return_value=[])
    return actae


class FakeActae:
    """In-memory stand-in for the Actae state/event API the saver consumes."""

    def __init__(self):
        self.states = {}
        self.cursor = 0
        self.version = 0
        self.events = []

    async def record(self, channel_id, event_type, payload, *, actor, **kw):
        self.cursor += 1
        event = type("Event", (), {"id": f"ev-{self.cursor}", "cursor": self.cursor})()
        self.events.append((channel_id, event_type, event.cursor))
        return event

    async def save_state(self, channel_id, cursor, state):
        self.version += 1
        self.states.setdefault(channel_id, {})[self.version] = {
            "cursor": cursor,
            "state": state,
        }
        return self.version

    async def latest_state(self, channel_id):
        versions = self.states.get(channel_id, {})
        if not versions:
            return None
        latest = versions[max(versions)]
        return {"cursor": latest["cursor"], "state": latest["state"]}

    async def list_states(self, channel_id, *, limit=100, offset=0):
        versions = sorted(self.states.get(channel_id, {}).items(), reverse=True)
        return [
            {"version": key, "cursor": entry["cursor"]}
            for key, entry in versions[offset:offset + limit]
        ]

    async def get_state(self, channel_id, version):
        entry = self.states.get(channel_id, {}).get(version)
        if entry is None:
            return None
        return {"cursor": entry["cursor"], "state": entry["state"], "version": version}

    async def delete_state(self, channel_id, version):
        self.states.get(channel_id, {}).pop(version, None)


def _echo_graph(saver):
    from langgraph.graph import END, START, StateGraph, MessagesState

    graph = (
        StateGraph(MessagesState)
        .add_node("echo", lambda s: {"messages": s["messages"]})
        .add_edge(START, "echo")
        .add_edge("echo", END)
        .compile(checkpointer=saver)
    )
    return graph


@_REQUIRES_LG
class TestLangGraphCheckpointSaver:
    def test_multi_turn_sync_invoke(self):
        from actae_client.adapters.langgraph import ActaeCheckpointSaver

        actae = FakeActae()
        graph = _echo_graph(ActaeCheckpointSaver(actae))
        cfg = {"configurable": {"thread_id": "sync-1"}}

        r1 = graph.invoke({"messages": [("user", "first")]}, cfg)
        assert [m.content for m in r1["messages"]] == ["first"]

        r2 = graph.invoke({"messages": [("user", "second")]}, cfg)
        assert [m.content for m in r2["messages"]] == ["first", "second"]

        state = graph.get_state(cfg)
        assert [m.content for m in state.values["messages"]] == ["first", "second"]

    def test_multi_turn_async_invoke(self):
        from actae_client.adapters.langgraph import ActaeCheckpointSaver

        async def run():
            actae = FakeActae()
            graph = _echo_graph(ActaeCheckpointSaver(actae))
            cfg = {"configurable": {"thread_id": "async-1"}}
            await graph.ainvoke({"messages": [("user", "first")]}, cfg)
            r2 = await graph.ainvoke({"messages": [("user", "second")]}, cfg)
            return [m.content for m in r2["messages"]]

        assert asyncio.run(run()) == ["first", "second"]

    def test_thread_isolation(self):
        from actae_client.adapters.langgraph import ActaeCheckpointSaver

        async def run():
            actae = FakeActae()
            graph = _echo_graph(ActaeCheckpointSaver(actae))
            await graph.ainvoke({"messages": [("user", "a")]}, {"configurable": {"thread_id": "t1"}})
            await graph.ainvoke({"messages": [("user", "b")]}, {"configurable": {"thread_id": "t2"}})
            s1 = await graph.aget_state({"configurable": {"thread_id": "t1"}})
            s2 = await graph.aget_state({"configurable": {"thread_id": "t2"}})
            return (
                [m.content for m in s1.values["messages"]],
                [m.content for m in s2.values["messages"]],
            )

        assert asyncio.run(run()) == (["a"], ["b"])

    def test_history_newest_first_unique_ids(self):
        from actae_client.adapters.langgraph import ActaeCheckpointSaver

        async def run():
            actae = FakeActae()
            graph = _echo_graph(ActaeCheckpointSaver(actae))
            cfg = {"configurable": {"thread_id": "hist-1"}}
            await graph.ainvoke({"messages": [("user", "one")]}, cfg)
            await graph.ainvoke({"messages": [("user", "two")]}, cfg)
            tuples = [t async for t in graph.aget_state_history(cfg)]
            ids = [t.config["configurable"]["checkpoint_id"] for t in tuples]
            return ids

        ids = asyncio.run(run())
        assert all(ids)
        assert len(set(ids)) == len(ids)
        assert ids == sorted(ids, reverse=True)

    def test_explicit_checkpoint_lookup_and_parent_chain(self):
        from actae_client.adapters.langgraph import ActaeCheckpointSaver

        async def run():
            actae = FakeActae()
            graph = _echo_graph(ActaeCheckpointSaver(actae))
            cfg = {"configurable": {"thread_id": "lookup-1"}}
            await graph.ainvoke({"messages": [("user", "one")]}, cfg)
            await graph.ainvoke({"messages": [("user", "two")]}, cfg)
            newest = [t async for t in graph.aget_state_history(cfg)][0]
            cid = newest.config["configurable"]["checkpoint_id"]
            explicit = await graph.aget_state(
                {"configurable": {"thread_id": "lookup-1", "checkpoint_id": cid}}
            )
            contents = [m.content for m in explicit.values["messages"]]
            parent = [t async for t in graph.aget_state_history(cfg)][1]
            return contents, parent.config["configurable"]["checkpoint_id"]

        contents, parent_id = asyncio.run(run())
        assert contents == ["one", "two"]
        assert parent_id

    def test_interrupt_resume_crash_recovery(self):
        from langgraph.graph import END, START, StateGraph, MessagesState
        from langgraph.types import Command, interrupt
        from actae_client.adapters.langgraph import ActaeCheckpointSaver

        def human(state):
            answer = interrupt({"question": "continue?"})
            return {"messages": [("user", answer)]}

        def answer(state):
            last = state["messages"][-1]
            return {"messages": [("assistant", f"answer: {last.content}")]}

        def build(saver):
            g = StateGraph(MessagesState)
            g.add_node("human", human)
            g.add_node("answer", answer)
            g.add_edge(START, "human")
            g.add_edge("human", "answer")
            g.add_edge("answer", END)
            return g.compile(checkpointer=saver)

        async def run():
            actae = FakeActae()
            graph = build(ActaeCheckpointSaver(actae))
            cfg = {"configurable": {"thread_id": "crash-1"}}
            r1 = await graph.ainvoke({"messages": [("user", "hi")]}, cfg)
            assert "__interrupt__" in r1
            r2 = await graph.ainvoke(Command(resume="4"), cfg)
            assert [m.content for m in r2["messages"]] == ["hi", "4", "answer: 4"]

            # A fresh saver (new process) recovers the thread from Actae.
            graph2 = build(ActaeCheckpointSaver(actae))
            r3 = await graph2.ainvoke({"messages": [("user", "again")]}, cfg)
            assert "__interrupt__" in r3
            r4 = await graph2.ainvoke(Command(resume="9"), cfg)
            return [m.content for m in r4["messages"]]

        assert asyncio.run(run()) == ["hi", "4", "answer: 4", "again", "9", "answer: 9"]

    def test_typed_serde_tool_calls_roundtrip(self):
        from langchain_core.messages import AIMessage
        from actae_client.adapters.langgraph import ActaeCheckpointSaver

        async def run():
            actae = FakeActae()
            saver = ActaeCheckpointSaver(actae)
            checkpoint = {
                "v": 1,
                "id": "chk-typed-1",
                "ts": "2026-01-01T00:00:00Z",
                "channel_values": {
                    "messages": [
                        AIMessage(
                            content="check",
                            tool_calls=[
                                {
                                    "name": "get_weather",
                                    "args": {"city": "SF"},
                                    "id": "call_1",
                                    "type": "tool_call",
                                }
                            ],
                        )
                    ]
                },
                "channel_versions": {},
                "versions_seen": {},
                "pending_sends": [],
            }
            metadata = {"source": "test", "step": 1, "parents": {}}
            cfg = {"configurable": {"thread_id": "typed-1"}}
            returned = await saver.aput(cfg, checkpoint, metadata, {})
            assert returned["configurable"]["checkpoint_id"] == "chk-typed-1"
            tup = await saver.aget_tuple(cfg)
            ai = tup.checkpoint["channel_values"]["messages"][0]
            assert isinstance(ai, AIMessage)
            assert ai.tool_calls == [
                {
                    "name": "get_weather",
                    "args": {"city": "SF"},
                    "id": "call_1",
                    "type": "tool_call",
                }
            ]
            assert ai.content == "check"

        asyncio.run(run())

    def test_aput_writes_idempotent(self):
        from actae_client.adapters.langgraph import ActaeCheckpointSaver

        async def run():
            actae = FakeActae()
            saver = ActaeCheckpointSaver(actae)
            checkpoint = {
                "v": 1,
                "id": "chk-pw-1",
                "ts": "2026-01-01T00:00:00Z",
                "channel_values": {},
                "channel_versions": {},
                "versions_seen": {},
                "pending_sends": [],
            }
            cfg = {"configurable": {"thread_id": "pw-1"}}
            await saver.aput(cfg, checkpoint, {"source": "loop"}, {})
            writes = [("messages", {"content": "partial"})]
            for _ in range(2):
                await saver.aput_writes(
                    {
                        "configurable": {
                            "thread_id": "pw-1",
                            "checkpoint_id": "chk-pw-1",
                        }
                    },
                    writes,
                    "task-1",
                )
            tup = await saver.aget_tuple(cfg)
            return tup.pending_writes

        pending = asyncio.run(run())
        assert len(pending) == 1
        assert pending[0][0] == "task-1"
        assert pending[0][1] == "messages"

    def test_aput_writes_deferred_until_checkpoint_saved(self):
        """put_writes can beat the checkpoint save (background executor race).

        The saver must buffer the writes and attach them once aput lands.
        """
        from actae_client.adapters.langgraph import ActaeCheckpointSaver

        async def run():
            actae = FakeActae()
            saver = ActaeCheckpointSaver(actae)
            target = "chk-deferred-1"
            writes = [("messages", {"content": "early"})]
            cfg = {"configurable": {"thread_id": "deferred-1"}}
            # Target checkpoint does not exist yet: must not raise.
            await saver.aput_writes(
                {"configurable": {"thread_id": "deferred-1", "checkpoint_id": target}},
                writes,
                "task-1",
            )
            assert len(saver._deferred_writes) == 1, "writes buffered for unsaved checkpoint"
            checkpoint = {
                "v": 1,
                "id": target,
                "ts": "2026-01-01T00:00:00Z",
                "channel_values": {},
                "channel_versions": {},
                "versions_seen": {},
                "pending_sends": [],
            }
            await saver.aput(
                {"configurable": {"thread_id": "deferred-1"}},
                checkpoint,
                {"source": "loop"},
                {},
            )
            assert not saver._deferred_writes, "writes should be flushed by aput"
            tup = await saver.aget_tuple(cfg)
            return tup.pending_writes

        pending = asyncio.run(run())
        assert pending == [("task-1", "messages", {"content": "early"})]

    def test_subgraph_namespace_isolation(self):
        from langgraph.graph import END, START, StateGraph, MessagesState
        from actae_client.adapters.langgraph import ActaeCheckpointSaver

        async def run():
            actae = FakeActae()
            saver = ActaeCheckpointSaver(actae)

            inner = StateGraph(MessagesState)
            inner.add_node(
                "inner_echo",
                lambda s: {"messages": [("assistant", "inner:" + s["messages"][-1].content)]},
            )
            inner.add_edge(START, "inner_echo")
            inner.add_edge("inner_echo", END)
            inner_graph = inner.compile()

            outer = StateGraph(MessagesState)
            outer.add_node(
                "outer_echo",
                lambda s: {"messages": [("assistant", "outer:" + s["messages"][-1].content)]},
            )
            outer.add_node("inner", inner_graph)
            outer.add_edge(START, "outer_echo")
            outer.add_edge("outer_echo", "inner")
            outer.add_edge("inner", END)
            graph = outer.compile(checkpointer=saver)

            cfg = {"configurable": {"thread_id": "sub-1"}}
            r1 = await graph.ainvoke({"messages": [("user", "hello")]}, cfg)
            assert [m.content for m in r1["messages"]] == [
                "hello",
                "outer:hello",
                "inner:outer:hello",
            ]

            nss = set()
            for versions in actae.states.values():
                for entry in versions.values():
                    nss.add(entry["state"].get("checkpoint_ns", ""))
            nested_ns = next(ns for ns in nss if ns)
            assert nested_ns.startswith("inner:")

            root_ch = saver.channel_for_config(cfg)
            nested_ch = saver.channel_for_config(
                {"configurable": {"thread_id": "sub-1", "checkpoint_ns": nested_ns}}
            )
            assert root_ch != nested_ch
            assert actae.states.get(root_ch)
            assert actae.states.get(nested_ch)

            tup = await saver.aget_tuple(
                {"configurable": {"thread_id": "sub-1", "checkpoint_ns": nested_ns}}
            )
            contents = [
                m.content for m in tup.checkpoint["channel_values"]["messages"]
            ]
            return contents

        assert asyncio.run(run()) == ["hello", "outer:hello", "inner:outer:hello"]

    def test_sync_in_loop_raises(self):
        from actae_client.adapters.langgraph import (
            ActaeCheckpointSaver,
            ActaeLangGraphSyncError,
        )

        async def run():
            actae = FakeActae()
            graph = _echo_graph(ActaeCheckpointSaver(actae))
            with pytest.raises(ActaeLangGraphSyncError):
                graph.invoke({"messages": [("user", "x")]}, {"configurable": {"thread_id": "loop-1"}})

        asyncio.run(run())

    def test_delete_thread(self):
        from actae_client.adapters.langgraph import ActaeCheckpointSaver

        async def run():
            actae = FakeActae()
            saver = ActaeCheckpointSaver(actae)
            graph = _echo_graph(saver)
            cfg = {"configurable": {"thread_id": "del-1"}}
            await graph.ainvoke({"messages": [("user", "x")]}, cfg)
            assert await saver.aget_tuple(cfg) is not None
            await saver.adelete_thread("del-1")
            return await saver.aget_tuple(cfg)

        assert asyncio.run(run()) is None

    def test_cursor_alignment(self):
        from actae_client.adapters.langgraph import ActaeCheckpointSaver

        async def run():
            actae = FakeActae()
            graph = _echo_graph(ActaeCheckpointSaver(actae))
            cfg = {"configurable": {"thread_id": "cur-1"}}
            await graph.ainvoke({"messages": [("user", "x")]}, cfg)
            await graph.ainvoke({"messages": [("user", "y")]}, cfg)
            cursors = []
            for versions in actae.states.values():
                for key, entry in versions.items():
                    state = entry["state"]
                    assert entry["cursor"] == state["event_cursor"]
                    cursors.append(entry["cursor"])
            return cursors

        cursors = asyncio.run(run())
        assert cursors == sorted(cursors), "every snapshot aligned to a real event cursor"

    def test_channel_for_config(self):
        from actae_client.adapters.langgraph import ActaeLangGraphError, channel_for_config

        cfg = {"configurable": {"thread_id": "t", "checkpoint_ns": "ns"}}
        a = channel_for_config(cfg)
        b = channel_for_config(cfg)
        assert a == b
        assert a != channel_for_config({"configurable": {"thread_id": "t"}})
        assert a != channel_for_config({"configurable": {"thread_id": "other", "checkpoint_ns": "ns"}})
        assert channel_for_config(
            cfg, channel_resolver=lambda c: f"custom-{c['configurable']['thread_id']}"
        ) == "custom-t"
        with pytest.raises(ActaeLangGraphError):
            channel_for_config({"configurable": {}})
        with pytest.raises(ActaeLangGraphError):
            channel_for_config(cfg, channel_resolver=lambda c: "")

    def test_config_specs_and_get_next_version(self):
        from actae_client.adapters.langgraph import ActaeCheckpointSaver

        actae = FakeActae()
        saver = ActaeCheckpointSaver(actae)
        assert saver.config_specs == [
            {"id": "thread_id", "scope": "checkpoint", "default": ""},
            {"id": "checkpoint_ns", "scope": "checkpoint", "default": "default"},
            {"id": "checkpoint_id", "scope": "checkpoint", "default": ""},
        ]
        first = saver.get_next_version(None, None)
        second = saver.get_next_version(first, None)
        assert first.startswith("00000000000000000000000000000001.")
        assert second.startswith("00000000000000000000000000000002.")
        assert first != second

    def test_constructor_validation(self):
        from actae_client.adapters.langgraph import ActaeCheckpointSaver

        with pytest.raises(ValueError):
            ActaeCheckpointSaver(FakeActae(), channel="")
        class Incomplete:
            async def record(self, *a, **k):
                pass

        with pytest.raises(TypeError):
            ActaeCheckpointSaver(Incomplete())

    def test_deprecated_shims(self):
        from actae_client.adapters.langgraph import ActaeCheckpointSaver

        async def run():
            actae = FakeActae()
            saver = ActaeCheckpointSaver(actae)
            checkpoint = {
                "v": 1,
                "id": "chk-dep-1",
                "ts": "2026-01-01T00:00:00Z",
                "channel_values": {},
                "channel_versions": {},
                "versions_seen": {},
                "pending_sends": [],
            }
            with pytest.warns(DeprecationWarning):
                await saver.save_checkpoint(
                    {"configurable": {"thread_id": "dep-1"}}, checkpoint, {"source": "test"}
                )
            with pytest.warns(DeprecationWarning):
                result = await saver.get_checkpoint({"configurable": {"thread_id": "dep-1"}})
            return result["checkpoint"]["id"]

        assert asyncio.run(run()) == "chk-dep-1"

    def test_alist_filter_before_limit(self):
        from actae_client.adapters.langgraph import ActaeCheckpointSaver

        async def run():
            actae = FakeActae()
            graph = _echo_graph(ActaeCheckpointSaver(actae))
            cfg = {"configurable": {"thread_id": "flt-1"}}
            await graph.ainvoke({"messages": [("user", "one")]}, cfg)
            await graph.ainvoke({"messages": [("user", "two")]}, cfg)
            all_tuples = [t async for t in graph.aget_state_history(cfg)]
            limit1 = [t async for t in graph.aget_state_history(cfg, limit=1)]
            before1 = [
                t
                async for t in graph.aget_state_history(
                    cfg, before=all_tuples[0].config
                )
            ]
            return len(all_tuples), len(limit1), len(before1)

        total, limited, before = asyncio.run(run())
        assert total > 1
        assert limited == 1
        assert before == total - 1

    def test_module_imports_without_langgraph(self):
        code = textwrap.dedent(
            """
            import sys

            class _Blocker:
                def find_spec(self, fullname, path=None, target=None):
                    if (
                        fullname == "langgraph"
                        or fullname.startswith("langgraph.")
                        or fullname == "langchain_core"
                        or fullname.startswith("langchain_core.")
                    ):
                        raise ImportError(f"blocked: {fullname}")
                    return None

            sys.meta_path.insert(0, _Blocker())
            import actae_client.adapters.langgraph as m
            print("has_langgraph:", m._HAS_LANGGRAPH)
            try:
                m.ActaeCheckpointSaver(None)
                print("UNEXPECTED")
            except ImportError as e:
                print("ok:", str(e)[:40])
            """
        )
        result = subprocess.run(
            [sys.executable, "-c", code], capture_output=True, text=True
        )
        assert result.returncode == 0, result.stderr
        assert "has_langgraph: False" in result.stdout
        assert "ok: ActaeCheckpointSaver requires langgraph" in result.stdout


class TestCrewAIAdapter:
    def test_after_step_saves_state(self):
        from actae_client.adapters.crewai import ActaeCrewStateHook

        actae = _mock_actae()
        hook = ActaeCrewStateHook(actae, channel="crew-test")

        mock_output = MagicMock()
        mock_output.description = "task-1"
        mock_output.raw = "output-1"

        asyncio.run(hook.after_step(mock_output))
        actae.save_state.assert_awaited_once()

    def test_after_step_appends_completed_tasks(self):
        from actae_client.adapters.crewai import ActaeCrewStateHook

        actae = _mock_actae()
        hook = ActaeCrewStateHook(actae, channel="crew-test")

        for i in range(3):
            output = MagicMock()
            output.description = f"task-{i}"
            output.raw = f"output-{i}"
            asyncio.run(hook.after_step(output))

        assert len(hook._completed) == 3
        assert hook._completed == ["task-0", "task-1", "task-2"]

    def test_after_step_with_crew(self):
        from actae_client.adapters.crewai import ActaeCrewStateHook

        actae = _mock_actae()
        hook = ActaeCrewStateHook(actae, channel="crew-test")

        mock_output = MagicMock()
        mock_output.description = "task-1"
        mock_output.raw = "output-1"

        mock_crew = MagicMock()
        mock_crew.id = "crew-42"

        state = asyncio.run(hook.after_step(mock_output, crew=mock_crew))
        assert state["crew_id"] == "crew-42"

    def test_load_state(self):
        from actae_client.adapters.crewai import ActaeCrewStateHook

        actae = _mock_actae()
        hook = ActaeCrewStateHook(actae, channel="crew-test")

        state = asyncio.run(hook.load_state())
        assert state == {"key": "val"}

    def test_load_state_none(self):
        from actae_client.adapters.crewai import ActaeCrewStateHook

        actae = _mock_actae()
        actae.latest_state = AsyncMock(return_value=None)
        hook = ActaeCrewStateHook(actae, channel="crew-test")

        state = asyncio.run(hook.load_state())
        assert state is None

    def test_save_state(self):
        from actae_client.adapters.crewai import ActaeCrewStateHook

        actae = _mock_actae()
        hook = ActaeCrewStateHook(actae, channel="crew-test")

        asyncio.run(hook.save_state({"custom": "state"}))
        actae.save_state.assert_awaited_once()

    def test_resumer_get_remaining_tasks_all_done(self):
        from actae_client.adapters.crewai import CrewAIResumer

        actae = _mock_actae()
        actae.latest_state = AsyncMock(return_value={
            "cursor": 42,
            "state": {"completed_tasks": ["task-0", "task-1"], "task_outputs": []},
        })
        resumer = CrewAIResumer(actae, channel="crew-test")

        remaining = asyncio.run(resumer.get_remaining_tasks(["task-0", "task-1"]))
        assert remaining == []

    def test_resumer_get_remaining_tasks_some_remaining(self):
        from actae_client.adapters.crewai import CrewAIResumer

        actae = _mock_actae()
        actae.latest_state = AsyncMock(return_value={
            "cursor": 42,
            "state": {"completed_tasks": ["task-0"], "task_outputs": []},
        })
        resumer = CrewAIResumer(actae, channel="crew-test")

        remaining = asyncio.run(resumer.get_remaining_tasks(["task-0", "task-1"]))
        assert remaining == ["task-1"]

    def test_resumer_get_remaining_tasks_no_state(self):
        from actae_client.adapters.crewai import CrewAIResumer

        actae = _mock_actae()
        actae.latest_state = AsyncMock(return_value=None)
        resumer = CrewAIResumer(actae, channel="crew-test")

        remaining = asyncio.run(resumer.get_remaining_tasks(["task-0", "task-1"]))
        assert remaining == ["task-0", "task-1"]

    def test_resumer_get_remaining_tasks_string_tasks(self):
        from actae_client.adapters.crewai import CrewAIResumer

        actae = _mock_actae()
        actae.latest_state = AsyncMock(return_value={
            "cursor": 42,
            "state": {"completed_tasks": ["task-0"], "task_outputs": []},
        })
        resumer = CrewAIResumer(actae, channel="crew-test")

        remaining = asyncio.run(resumer.get_remaining_tasks(["task-0", "task-1"]))
        assert remaining == ["task-1"]

    def test_resume_all_tasks_done_returns_none(self):
        from actae_client.adapters.crewai import CrewAIResumer

        actae = _mock_actae()
        actae.latest_state = AsyncMock(return_value={
            "cursor": 42,
            "state": {"completed_tasks": ["task-0"], "task_outputs": []},
        })
        resumer = CrewAIResumer(actae, channel="crew-test")

        result = asyncio.run(resumer.resume(None, ["task-0"]))
        assert result is None

    def test_resumer_fork_returns_resumer_for_new_channel(self):
        from actae_client.adapters.crewai import CrewAIResumer

        actae = _mock_actae()
        resumer = CrewAIResumer(actae, channel="crew-test")
        forked = asyncio.run(resumer.fork("crew-fork", reason="refine"))
        assert isinstance(forked, CrewAIResumer)
        assert forked.channel == "crew-fork"
        actae.fork.assert_awaited_once()
        args = actae.fork.await_args
        assert args[0][0] == "crew-test"
        assert args[0][1] == "crew-fork"
        assert args[0][2] == 42


@pytest.mark.skipif(_SKIP_LC, reason="LangChain installed — base class tests skipped")
class TestLangChainAdapter:
    def test_context_saver_initializes_empty(self):
        from actae_client.adapters.langchain import ActaeContextSaver

        actae = _mock_actae()
        saver = ActaeContextSaver(actae, channel="lc-test", save_every_n=3)

        assert saver.save_every_n == 3
        assert saver._context["messages"] == []
        assert saver._context["tool_outputs"] == []
        assert saver._context["chain_steps"] == []

    def test_on_llm_end_appends_message(self):
        from actae_client.adapters.langchain import ActaeContextSaver

        actae = _mock_actae()
        saver = ActaeContextSaver(actae, channel="lc-test", save_every_n=1)

        response = MagicMock()
        response.generations = [[MagicMock(text="Hello world")]]

        saver.on_llm_end(response)
        assert len(saver._context["messages"]) == 1
        assert saver._context["messages"][0]["content"] == "Hello world"

    def test_on_llm_end_with_message_generation(self):
        from actae_client.adapters.langchain import ActaeContextSaver

        actae = _mock_actae()
        saver = ActaeContextSaver(actae, channel="lc-test", save_every_n=1)

        response = MagicMock()
        response.generations = [[MagicMock(message="AI response")]]

        saver.on_llm_end(response)
        assert saver._context["messages"][0]["content"] == "AI response"

    def test_on_tool_end_appends(self):
        from actae_client.adapters.langchain import ActaeContextSaver

        actae = _mock_actae()
        saver = ActaeContextSaver(actae, channel="lc-test", save_every_n=1)

        saver.on_tool_end({"result": "success"})
        assert len(saver._context["tool_outputs"]) == 1
        assert saver._context["tool_outputs"][0]["output"]["result"] == "success"

    def test_on_chain_end_appends_and_triggers_save(self):
        from actae_client.adapters.langchain import ActaeContextSaver

        actae = _mock_actae()
        saver = ActaeContextSaver(actae, channel="lc-test")

        saver.on_chain_end({"final": "output"})
        assert len(saver._context["chain_steps"]) == 1

    def test_load_context(self):
        from actae_client.adapters.langchain import ActaeContextSaver

        actae = _mock_actae()
        actae.latest_state = AsyncMock(return_value={
            "cursor": 42,
            "state": {"context": {"messages": [{"role": "user", "content": "hi"}]}},
        })
        saver = ActaeContextSaver(actae, channel="lc-test")

        context = asyncio.run(saver.load_context())
        assert context["messages"][0]["content"] == "hi"

    def test_load_context_no_state(self):
        from actae_client.adapters.langchain import ActaeContextSaver

        actae = _mock_actae()
        actae.latest_state = AsyncMock(return_value=None)
        saver = ActaeContextSaver(actae, channel="lc-test")

        context = asyncio.run(saver.load_context())
        assert context is None

    def test_serialize_primitives(self):
        from actae_client._utils import serialize

        assert serialize(None) is None
        assert serialize(42) == 42
        assert serialize("hello") == "hello"

    def test_serialize_complex(self):
        from actae_client._utils import serialize

        result = serialize({"a": [1, 2], "b": {"c": True}})
        assert result == {"a": [1, 2], "b": {"c": True}}

    def test_serialize_non_json_falls_back_to_string(self):
        from actae_client._utils import serialize

        class Custom:
            def __str__(self):
                return "custom-str"

        assert serialize(Custom()) == "custom-str"

    def test_serialize_exception_falls_back_to_repr(self):
        from actae_client._utils import serialize

        class Broken:
            def __str__(self):
                raise ValueError("broken")
            def __repr__(self):
                return "broken-repr"

        assert serialize(Broken()) == "broken-repr"

    def test_chain_resumer_resume_with_context(self):
        from actae_client.adapters.langchain import ChainResumer

        actae = _mock_actae()
        actae.latest_state = AsyncMock(return_value={
            "cursor": 42,
            "state": {"context": {"messages": [{"role": "assistant", "content": "prev response"}]}},
        })

        resumer = ChainResumer(actae, channel="lc-test")

        mock_chain = MagicMock()
        mock_chain.invoke = MagicMock(return_value="chain result")

        result = asyncio.run(resumer.resume(mock_chain, "default input"))
        assert result == "chain result"
        # Should invoke with the last message's content, not the default
        assert mock_chain.invoke.call_args[0][0] == "prev response"

    def test_chain_resumer_resume_without_context(self):
        from actae_client.adapters.langchain import ChainResumer

        actae = _mock_actae()
        actae.latest_state = AsyncMock(return_value=None)

        resumer = ChainResumer(actae, channel="lc-test")

        mock_chain = MagicMock()
        mock_chain.invoke = MagicMock(return_value="chain result")

        result = asyncio.run(resumer.resume(mock_chain, "default input"))
        assert result == "chain result"
        assert mock_chain.invoke.call_args[0][0] == "default input"

    def test_chain_resumer_fork_returns_resumer_for_new_channel(self):
        from actae_client.adapters.langchain import ChainResumer

        actae = _mock_actae()
        resumer = ChainResumer(actae, channel="lc-test")
        forked = asyncio.run(resumer.fork("lc-fork", reason="refine"))
        assert isinstance(forked, ChainResumer)
        assert forked.channel == "lc-fork"
        actae.fork.assert_awaited_once()
        args = actae.fork.await_args
        assert args[0][0] == "lc-test"
        assert args[0][1] == "lc-fork"
        assert args[0][2] == 42
