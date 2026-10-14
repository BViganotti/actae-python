"""Live Actae + LangGraph integration tests.

These tests require a running Actae server and are skipped otherwise:

    ACTAE_URL=http://localhost:8002 ACTAE_API_KEY=sk-dev-0000000000000000000000 \
        python3 -m pytest sdks/python/tests/test_langgraph_live.py -v

`ACTAE_ENDPOINT` is accepted as an alias for `ACTAE_URL`.
"""

import asyncio
import os
import uuid

import pytest

from actae_client import ActaeClient
from actae_client.adapters.langgraph import _HAS_LANGGRAPH

if _HAS_LANGGRAPH:
    from langgraph.types import Command

_ENDPOINT = os.environ.get("ACTAE_URL") or os.environ.get("ACTAE_ENDPOINT")
_API_KEY = os.environ.get("ACTAE_API_KEY")

pytestmark = [
    pytest.mark.skipif(
        not (_ENDPOINT and _API_KEY), reason="ACTAE_URL/ACTAE_API_KEY not set"
    ),
    pytest.mark.skipif(not _HAS_LANGGRAPH, reason="langgraph not installed"),
]


def _client():
    client = ActaeClient(
        api_key=_API_KEY,
        endpoint=_ENDPOINT,
    )
    return client


def _thread_id(prefix):
    return f"lg-live-{prefix}-{uuid.uuid4().hex[:8]}"


def _build_graph(saver):
    from langgraph.graph import END, START, MessagesState, StateGraph
    from langgraph.types import interrupt

    def human(state):
        answer = interrupt({"question": "continue?"})
        return {"messages": [("user", answer)]}

    def answer(state):
        last = state["messages"][-1]
        return {"messages": [("assistant", f"answer: {last.content}")]}

    def echo(state):
        return {"messages": state["messages"]}

    g = StateGraph(MessagesState)
    g.add_node("human", human)
    g.add_node("answer", answer)
    g.add_node("echo", echo)
    g.add_edge(START, "human")
    g.add_edge("human", "answer")
    g.add_edge("answer", "echo")
    g.add_edge("echo", END)
    return g.compile(checkpointer=saver)


@pytest.fixture(scope="module")
def actae():
    client = _client()
    yield client
    asyncio.run(client.disconnect())


def test_multi_turn_memory(actae):
    from actae_client.adapters.langgraph import ActaeCheckpointSaver

    saver = ActaeCheckpointSaver(actae)
    graph = _build_graph(saver)
    cfg = {"configurable": {"thread_id": _thread_id("memory")}}

    r1 = asyncio.run(graph.ainvoke({"messages": [("user", "hello")]}, cfg))
    assert "__interrupt__" in r1
    r2 = asyncio.run(graph.ainvoke(Command(resume="world"), cfg))
    contents = [m.content for m in r2["messages"]]
    assert contents == ["hello", "world", "answer: world"], contents


def test_history_and_explicit_lookup(actae):
    from actae_client.adapters.langgraph import ActaeCheckpointSaver

    saver = ActaeCheckpointSaver(actae)
    graph = _build_graph(saver)
    cfg = {"configurable": {"thread_id": _thread_id("history")}}

    asyncio.run(graph.ainvoke({"messages": [("user", "one")]}, cfg))
    asyncio.run(graph.ainvoke(Command(resume="two"), cfg))

    hist = asyncio.run(_collect(graph.aget_state_history(cfg)))
    ids = [t.config["configurable"]["checkpoint_id"] for t in hist]
    assert all(ids), "every history entry must carry a checkpoint_id"
    assert len(set(ids)) == len(ids), "checkpoint ids must be unique"
    assert ids == sorted(ids, reverse=True), "history must be newest-first"

    cid = ids[0]
    explicit = asyncio.run(
        graph.aget_state(
            {"configurable": {"thread_id": cfg["configurable"]["thread_id"], "checkpoint_id": cid}}
        )
    )
    assert [m.content for m in explicit.values["messages"]] == ["one", "two", "answer: two"]


def test_crash_recovery_fresh_saver(actae):
    from actae_client.adapters.langgraph import ActaeCheckpointSaver

    thread = _thread_id("crash")
    cfg = {"configurable": {"thread_id": thread}}

    saver1 = ActaeCheckpointSaver(actae)
    graph1 = _build_graph(saver1)
    asyncio.run(graph1.ainvoke({"messages": [("user", "first")]}, cfg))

    saver2 = ActaeCheckpointSaver(actae)
    graph2 = _build_graph(saver2)
    r = asyncio.run(graph2.ainvoke(Command(resume="second"), cfg))
    contents = [m.content for m in r["messages"]]
    assert contents == ["first", "second", "answer: second"], contents


def test_thread_isolation(actae):
    from actae_client.adapters.langgraph import ActaeCheckpointSaver

    saver = ActaeCheckpointSaver(actae)
    graph = _build_graph(saver)
    cfg_a = {"configurable": {"thread_id": _thread_id("iso-a")}}
    cfg_b = {"configurable": {"thread_id": _thread_id("iso-b")}}

    asyncio.run(graph.ainvoke({"messages": [("user", "a1")]}, cfg_a))
    asyncio.run(graph.ainvoke({"messages": [("user", "b1")]}, cfg_b))
    asyncio.run(graph.ainvoke(Command(resume="a2"), cfg_a))

    state_b = asyncio.run(graph.aget_state(cfg_b))
    assert [m.content for m in state_b.values["messages"]] == ["b1"]

    state_a = asyncio.run(graph.aget_state(cfg_a))
    assert [m.content for m in state_a.values["messages"]] == ["a1", "a2", "answer: a2"]


def test_sync_invoke_live(actae):
    from actae_client.adapters.langgraph import ActaeCheckpointSaver

    saver = ActaeCheckpointSaver(actae)
    graph = _build_graph(saver)
    cfg = {"configurable": {"thread_id": _thread_id("sync")}}

    r1 = graph.invoke({"messages": [("user", "sync-first")]}, cfg)
    assert "__interrupt__" in r1
    r2 = graph.invoke(Command(resume="sync-second"), cfg)
    contents = [m.content for m in r2["messages"]]
    assert contents == ["sync-first", "sync-second", "answer: sync-second"], contents


async def _collect(async_iter):
    return [item async for item in async_iter]
