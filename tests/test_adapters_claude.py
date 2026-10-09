import asyncio
import copy
import subprocess
import sys
import textwrap
from typing import Any, Dict, List, Optional

import pytest

pytest.importorskip("claude_agent_sdk")

from actae_client.adapters.claude import (
    ActaeClaudeHook,
    ActaeClaudeSessionStore,
    channel_for_session,
)
from actae_client.errors import SnapshotBoundaryError


class FakeActae:
    """In-memory Actae client implementing the primitives the adapters use."""

    def __init__(self):
        self.states: Dict[str, List[Dict[str, Any]]] = {}
        self.events: List[Dict[str, Any]] = []

    async def latest_cursor(self, channel_id: str) -> Optional[int]:
        return 0 if channel_id in self.events_by_channel() else None

    def events_by_channel(self) -> Dict[str, int]:
        counts: Dict[str, int] = {}
        for ev in self.events:
            counts[ev["channel_id"]] = counts.get(ev["channel_id"], 0) + 1
        return counts

    async def transition(
        self,
        channel_id: str,
        event_type: str,
        payload: Any,
        state: Any,
        *,
        actor: str = "system",
        **kwargs: Any,
    ):
        cursor = (await self.latest_cursor(channel_id) or 0) + 1
        self.events.append(
            {
                "channel_id": channel_id,
                "type": event_type,
                "payload": payload,
                "actor": actor,
                "cursor": cursor,
            }
        )
        self.states.setdefault(channel_id, []).append(
            {"version": len(self.states.get(channel_id, [])) + 1,
             "cursor": cursor, "state": state}
        )
        return cursor

    async def save_state(self, channel_id: str, cursor: int, state: Any) -> int:
        self.states.setdefault(channel_id, []).append(
            {"version": len(self.states.get(channel_id, [])) + 1,
             "cursor": cursor, "state": state}
        )
        return len(self.states[channel_id])

    async def latest_state(self, channel_id: str) -> Optional[Dict[str, Any]]:
        versions = self.states.get(channel_id)
        if not versions:
            return None
        latest = versions[-1]
        return {"cursor": latest["cursor"], "state": copy.deepcopy(latest["state"])}

    async def record(self, channel_id: str, event_type: str, payload: Any, **kwargs: Any):
        cursor = (await self.latest_cursor(channel_id) or 0) + 1
        self.events.append(
            {"channel_id": channel_id, "type": event_type,
             "payload": payload, "actor": kwargs.get("actor"), "cursor": cursor}
        )

    async def fork(self, source_channel_id: str, new_channel_id: str, at_cursor: int,
                   *, display_name=None, reason=None, **kwargs: Any):
        """Mirror the server: copy the source's latest state to the child."""
        src = self.states.get(source_channel_id)
        if not src:
            raise SnapshotBoundaryError("no state")
        latest = src[-1]
        self.states.setdefault(new_channel_id, []).append(
            {"version": 1, "cursor": latest["cursor"],
             "state": copy.deepcopy(latest["state"])}
        )
        self.events.append(
            {"channel_id": new_channel_id, "type": "fork.started",
             "payload": {"source_channel_id": source_channel_id}, "actor": "system",
             "cursor": 1}
        )
        return {"channel_id": new_channel_id}

    def state_versions(self, channel_id: str) -> List[Dict[str, Any]]:
        return list(self.states.get(channel_id, []))


def _key(project="proj-a", session="sess-1", subpath=None):
    k = {"project_key": project, "session_id": session}
    if subpath is not None:
        k["subpath"] = subpath
    return k


def _entry(etype="assistant_message", uuid="u1"):
    return {"type": etype, "uuid": uuid, "timestamp": "2026-01-01T00:00:00Z"}


class TestChannelResolution:
    def test_deterministic(self):
        a = channel_for_session("proj", "sess")
        b = channel_for_session("proj", "sess")
        assert a == b

    def test_distinct_per_session_and_subpath(self):
        main = channel_for_session("proj", "sess")
        other = channel_for_session("proj", "other")
        sub = channel_for_session("proj", "sess", "subagents/x")
        assert len({main, other, sub}) == 3
        assert main.startswith("claude:")

    def test_distinct_per_project(self):
        assert channel_for_session("p1", "s") != channel_for_session("p2", "s")


class TestClaudeSessionStore:
    def test_append_and_load(self):
        actae = FakeActae()
        store = ActaeClaudeSessionStore(actae)
        asyncio.run(store.append(_key(), [_entry("user_message"), _entry()]))
        entries = asyncio.run(store.load(_key()))
        assert len(entries) == 2
        assert entries[0]["type"] == "user_message"

    def test_load_absent_returns_none(self):
        actae = FakeActae()
        store = ActaeClaudeSessionStore(actae)
        assert asyncio.run(store.load(_key())) is None

    def test_append_accumulates(self):
        actae = FakeActae()
        store = ActaeClaudeSessionStore(actae)
        asyncio.run(store.append(_key(), [_entry(etype="user_message", uuid="1")]))
        asyncio.run(store.append(_key(), [_entry(etype="assistant_message", uuid="2")]))
        entries = asyncio.run(store.load(_key()))
        assert [e["uuid"] for e in entries] == ["1", "2"]

    def test_append_persists_versioned_snapshots(self):
        actae = FakeActae()
        store = ActaeClaudeSessionStore(actae)
        ch = channel_for_session("proj-a", "sess-1")
        asyncio.run(store.append(_key(), [_entry(etype="user_message")]))
        asyncio.run(store.append(_key(), [_entry(etype="assistant_message")]))
        versions = actae.state_versions(ch)
        assert len(versions) == 2
        assert [v["state"]["entries"][-1]["type"] for v in versions] == [
            "user_message",
            "assistant_message",
        ]

    def test_append_streams_events(self):
        actae = FakeActae()
        store = ActaeClaudeSessionStore(actae)
        asyncio.run(store.append(_key(), [_entry()]))
        ch = channel_for_session("proj-a", "sess-1")
        append_events = [
            e for e in actae.events
            if e["channel_id"] == ch and e["type"] == "claude.session.append"
        ]
        assert len(append_events) == 1
        assert append_events[0]["actor"] == "claude-session-store"
        assert append_events[0]["payload"]["count"] == 1
        assert append_events[0]["payload"]["last_type"] == "assistant_message"

    def test_survives_restart(self):
        actae = FakeActae()
        asyncio.run(ActaeClaudeSessionStore(actae).append(_key(), [_entry(etype="user_message")]))
        fresh = ActaeClaudeSessionStore(actae)
        entries = asyncio.run(fresh.load(_key()))
        assert entries[0]["type"] == "user_message"

    def test_subpath_isolated_channel_and_indexed(self):
        actae = FakeActae()
        store = ActaeClaudeSessionStore(actae)
        asyncio.run(store.append(_key(), [_entry(etype="user_message")]))
        asyncio.run(store.append(_key(subpath="subagents/abc"), [_entry(etype="assistant_message")]))
        sub_entries = asyncio.run(store.load(_key(subpath="subagents/abc")))
        assert sub_entries[0]["type"] == "assistant_message"
        subkeys = asyncio.run(store.list_subkeys({"project_key": "proj-a", "session_id": "sess-1"}))
        assert subkeys == ["subagents/abc"]
        main_entries = asyncio.run(store.load(_key()))
        assert len(main_entries) == 1

    def test_list_sessions(self):
        actae = FakeActae()
        store = ActaeClaudeSessionStore(actae)
        asyncio.run(store.append(_key(session="s1"), [_entry()]))
        asyncio.run(store.append(_key(session="s2"), [_entry()]))
        sessions = asyncio.run(store.list_sessions("proj-a"))
        assert sorted(s["session_id"] for s in sessions) == ["s1", "s2"]
        assert all(s["mtime"] > 0 for s in sessions)

    def test_list_sessions_is_project_scoped(self):
        actae = FakeActae()
        store = ActaeClaudeSessionStore(actae)
        asyncio.run(store.append(_key(project="proj-a", session="s1"), [_entry()]))
        asyncio.run(store.append(_key(project="proj-b", session="s1"), [_entry()]))
        assert len(asyncio.run(store.list_sessions("proj-a"))) == 1
        assert len(asyncio.run(store.list_sessions("proj-b"))) == 1

    def test_list_session_summaries(self):
        actae = FakeActae()
        store = ActaeClaudeSessionStore(actae)
        asyncio.run(store.append(_key(), [_entry(etype="user_message")]))
        summaries = asyncio.run(store.list_session_summaries("proj-a"))
        assert len(summaries) == 1
        assert summaries[0]["session_id"] == "sess-1"
        assert "data" in summaries[0]
        assert summaries[0]["mtime"] > 0

    def test_delete_main_cascades_subkeys(self):
        actae = FakeActae()
        store = ActaeClaudeSessionStore(actae)
        asyncio.run(store.append(_key(), [_entry()]))
        asyncio.run(store.append(_key(subpath="subagents/abc"), [_entry()]))
        asyncio.run(store.delete(_key()))
        assert asyncio.run(store.load(_key())) == []
        assert asyncio.run(store.load(_key(subpath="subagents/abc"))) == []
        assert asyncio.run(store.list_sessions("proj-a")) == []
        assert asyncio.run(store.list_subkeys({"project_key": "proj-a", "session_id": "sess-1"})) == []

    def test_delete_subpath_only(self):
        actae = FakeActae()
        store = ActaeClaudeSessionStore(actae)
        asyncio.run(store.append(_key(), [_entry()]))
        asyncio.run(store.append(_key(subpath="subagents/abc"), [_entry()]))
        asyncio.run(store.delete(_key(subpath="subagents/abc")))
        assert asyncio.run(store.load(_key())) != []
        assert asyncio.run(store.load(_key(subpath="subagents/abc"))) == []
        subkeys = asyncio.run(store.list_subkeys({"project_key": "proj-a", "session_id": "sess-1"}))
        assert subkeys == []

    def test_mtime_strictly_increasing(self):
        actae = FakeActae()
        store = ActaeClaudeSessionStore(actae)
        m1 = store._next_mtime()
        m2 = store._next_mtime()
        m3 = store._next_mtime()
        assert m1 < m2 < m3

    def test_requires_claude_sdk(self):
        code = textwrap.dedent(
            """
            import sys

            class _Blocker:
                def find_spec(self, fullname, path=None, target=None):
                    if fullname == "claude_agent_sdk" or fullname.startswith("claude_agent_sdk."):
                        raise ImportError(f"blocked: {fullname}")
                    return None

            sys.meta_path.insert(0, _Blocker())
            import actae_client.adapters.claude as m
            print("has_sdk:", m._HAS_CLAUDE_SDK)
            print("hook_buildable_without_sdk:", m.ActaeClaudeHook is not None)
            try:
                m.ActaeClaudeSessionStore(None)
                print("UNEXPECTED")
            except ImportError as e:
                print("ok:", str(e)[:60])
            """
        )
        result = subprocess.run(
            [sys.executable, "-c", code], capture_output=True, text=True
        )
        assert result.returncode == 0, result.stderr
        assert "has_sdk: False" in result.stdout
        assert "ok: ActaeClaudeSessionStore requires claude-agent-sdk" in result.stdout


class TestClaudeSessionStoreFork:
    def test_fork_session_copies_transcript_to_new_session(self):
        actae = FakeActae()
        store = ActaeClaudeSessionStore(actae)
        asyncio.run(store.append(_key(), [_entry(etype="user_message", uuid="1")]))
        asyncio.run(store.append(_key(), [_entry(etype="assistant_message", uuid="2")]))

        new_key = asyncio.run(store.fork_session("proj-a", "sess-1", "sess-2"))
        assert new_key["session_id"] == "sess-2"
        # The fork's transcript is identical to the source's.
        forked = asyncio.run(store.load(new_key))
        assert [e["uuid"] for e in forked] == ["1", "2"]

    def test_fork_session_registers_in_project_index(self):
        actae = FakeActae()
        store = ActaeClaudeSessionStore(actae)
        asyncio.run(store.append(_key(), [_entry(etype="user_message", uuid="1")]))
        asyncio.run(store.fork_session("proj-a", "sess-1", "sess-2"))
        sessions = asyncio.run(store.list_sessions("proj-a"))
        ids = {s["session_id"] for s in sessions}
        assert "sess-2" in ids, "forked session must be discoverable"

    def test_fork_session_missing_transcript_raises(self):
        actae = FakeActae()
        store = ActaeClaudeSessionStore(actae)
        with pytest.raises(ValueError, match="no transcript"):
            asyncio.run(store.fork_session("proj-a", "missing", "sess-2"))

    def test_fork_session_preserves_context_for_resume(self):
        actae = FakeActae()
        store = ActaeClaudeSessionStore(actae)
        # 3-turn conversation.
        for i, t in enumerate(["user_message", "assistant_message", "user_message"]):
            asyncio.run(store.append(_key(), [_entry(etype=t, uuid=str(i))]))
        asyncio.run(store.fork_session("proj-a", "sess-1", "sess-fix"))
        forked = asyncio.run(store.load({"project_key": "proj-a", "session_id": "sess-fix"}))
        # Full context carried over: the fork resumes with all 3 prior turns.
        assert len(forked) == 3
        assert [e["uuid"] for e in forked] == ["0", "1", "2"]


class TestClaudeHook:
    def test_hooks_builds_matchers(self):
        actae = FakeActae()
        hook = ActaeClaudeHook(actae)
        hooks = hook.hooks
        assert set(hooks.keys()) == {
            "PreToolUse", "PostToolUse", "PostToolUseFailure",
            "UserPromptSubmit", "Stop",
        }
        for matchers in hooks.values():
            assert len(matchers) == 1
            assert matchers[0].matcher is None

    def test_hook_records_events(self):
        actae = FakeActae()
        hook = ActaeClaudeHook(actae)
        asyncio.run(hook(
            {
                "hook_event_name": "PostToolUse",
                "session_id": "sess-1",
                "tool_name": "Bash",
                "tool_input": {"command": "ls"},
                "tool_response": "file.txt",
            },
            "tooluse_123",
        ))
        assert len(actae.events) == 1
        ev = actae.events[0]
        assert ev["type"] == "claude.hook.posttooluse"
        assert ev["payload"]["tool_name"] == "Bash"
        assert ev["payload"]["tool_use_id"] == "tooluse_123"
        assert ev["payload"]["tool_response"] == "file.txt"
        assert ev["channel_id"].startswith("claude-hooks:")

    def test_hook_failure_never_raises(self):
        class BoomActae(FakeActae):
            async def record(self, *args, **kwargs):
                raise RuntimeError("server down")

        hook = ActaeClaudeHook(BoomActae())
        result = asyncio.run(hook({"hook_event_name": "Stop", "session_id": "s"}))
        assert result == {}

    def test_hook_trims_long_responses(self):
        actae = FakeActae()
        hook = ActaeClaudeHook(actae)
        asyncio.run(hook(
            {
                "hook_event_name": "PostToolUse",
                "session_id": "s",
                "tool_name": "Read",
                "tool_input": {},
                "tool_response": "x" * 5000,
            },
        ))
        ev = actae.events[0]
        assert len(ev["payload"]["tool_response"]) == 2000
