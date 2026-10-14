"""
Claude Agent SDK Session Store adapter for Actae.

Implements the ``claude_agent_sdk.SessionStore`` protocol backed by Actae,
giving the Claude Agent SDK durable, cursor-aligned conversation state that
survives process restarts, is forkable at any transcript point, and streams
every transcript append as a real-time event.

Usage:
    from actae_client.adapters.claude import ActaeClaudeSessionStore
    from claude_agent_sdk import ClaudeAgentOptions, query

    store = ActaeClaudeSessionStore(actae_client)

    async for message in query(
        prompt="Refactor this module",
        options=ClaudeAgentOptions(session_store=store),
    ):
        ...  # store persists the full transcript in Actae automatically

    # On restart, continue the same conversation:
    async for message in query(
        prompt="Now run the tests",
        options=ClaudeAgentOptions(session_store=store, resume=True),
    ):
        ...
"""

from __future__ import annotations

import hashlib
import logging
import threading
import time
from typing import Any, Dict, List, Optional

from ..client import ActaeClient
from .contract import adapter_checkpoint_state

try:
    from claude_agent_sdk.types import (
        SessionKey,
        SessionListSubkeysKey,
        SessionStoreEntry,
        SessionStoreListEntry,
        SessionSummaryEntry,
    )
    from claude_agent_sdk import fold_session_summary

    _HAS_CLAUDE_SDK = True
except ImportError:  # pragma: no cover - exercised via subprocess test
    _HAS_CLAUDE_SDK = False

logger = logging.getLogger("actae_client.adapters.claude")

_EVENT_APPEND = "claude.session.append"
_EVENT_DELETE = "claude.session.delete"
_ACTOR = "claude-session-store"


def _digest(*parts: str) -> str:
    return hashlib.sha256("\x00".join(parts).encode("utf-8")).hexdigest()[:16]


def channel_for_session(
    project_key: str,
    session_id: str,
    subpath: Optional[str] = None,
) -> str:
    """Deterministically resolve the Actae channel for a Claude session key.

    Main transcripts and their subkey transcripts (subagent transcripts,
    metadata) map to distinct channels::

        claude:<sha256(project_key \x00 session_id)[:16]>          # main transcript
        claude:<sha256(project_key \x00 session_id \x00 sub)[:16]> # subkey

    Args:
        project_key: Claude project key (directory-relative project name).
        session_id: Claude session ID.
        subpath: Optional subkey (e.g. ``"subagents/<uuid>"``).
    """
    if subpath is None:
        return f"claude:{_digest(project_key, session_id)}"
    return f"claude:{_digest(project_key, session_id, subpath)}"


def _channel_for_index(project_key: str) -> str:
    return f"claude:idx:{_digest(project_key)}"


class ActaeClaudeSessionStore:
    """Durable Claude Agent SDK ``SessionStore`` backed by Actae.

    Every transcript append is persisted as a cursor-aligned, versioned
    state snapshot on a deterministic per-session channel and streamed as a
    ``claude.session.append`` event — so a running Claude conversation is
    visible in real time, replayable, and forkable from any transcript point.

    Pass an instance as ``session_store=...`` in ``ClaudeAgentOptions``.
    """

    def __init__(self, actae: ActaeClient):
        """Create a Claude session store.

        Args:
            actae: Connected ActaeClient instance.

        Raises:
            ImportError: If ``claude-agent-sdk`` is not installed (install
                with ``pip install 'actae-client[claude]'``).
        """
        if not _HAS_CLAUDE_SDK:
            raise ImportError(
                "ActaeClaudeSessionStore requires claude-agent-sdk; "
                "install with `pip install 'actae-client[claude]'`"
            )
        self.actae = actae
        self._mtimes: Dict[str, int] = {}
        self._last_mtime = 0
        self._lock = threading.Lock()

    # ------------------------------------------------------------------ #
    # SessionStore protocol
    # ------------------------------------------------------------------ #

    async def append(
        self,
        key: SessionKey,
        entries: List[SessionStoreEntry],
    ) -> None:
        """Append transcript entries and persist a new snapshot version."""
        if not entries:
            return
        ch = channel_for_session(
            key["project_key"], key["session_id"], key.get("subpath")
        )
        snapshot = await self._load_snapshot(ch)
        if snapshot is None:
            snapshot = {"entries": [], "mtime": 0, "subkeys": []}
        mtime = self._next_mtime()
        snapshot["entries"] = list(snapshot["entries"]) + list(entries)
        snapshot["mtime"] = mtime

        await self.actae.transition(
            ch,
            _EVENT_APPEND,
            {
                "session_id": key["session_id"],
                "subpath": key.get("subpath"),
                "count": len(entries),
                "last_type": entries[-1].get("type"),
            },
            self._checkpoint_state(ch, snapshot),
            actor=_ACTOR,
        )
        if key.get("subpath") is not None:
            await self._register_subkey(
                key["project_key"], key["session_id"], key["subpath"]
            )
        else:
            await self._update_project_index(key["project_key"], key["session_id"], mtime, entries)

    async def load(
        self,
        key: SessionKey,
    ) -> Optional[List[SessionStoreEntry]]:
        """Return all transcript entries for a key, or None if absent."""
        ch = channel_for_session(
            key["project_key"], key["session_id"], key.get("subpath")
        )
        snapshot = await self._load_snapshot(ch)
        if snapshot is None:
            return None
        return list(snapshot["entries"])

    async def fork_session(
        self,
        project_key: str,
        session_id: str,
        new_session_id: str,
        *,
        subpath: Optional[str] = None,
        reason: Optional[str] = None,
    ) -> SessionKey:
        """Fork a Claude session's transcript into a new session.

        The fork copies the full conversation transcript (all entries) into a
        new session whose channel is derived from ``new_session_id``, and
        registers it in the project index so ``resume=True`` on the new
        session continues from the inherited transcript.

        This is the Claude-SDK counterpart of ``AgentSession.resume(
        fork_at_step=...)`` / ``ActaeCheckpointSaver.fork_thread``: fork the
        conversation, then refine a later turn against the inherited context
        without re-running the earlier turns.

        Args:
            project_key: Claude project key (directory-relative project name).
            session_id: Source session to fork.
            new_session_id: ID of the new (fork) session.
            subpath: Optional subkey to fork instead of the main transcript.
            reason: Optional human-readable reason recorded on the fork.

        Returns:
            The new session's key, ready for ``query(..., session_store=...,
            resume=True)``.

        Raises:
            ValueError: If the source session has no transcript.
        """
        src_ch = channel_for_session(project_key, session_id, subpath)
        snapshot = await self._load_snapshot(src_ch)
        if snapshot is None:
            raise ValueError(
                f"Claude session '{session_id}' has no transcript to fork"
            )
        # Copy the transcript snapshot into the fork's channel (the server's
        # fork copies the latest state at-or-before the cursor — the full
        # transcript).
        dst_ch = channel_for_session(project_key, new_session_id, subpath)
        cursor = await self.actae.latest_cursor(src_ch) or 0
        await self.actae.fork(
            src_ch,
            dst_ch,
            cursor,
            display_name=new_session_id,
            reason=reason or f"Forked Claude session {session_id} → {new_session_id}",
        )

        # Register the new session in the project index (and subkey registry
        # for main transcripts) so resume/list find it.
        mtime = self._next_mtime()
        entries = list(snapshot["entries"])
        if subpath is not None:
            # Register the subkey on the fork's main channel so list_subkeys
            # and the delete cascade see it.
            main_ch = channel_for_session(project_key, new_session_id)
            main_snapshot = await self._load_snapshot(main_ch)
            if main_snapshot is None:
                main_snapshot = {"entries": [], "mtime": 0, "subkeys": []}
            if subpath not in main_snapshot["subkeys"]:
                main_snapshot["subkeys"].append(subpath)
                cursor = await self.actae.latest_cursor(main_ch) or 0
                await self.actae.save_state(
                    main_ch, cursor, self._checkpoint_state(main_ch, main_snapshot, cursor)
                )
        else:
            await self._update_project_index(project_key, new_session_id, mtime, entries)
        return {
            "project_key": project_key,
            "session_id": new_session_id,
            "subpath": subpath,
        }

    async def delete(self, key: SessionKey) -> None:
        """Delete a transcript.

        Deleting a main transcript cascades to its subkey transcripts
        (subagent transcripts, metadata) so nothing is orphaned.
        """
        main_ch = channel_for_session(key["project_key"], key["session_id"])
        snapshot = await self._load_snapshot(main_ch)
        subkeys = list(snapshot["subkeys"]) if snapshot else []

        ch = channel_for_session(
            key["project_key"], key["session_id"], key.get("subpath")
        )
        if key.get("subpath") is None:
            for sub in subkeys:
                sub_ch = channel_for_session(key["project_key"], key["session_id"], sub)
                cursor = await self.actae.latest_cursor(sub_ch) or 0
                await self.actae.transition(
                    sub_ch,
                    _EVENT_DELETE,
                    {"session_id": key["session_id"], "subpath": sub},
                    self._checkpoint_state(
                        sub_ch, {"entries": [], "mtime": 0, "subkeys": []}, cursor
                    ),
                    actor=_ACTOR,
                )
            await self._update_project_index_remove(key["project_key"], key["session_id"])
        else:
            main_ch = channel_for_session(key["project_key"], key["session_id"])
            main_snapshot = await self._load_snapshot(main_ch)
            if main_snapshot is not None and key["subpath"] in main_snapshot["subkeys"]:
                main_snapshot["subkeys"].remove(key["subpath"])
                cursor = await self.actae.latest_cursor(main_ch) or 0
                await self.actae.save_state(
                    main_ch, cursor, self._checkpoint_state(main_ch, main_snapshot, cursor)
                )

        cursor = await self.actae.latest_cursor(ch) or 0
        await self.actae.transition(
            ch,
            _EVENT_DELETE,
            {"session_id": key["session_id"], "subpath": key.get("subpath")},
            self._checkpoint_state(
                ch, {"entries": [], "mtime": 0, "subkeys": []}, cursor
            ),
            actor=_ACTOR,
        )

    async def list_sessions(self, project_key: str) -> List[SessionStoreListEntry]:
        """List main transcripts for a project with their storage mtimes."""
        index = await self._load_index(project_key)
        return [
            {"session_id": sid, "mtime": meta["mtime"]}
            for sid, meta in index["sessions"].items()
        ]

    async def list_subkeys(self, key: SessionListSubkeysKey) -> List[str]:
        """List subkey paths (subagent transcripts) for a session."""
        ch = channel_for_session(key["project_key"], key["session_id"])
        snapshot = await self._load_snapshot(ch)
        if snapshot is None:
            return []
        return list(snapshot["subkeys"])

    async def list_session_summaries(
        self, project_key: str
    ) -> List[SessionSummaryEntry]:
        """Return SDK-owned summary sidecars for a project, verbatim."""
        index = await self._load_index(project_key)
        return [
            {"session_id": sid, "mtime": meta["mtime"], "data": meta["data"]}
            for sid, meta in index["summaries"].items()
        ]

    # ------------------------------------------------------------------ #
    # Internals
    # ------------------------------------------------------------------ #

    def _next_mtime(self) -> int:
        now_ms = int(time.time() * 1000)
        with self._lock:
            if now_ms <= self._last_mtime:
                now_ms = self._last_mtime + 1
            self._last_mtime = now_ms
        return now_ms

    def _checkpoint_state(
        self, channel: str, state: Dict[str, Any], cursor: Optional[int] = None
    ) -> Dict[str, Any]:
        """Preserve the SessionStore schema and add the shared Actae contract."""

        portable: Dict[str, Any]
        if "entries" in state:
            portable = {
                "entry_count": len(state.get("entries", [])),
                "mtime": state.get("mtime", 0),
                "subkeys": list(state.get("subkeys", [])),
            }
        else:
            portable = {
                "session_count": len(state.get("sessions", {})),
                "summary_count": len(state.get("summaries", {})),
            }
        return adapter_checkpoint_state(
            state,
            framework="claude-agent-sdk",
            channel_id=channel,
            portable_state=portable,
            native_checkpoint={"storage": "session_store"},
            event_cursor=cursor,
        )

    async def _load_snapshot(self, channel: str) -> Optional[Dict[str, Any]]:
        snapshot = await self.actae.latest_state(channel)
        if snapshot is None:
            return None
        return snapshot["state"]

    async def _load_index(self, project_key: str) -> Dict[str, Any]:
        index = await self._load_snapshot(_channel_for_index(project_key))
        if index is None:
            return {"sessions": {}, "summaries": {}}
        return {
            "sessions": dict(index.get("sessions", {})),
            "summaries": dict(index.get("summaries", {})),
        }

    async def _save_index(self, project_key: str, index: Dict[str, Any]) -> None:
        ch = _channel_for_index(project_key)
        cursor = await self.actae.latest_cursor(ch) or 0
        await self.actae.save_state(ch, cursor, self._checkpoint_state(ch, index, cursor))

    async def _update_project_index(
        self,
        project_key: str,
        session_id: str,
        mtime: int,
        entries: List[SessionStoreEntry],
    ) -> None:
        index = await self._load_index(project_key)
        index["sessions"][session_id] = {"mtime": mtime}
        prev = index["summaries"].get(session_id)
        folded = fold_session_summary(
            prev,
            {"project_key": project_key, "session_id": session_id, "subpath": None},
            entries,
        )
        folded["mtime"] = mtime
        index["summaries"][session_id] = folded
        await self._save_index(project_key, index)

    async def _register_subkey(
        self, project_key: str, session_id: str, subpath: str
    ) -> None:
        """Record a subkey path on the main transcript so ``list_subkeys``
        and the delete cascade can find it."""
        main_ch = channel_for_session(project_key, session_id)
        snapshot = await self._load_snapshot(main_ch)
        if snapshot is None:
            snapshot = {"entries": [], "mtime": 0, "subkeys": []}
        if subpath not in snapshot["subkeys"]:
            snapshot["subkeys"].append(subpath)
            cursor = await self.actae.latest_cursor(main_ch) or 0
            await self.actae.save_state(
                main_ch, cursor, self._checkpoint_state(main_ch, snapshot, cursor)
            )

    async def _update_project_index_remove(
        self, project_key: str, session_id: str
    ) -> None:
        index = await self._load_index(project_key)
        index["sessions"].pop(session_id, None)
        index["summaries"].pop(session_id, None)
        await self._save_index(project_key, index)


class ActaeClaudeHook:
    """Claude Agent SDK lifecycle hooks that mirror conversation activity
    to Actae as real-time events.

    This is an event-forwarding adapter (not a persistence layer): it
    subscribes ``PreToolUse`` / ``PostToolUse`` / ``PostToolUseFailure`` /
    ``UserPromptSubmit`` / ``Stop`` hooks and records each occurrence as an
    ``claude.hook.<event>`` event on a per-session channel. Use it when you
    want live dashboards/replay of Claude activity; use
    :class:`ActaeClaudeSessionStore` for durable, resumable transcripts.

    Usage:
        from actae_client.adapters.claude import ActaeClaudeHook
        from claude_agent_sdk import ClaudeAgentOptions, query

        hook = ActaeClaudeHook(actae_client)
        async for message in query(
            prompt="Hello",
            options=ClaudeAgentOptions(hooks=hook.hooks),
        ):
            ...
    """

    def __init__(self, actae: ActaeClient, channel_prefix: str = "claude-hooks"):
        """Create a Claude hooks event forwarder.

        Args:
            actae: Connected ActaeClient instance.
            channel_prefix: Prefix for hook event channels (default
                ``"claude-hooks"``).
        """
        self.actae = actae
        self.channel_prefix = channel_prefix
        self._hook_event_names = [
            "PreToolUse",
            "PostToolUse",
            "PostToolUseFailure",
            "UserPromptSubmit",
            "Stop",
        ]
        self._hooks: Optional[Dict[str, List[Any]]] = None

    def _channel(self, session_id: str) -> str:
        return f"{self.channel_prefix}:{_digest(session_id)}"

    async def __call__(self, input_data: Any, tool_use_id: Any = None, context: Any = None) -> Dict[str, Any]:
        """Hook callback: record the event and return ``{}`` (no-op control)."""
        event_name = getattr(input_data, "get", lambda k, d=None: d)("hook_event_name")
        session_id = getattr(input_data, "get", lambda k, d=None: d)("session_id") or "unknown"
        tool_name = getattr(input_data, "get", lambda k, d=None: d)("tool_name")
        payload: Dict[str, Any] = {}
        if tool_use_id is not None:
            payload["tool_use_id"] = tool_use_id
        if tool_name is not None:
            payload["tool_name"] = tool_name
        if event_name in ("PreToolUse", "PostToolUse"):
            payload["tool_input"] = input_data.get("tool_input")
        if event_name == "PostToolUse":
            payload["tool_response"] = _trim(input_data.get("tool_response"))
        if event_name == "PostToolUseFailure":
            payload["error"] = input_data.get("error")
        if event_name == "UserPromptSubmit":
            payload["prompt"] = _trim(input_data.get("prompt"))
        if event_name == "Stop":
            payload["stop_reason"] = input_data.get("stop_reason")

        try:
            await self.actae.record(
                self._channel(session_id),
                f"claude.hook.{event_name.lower()}" if event_name else "claude.hook",
                payload,
                actor="claude-sdk",
                metadata={"session_id": session_id},
            )
        except Exception:  # hooks must never break the agent run
            logger.exception("failed to record claude hook event")
        return {}

    @property
    def hooks(self) -> Dict[str, List[Any]]:
        """Build the ``hooks=`` dict for ``ClaudeAgentOptions``.

        Requires ``claude-agent-sdk`` to be installed.
        """
        if self._hooks is None:
            try:
                from claude_agent_sdk import HookMatcher
            except ImportError as e:  # pragma: no cover
                raise ImportError(
                    "ActaeClaudeHook.hooks requires claude-agent-sdk; "
                    "install with `pip install 'actae-client[claude]'`"
                ) from e
            self._hooks = {
                name: [HookMatcher(matcher=None, hooks=[self])]
                for name in self._hook_event_names
            }
        return self._hooks


def _trim(value: Any, limit: int = 2000) -> Any:
    """Truncate long string payloads to keep events compact."""
    if isinstance(value, str):
        return value[:limit]
    if isinstance(value, dict):
        return {k: _trim(v, limit) for k, v in value.items()}
    return value
