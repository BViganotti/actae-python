"""
LangGraph Checkpointer Adapter for Actae.

Implements LangGraph's ``BaseCheckpointSaver`` protocol using Actae as the
backend. Every checkpoint is stored as a cursor-aligned, immutable state
snapshot in Actae, enabling deterministic resume with full context.

Usage:
    from actae_client.adapters.langgraph import ActaeCheckpointSaver

    saver = ActaeCheckpointSaver(actae_client)
    graph = builder.compile(checkpointer=saver)

    # async execution (recommended)
    result = await graph.ainvoke(input, {"configurable": {"thread_id": "1"}})

    # sync execution (fresh event loop; errors inside a running loop)
    result = graph.invoke(input, {"configurable": {"thread_id": "1"}})

On restart:
    config = {"configurable": {"thread_id": "1"}}
    previous = graph.get_state(config)   # loads from Actae
    graph.invoke(None, config)           # resumes from last checkpoint

Fork & resume (refine a later step without re-running earlier ones):
    fork_cfg = await saver.fork_thread(
        {"configurable": {"thread_id": "1"}},
        new_thread_id="1-fix", reason="refine node 5",
    )
    result = await graph.ainvoke(None, fork_cfg)
    # graph's nodes re-run from the fork point with the full LLM message
    # context preserved. `fork_thread` forks at an explicit checkpoint_id when
    # one is given (intermediate step), else the latest checkpoint. The sync
    # counterpart is `fork_thread_sync` for `graph.invoke` users.

Design notes
------------
* LangGraph state is treated as opaque. Checkpoints, metadata and pending
  writes are serialized losslessly with LangGraph's own serde
  (``dumps_typed`` / ``loads_typed``) and stored as base64 envelopes.
* Channel resolution is deterministic: the configured ``channel`` acts as a
  prefix and a bounded SHA-256 identity segment (derived from ``thread_id``
  and ``checkpoint_ns``) is appended. No user/workflow inference.
* Every snapshot is aligned to a real Actae event cursor: the checkpointer
  records a compact ``langgraph.checkpoint`` or ``langgraph.pending_writes``
  event and persists the snapshot with the exact cursor returned for it.
* ``aput_writes`` merges pending writes idempotently per (task_id, idx) and
  writes a new immutable snapshot version of the same checkpoint, which
  restores interrupted graph steps after a crash. The runtime submits the
  checkpoint save on a background executor, so ``put_writes`` can arrive
  before the checkpoint it targets is persisted; such writes are buffered
  in memory and flushed by ``aput`` as soon as the checkpoint lands.

Migration: data written by the pre-1.1.0 fixed-channel adapter is retained
in Actae for audit but is not read or migrated by this implementation.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import logging
import random
import threading
import warnings
from typing import (
    Any,
    AsyncIterator,
    Callable,
    Dict,
    Iterator,
    List,
    Optional,
    Sequence,
    Tuple,
)

from ..client import ActaeClient
# Defined in actae_client.errors so they are importable from the top-level
# package without installing LangGraph; this module re-exports them for
# back-compat (from actae_client.adapters.langgraph import ActaeLangGraphError).
from ..errors import ActaeLangGraphError, ActaeLangGraphSyncError
from .contract import adapter_checkpoint_state

try:
    from langgraph.checkpoint.base import (
        BaseCheckpointSaver,
        Checkpoint,
        CheckpointMetadata,
        CheckpointTuple,
        get_checkpoint_id,
    )
    from langgraph.errors import EmptyChannelError
    from langchain_core.runnables import RunnableConfig

    _HAS_LANGGRAPH = True
except ImportError:  # pragma: no cover - exercised in a subprocess test
    BaseCheckpointSaver = object  # type: ignore[misc,assignment]
    Checkpoint = dict  # type: ignore[misc,assignment]
    CheckpointMetadata = dict  # type: ignore[misc,assignment]
    CheckpointTuple = tuple  # type: ignore[misc,assignment]
    EmptyChannelError = Exception  # type: ignore[misc,assignment]
    RunnableConfig = dict  # type: ignore[misc,assignment]
    _HAS_LANGGRAPH = False

_ENVELOPE_VERSION = 1
_IDENTITY_DIGEST_BYTES = 16  # 64-bit collision resistance; bounded channel length
_PAGE_SIZE = 100
_MAX_DEFERRED_WRITES = 256
_EVENT_ACTOR = "langgraph-checkpointer"
_EVENT_CHECKPOINT = "langgraph.checkpoint"
_EVENT_PENDING_WRITES = "langgraph.pending_writes"
_EMPTY_TYPE = "__empty__"

logger = logging.getLogger(__name__)

_DEPRECATION_MSG = (
    "ActaeCheckpointSaver.save_checkpoint/get_checkpoint are deprecated; "
    "use the BaseCheckpointSaver protocol methods (aput/aget_tuple) instead."
)


def _default_channel_for(
    channel_prefix: str, thread_id: str, checkpoint_ns: str
) -> str:
    """Deterministic, collision-free channel for a (thread, namespace) pair."""
    identity = f"{thread_id}\x00{checkpoint_ns}".encode("utf-8")
    digest = hashlib.sha256(identity).hexdigest()[:_IDENTITY_DIGEST_BYTES]
    return f"{channel_prefix}:{digest}"


def _require_thread_id(config: Any) -> str:
    if not isinstance(config, dict):
        raise ActaeLangGraphError(
            f"config must be a dict, got {type(config).__name__}"
        )
    configurable = config.get("configurable") or {}
    thread_id = configurable.get("thread_id")
    if not thread_id:
        raise ActaeLangGraphError(
            "config['configurable']['thread_id'] is required and must be a "
            "non-empty string; Actae does not infer users, workflows "
            "or conversations"
        )
    return str(thread_id)


def _checkpoint_ns(config: Any) -> str:
    configurable = config.get("configurable") or {}
    ns = configurable.get("checkpoint_ns") or ""
    return str(ns)


def channel_for_config(
    config: dict,
    *,
    channel: str = "langgraph",
    channel_resolver: Optional[Callable[[dict], str]] = None,
) -> str:
    """Resolve the Actae channel used to store checkpoints for ``config``.

    The channel is the ``channel`` prefix plus a bounded SHA-256 segment
    derived from ``config["configurable"]["thread_id"]`` and the optional
    ``checkpoint_ns``. When ``channel_resolver`` is provided it is called
    with the full config and must return a non-empty string deterministically.

    Raises ``ActaeLangGraphError`` when ``thread_id`` is missing or empty.
    """
    thread_id = _require_thread_id(config)
    if channel_resolver is not None:
        resolved = channel_resolver(config)
        if not isinstance(resolved, str) or not resolved:
            raise ActaeLangGraphError(
                "channel_resolver must return a non-empty string"
            )
        return resolved
    return _default_channel_for(channel, thread_id, _checkpoint_ns(config))


def _pack(serde: Any, value: Any) -> Dict[str, str]:
    """Serialize a value into a base64 envelope entry via LangGraph serde."""
    if isinstance(value, EmptyChannelError):
        return {"t": _EMPTY_TYPE, "b": ""}
    type_tag, blob = serde.dumps_typed(value)
    return {"t": type_tag, "b": base64.b64encode(blob).decode("ascii")}


def _unpack(serde: Any, entry: Dict[str, str]) -> Any:
    if not isinstance(entry, dict):
        raise ActaeLangGraphError(f"corrupt envelope entry: {entry!r}")
    if entry.get("t") == _EMPTY_TYPE:
        return EmptyChannelError()
    blob = base64.b64decode(entry.get("b", ""))
    return serde.loads_typed((entry["t"], blob))


class _NamespaceLocks:
    """Per-(thread, namespace) ``asyncio.Lock`` registry.

    Locks bind to the event loop they are first awaited on; a stale lock
    from a dead loop is replaced on demand. Sync bridge calls are serialized
    by a separate threading lock inside ``ActaeCheckpointSaver``.
    """

    def __init__(self) -> None:
        self._locks: Dict[Tuple[str, str], asyncio.Lock] = {}
        self._guard = threading.Lock()

    def acquire(self, thread_id: str, checkpoint_ns: str) -> asyncio.Lock:
        key = (thread_id, checkpoint_ns)
        loop = asyncio.get_running_loop()
        with self._guard:
            lock = self._locks.get(key)
            if lock is None:
                lock = asyncio.Lock()
                self._locks[key] = lock
                return lock
            bound = getattr(lock, "_loop", None)
            if bound is not None and bound is not loop:
                lock = asyncio.Lock()
                self._locks[key] = lock
            return lock


if _HAS_LANGGRAPH:

    class ActaeCheckpointSaver(BaseCheckpointSaver[str]):  # type: ignore[no-redef]
        """LangGraph ``BaseCheckpointSaver`` backed by Actae state snapshots.

        Implements the full 1.x protocol: ``get_tuple``, ``list``, ``put``,
        ``put_writes`` plus the async ``aget_tuple``, ``alist``, ``aput``,
        ``aput_writes`` and ``delete_thread`` / ``adelete_thread``.

        Args:
            actae: Connected ``ActaeClient`` instance.
            channel: Channel prefix for checkpoint storage. The effective
                channel is ``f"{channel}:{sha256(thread_id\\x00ns)[:16]}"``.
                Defaults to ``"langgraph"``.
            channel_resolver: Optional callable ``(config) -> str`` returning
                the full Actae channel for a config. When provided it replaces
                the default prefix+digest resolution and must be
                deterministic for the same (thread_id, checkpoint_ns).
            serde: Optional LangGraph ``SerializerProtocol``. Defaults to the
                ``JsonPlusSerializer`` used by LangGraph itself.

        Raises:
            ImportError: If ``langgraph`` is not installed (install with
                ``pip install actae-client[langgraph]``).
        """

        def __init__(
            self,
            actae: ActaeClient,
            channel: str = "langgraph",
            *,
            channel_resolver: Optional[Callable[[dict], str]] = None,
            serde: Any = None,
        ) -> None:
            if not channel or not isinstance(channel, str):
                raise ValueError("channel must be a non-empty string")
            required = (
                "record",
                "save_state",
                "latest_state",
                "list_states",
                "get_state",
            )
            missing = [m for m in required if not callable(getattr(actae, m, None))]
            if missing:
                raise TypeError(
                    "actae must implement the Actae state/event API "
                    f"(missing: {', '.join(missing)})"
                )
            super().__init__(serde=serde)
            self.actae = actae
            self.channel = channel
            self.channel_resolver = channel_resolver
            self._locks = _NamespaceLocks()
            self._sync_guard = threading.Lock()
            self._thread_namespaces: Dict[str, set] = {}
            # Pending writes for checkpoints the runtime has not persisted yet.
            # LangGraph submits aput before put_writes but runs the checkpoint
            # save on a background executor; put_writes can therefore arrive
            # first. Writes are buffered here and flushed by aput once the
            # checkpoint is on disk (mirrors InMemorySaver's decoupled store).
            self._deferred_writes: Dict[
                Tuple[str, str, str], Dict[Tuple[str, int], Dict[str, Any]]
            ] = {}

        # ------------------------------------------------------------------
        # Config
        # ------------------------------------------------------------------

        @property
        def config_specs(self) -> list:
            return [
                {"id": "thread_id", "scope": "checkpoint", "default": ""},
                {"id": "checkpoint_ns", "scope": "checkpoint", "default": "default"},
                {"id": "checkpoint_id", "scope": "checkpoint", "default": ""},
            ]

        def get_next_version(self, current: Optional[str], channel: Any) -> str:
            """Monotonic string channel versions (matches InMemorySaver)."""
            if current is None:
                current_v = 0
            elif isinstance(current, int):
                current_v = current
            else:
                current_v = int(current.split(".")[0])
            next_v = current_v + 1
            next_h = random.random()
            return f"{next_v:032}.{next_h:016}"

        # ------------------------------------------------------------------
        # Channel resolution
        # ------------------------------------------------------------------

        def channel_for_config(self, config: dict) -> str:
            """Actae channel for ``config`` (see module-level helper)."""
            return channel_for_config(
                config,
                channel=self.channel,
                channel_resolver=self.channel_resolver,
            )

        def _record_namespace(self, thread_id: str, checkpoint_ns: str) -> None:
            self._thread_namespaces.setdefault(thread_id, set()).add(checkpoint_ns)

        # ------------------------------------------------------------------
        # Serialization helpers
        # ------------------------------------------------------------------

        def _pack(self, value: Any) -> Dict[str, str]:
            return _pack(self.serde, value)

        def _unpack(self, entry: Dict[str, str]) -> Any:
            return _unpack(self.serde, entry)

        # ------------------------------------------------------------------
        # Snapshot persistence (event cursor aligned)
        # ------------------------------------------------------------------

        async def _save_envelope(
            self, channel: str, envelope: Dict[str, Any], event_type: str
        ) -> None:
            """Record a compact event, then persist the snapshot at its cursor."""
            event = await self.actae.record(
                channel,
                event_type,
                {"checkpoint_id": envelope.get("checkpoint_id")},
                actor=_EVENT_ACTOR,
            )
            envelope["event_cursor"] = event.cursor
            envelope["event_id"] = event.id
            state = adapter_checkpoint_state(
                envelope,
                framework="langgraph",
                channel_id=channel,
                portable_state={
                    "thread_id": envelope.get("thread_id"),
                    "checkpoint_ns": envelope.get("checkpoint_ns"),
                },
                native_checkpoint={"checkpoint_id": envelope.get("checkpoint_id")},
                pending_work={"count": len(envelope.get("pending_writes", []))},
                event_cursor=event.cursor,
                event_id=event.id,
            )
            await self.actae.save_state(channel, event.cursor, state)

        async def _latest_envelope(self, channel: str) -> Optional[Dict[str, Any]]:
            snapshot = await self.actae.latest_state(channel)
            if snapshot is None:
                return None
            envelope = snapshot["state"]
            self._validate_envelope(envelope)
            return envelope

        async def _find_envelope(
            self, channel: str, checkpoint_id: str
        ) -> Optional[Dict[str, Any]]:
            """Locate the newest snapshot version for a checkpoint ID.

            Scans state-version history newest-first in bounded pages; the
            first match is the latest version of that checkpoint (it contains
            the most merged pending writes).
            """
            offset = 0
            while True:
                versions = await self.actae.list_states(
                    channel, limit=_PAGE_SIZE, offset=offset
                )
                if not versions:
                    return None
                for entry in versions:
                    snapshot = await self.actae.get_state(channel, entry["version"])
                    if snapshot is None:
                        continue
                    envelope = snapshot["state"]
                    self._validate_envelope(envelope)
                    if envelope.get("checkpoint_id") == checkpoint_id:
                        return envelope
                offset += len(versions)
                if len(versions) < _PAGE_SIZE:
                    return None

        def _validate_envelope(self, envelope: Any) -> None:
            if not isinstance(envelope, dict):
                raise ActaeLangGraphError(
                    f"corrupt checkpoint snapshot on channel: {envelope!r}"
                )
            if envelope.get("v") != _ENVELOPE_VERSION:
                raise ActaeLangGraphError(
                    "checkpoint snapshot format version mismatch: found "
                    f"{envelope.get('v')!r}, expected {_ENVELOPE_VERSION}; "
                    "the data was written by an incompatible SDK version"
                )

        def _new_envelope(
            self,
            thread_id: str,
            checkpoint_ns: str,
            checkpoint_id: Optional[str],
            parent_checkpoint_id: Optional[str],
            checkpoint: Optional[Any],
            metadata: Optional[Any],
            pending_writes: Optional[List[Dict[str, Any]]],
        ) -> Dict[str, Any]:
            envelope: Dict[str, Any] = {
                "v": _ENVELOPE_VERSION,
                "thread_id": thread_id,
                "checkpoint_ns": checkpoint_ns,
                "checkpoint_id": checkpoint_id,
                "parent_checkpoint_id": parent_checkpoint_id,
                "pending_writes": pending_writes or [],
                "event_cursor": None,
                "event_id": None,
            }
            if checkpoint is not None:
                envelope["checkpoint"] = self._pack(checkpoint)
            if metadata is not None:
                envelope["metadata"] = self._pack(metadata)
            return envelope

        def _tuple_from_envelope(
            self, config: dict, envelope: Dict[str, Any]
        ) -> Optional[CheckpointTuple]:
            if not envelope.get("checkpoint"):
                return None
            # The requested config is authoritative after a server-side fork:
            # inherited snapshot bytes still contain the source thread ID.
            effective_thread_id = _require_thread_id(config)
            effective_namespace = _checkpoint_ns(config)
            checkpoint = self._unpack(envelope["checkpoint"])
            metadata = (
                self._unpack(envelope["metadata"]) if envelope.get("metadata") else {}
            )
            pending_writes: Optional[List[Tuple[str, str, Any]]] = None
            raw_writes = envelope.get("pending_writes") or []
            if raw_writes:
                pending_writes = [
                    (
                        str(w["task_id"]),
                        str(w["channel"]),
                        self._unpack(w),
                    )
                    for w in sorted(raw_writes, key=lambda w: (w["task_id"], w["idx"]))
                ]
            parent_checkpoint_id = envelope.get("parent_checkpoint_id")
            parent_config = None
            if parent_checkpoint_id:
                parent_config = {
                    "configurable": {
                        "thread_id": effective_thread_id,
                        "checkpoint_ns": effective_namespace,
                        "checkpoint_id": parent_checkpoint_id,
                    }
                }
            tuple_config = {
                **config,
                "configurable": {
                    **config.get("configurable", {}),
                    "thread_id": effective_thread_id,
                    "checkpoint_ns": effective_namespace,
                    "checkpoint_id": envelope["checkpoint_id"],
                },
            }
            return CheckpointTuple(
                config=tuple_config,
                checkpoint=checkpoint,
                metadata=metadata,
                parent_config=parent_config,
                pending_writes=pending_writes,
            )

        # ------------------------------------------------------------------
        # Writes — async
        # ------------------------------------------------------------------

        async def aput(
            self,
            config: RunnableConfig,
            checkpoint: Checkpoint,
            metadata: CheckpointMetadata,
            new_versions: dict,
        ) -> RunnableConfig:
            """Store a checkpoint as a cursor-aligned Actae snapshot.

            ``new_versions`` is intentionally ignored: LangGraph state is
            treated as opaque and stored whole inside the snapshot envelope.
            """
            thread_id = _require_thread_id(config)
            checkpoint_ns = _checkpoint_ns(config)
            self._record_namespace(thread_id, checkpoint_ns)
            channel = self.channel_for_config(config)
            checkpoint_id = checkpoint["id"]
            parent_checkpoint_id = get_checkpoint_id(config)

            envelope = self._new_envelope(
                thread_id=thread_id,
                checkpoint_ns=checkpoint_ns,
                checkpoint_id=checkpoint_id,
                parent_checkpoint_id=parent_checkpoint_id,
                checkpoint=checkpoint,
                metadata=metadata,
                pending_writes=None,
            )
            async with self._locks.acquire(thread_id, checkpoint_ns):
                await self._save_envelope(channel, envelope, _EVENT_CHECKPOINT)
                # Flush any writes the runtime submitted before this save
                # landed (aput and put_writes race on a background executor).
                await self._flush_deferred_writes(
                    channel, thread_id, checkpoint_ns, checkpoint_id, envelope
                )

            return {
                "configurable": {
                    "thread_id": thread_id,
                    "checkpoint_ns": checkpoint_ns,
                    "checkpoint_id": checkpoint_id,
                }
            }

        async def _flush_deferred_writes(
            self,
            channel: str,
            thread_id: str,
            checkpoint_ns: str,
            checkpoint_id: str,
            envelope: Dict[str, Any],
        ) -> None:
            """Persist writes buffered for a checkpoint that was just saved.

            Called from ``aput`` right after the checkpoint snapshot lands;
            caller must hold the per-namespace lock.
            """
            key = (thread_id, checkpoint_ns, checkpoint_id)
            merged = self._deferred_writes.pop(key, None)
            if not merged:
                return
            logger.debug(
                "flushing %d deferred pending write(s) onto checkpoint %s",
                len(merged),
                checkpoint_id,
            )
            updated: Dict[str, Any] = {
                **envelope,
                "pending_writes": sorted(
                    merged.values(), key=lambda w: (w["task_id"], w["idx"])
                ),
            }
            await self._save_envelope(channel, updated, _EVENT_PENDING_WRITES)

        def _defer_writes(
            self,
            thread_id: str,
            checkpoint_ns: str,
            target_id: str,
            writes: Sequence[Tuple[str, Any]],
            task_id: str,
        ) -> None:
            """Buffer writes for a checkpoint not yet persisted.

            Caller must hold the sync guard (i.e. run inside ``_run_sync``).
            """
            key = (thread_id, checkpoint_ns, target_id)
            deferred = self._deferred_writes.setdefault(key, {})
            for idx, (channel_name, value) in enumerate(writes):
                deferred[(task_id, idx)] = {
                    "task_id": task_id,
                    "idx": idx,
                    "channel": channel_name,
                    **self._pack(value),
                }
            if len(self._deferred_writes) > _MAX_DEFERRED_WRITES:
                stale_key, _ = next(iter(self._deferred_writes.items()))
                del self._deferred_writes[stale_key]
                logger.warning(
                    "deferred pending writes buffer overflow; dropping writes "
                    "for %s",
                    stale_key,
                )

        async def aput_writes(
            self,
            config: RunnableConfig,
            writes: Sequence[Tuple[str, Any]],
            task_id: str,
            task_path: str = "",
        ) -> None:
            """Merge a task's pending writes into the target checkpoint.

            Writes are merged idempotently by ``(task_id, idx)`` — the last
            batch for a task wins, matching the runtime's in-memory
            semantics. A new immutable snapshot version of the same
            checkpoint is persisted, aligned to a ``langgraph.pending_writes``
            event cursor. This is what restores interrupted steps after a
            crash.
            """
            thread_id = _require_thread_id(config)
            checkpoint_ns = _checkpoint_ns(config)
            self._record_namespace(thread_id, checkpoint_ns)
            channel = self.channel_for_config(config)
            target_id = get_checkpoint_id(config)
            if not target_id:
                raise ActaeLangGraphError(
                    "aput_writes requires config['configurable']['checkpoint_id']"
                )

            async with self._locks.acquire(thread_id, checkpoint_ns):
                latest = await self._latest_envelope(channel)
                if latest is not None and latest.get("checkpoint_id") == target_id:
                    envelope = latest
                else:
                    envelope = await self._find_envelope(channel, target_id)
                    if envelope is None:
                        # The runtime persists the checkpoint right after
                        # flushing its writes; the save runs on a background
                        # executor and may not have landed yet. Buffer the
                        # writes and let aput attach them on save.
                        logger.debug(
                            "deferring %d pending write(s) for not-yet-saved "
                            "checkpoint %s on channel %s",
                            len(writes),
                            target_id,
                            channel,
                        )
                        self._defer_writes(
                            thread_id, checkpoint_ns, target_id, writes, task_id
                        )
                        return

                merged = {
                    (w["task_id"], w["idx"]): w
                    for w in envelope.get("pending_writes") or []
                }
                for idx, (channel_name, value) in enumerate(writes):
                    merged[(task_id, idx)] = {
                        "task_id": task_id,
                        "idx": idx,
                        "channel": channel_name,
                        **self._pack(value),
                    }
                updated: Dict[str, Any] = {
                    **envelope,
                    "pending_writes": sorted(
                        merged.values(), key=lambda w: (w["task_id"], w["idx"])
                    ),
                }
                await self._save_envelope(channel, updated, _EVENT_PENDING_WRITES)

        # ------------------------------------------------------------------
        # Reads — async
        # ------------------------------------------------------------------

        async def aget_tuple(
            self, config: RunnableConfig
        ) -> Optional[CheckpointTuple]:
            """Latest checkpoint for the thread, or the requested checkpoint ID."""
            _require_thread_id(config)  # validation: raises when thread_id missing
            channel = self.channel_for_config(config)
            checkpoint_id = get_checkpoint_id(config)

            if checkpoint_id is not None:
                envelope = await self._find_envelope(channel, checkpoint_id)
            else:
                envelope = await self._latest_envelope(channel)
            if envelope is None:
                return None
            return self._tuple_from_envelope(config, envelope)

        async def fork_thread(
            self,
            config: RunnableConfig,
            *,
            new_thread_id: str,
            reason: Optional[str] = None,
        ) -> RunnableConfig:
            """Fork a thread's checkpoint into a NEW thread's channel.

            The fork copies the latest checkpoint envelope (the full graph
            state — including the LLM message context) from this thread's
            channel into a fresh channel derived from ``new_thread_id``.
            Resume the fork by compiling a graph with this saver (or a saver
            whose ``channel_resolver`` returns the fork's channel) and running
            it with ``configurable.checkpoint_id`` set to the forked
            checkpoint.

            This is the ergonomic way to do the "fork a LangGraph run at step
            N, refine step N+1" workflow::

                saver = ActaeCheckpointSaver(actae, channel="lg")
                cfg = {"configurable": {"thread_id": "run-1"}}
                await graph.ainvoke(input, cfg)                 # full run
                fork_cfg = await saver.fork_thread(
                    cfg, new_thread_id="run-1-fix", reason="refine step N+1")
                fork_graph = build(saver=saver)                  # modified node
                res = await fork_graph.ainvoke(None, fork_cfg)   # resume from fork

            Returns the fork's config (with the forked ``checkpoint_id``),
            ready to pass to ``ainvoke(None, ...)``. The fork's state — and
            with it the LLM conversation context — is preserved exactly.
            """
            thread_id = _require_thread_id(config)
            checkpoint_ns = _checkpoint_ns(config)
            src_channel = self.channel_for_config(config)

            # Honor an explicit checkpoint_id (fork at an intermediate step);
            # default to the latest checkpoint.
            requested_id = get_checkpoint_id(config)
            if requested_id is not None:
                envelope = await self._find_envelope(src_channel, requested_id)
            else:
                envelope = await self._latest_envelope(src_channel)
            if envelope is None or not envelope.get("checkpoint"):
                raise ActaeLangGraphError(
                    f"thread '{thread_id}' has no checkpoint to fork"
                )
            checkpoint_id = envelope["checkpoint_id"]
            event_cursor = envelope.get("event_cursor")
            if not event_cursor:
                raise ActaeLangGraphError(
                    f"thread '{thread_id}' checkpoint {checkpoint_id} has no "
                    f"event cursor — cannot fork"
                )

            # Fork the source channel at the checkpoint's cursor into a fresh
            # channel derived from new_thread_id.
            fork_config = {
                "configurable": {
                    "thread_id": new_thread_id,
                    "checkpoint_ns": checkpoint_ns,
                    "checkpoint_id": checkpoint_id,
                }
            }
            fork_channel = self.channel_for_config(fork_config)
            await self.actae.fork(
                src_channel,
                fork_channel,
                event_cursor,
                display_name=new_thread_id,
                reason=reason or f"Forked thread {thread_id} at checkpoint {checkpoint_id[:8]}",
            )
            self._record_namespace(new_thread_id, checkpoint_ns)

            # Return a config that resumes from the forked checkpoint on the
            # fork's channel. The caller's graph must be built with a saver
            # that resolves this thread to the fork channel (this same saver
            # does, because new_thread_id hashes to fork_channel).
            return fork_config

        async def alist(
            self,
            config: Optional[RunnableConfig],
            *,
            filter: Optional[dict] = None,
            before: Optional[RunnableConfig] = None,
            limit: Optional[int] = None,
        ) -> AsyncIterator[CheckpointTuple]:
            """Checkpoints newest-first, honoring LangGraph semantics.

            Snapshot versions of the same checkpoint are deduplicated (the
            newest version wins, keeping the most merged pending writes).
            ``before`` is exclusive; ``filter`` requires every metadata key
            to match; ``limit`` bounds the result count.
            """
            if config is None:
                raise ActaeLangGraphError(
                    "list/alist requires a config with thread_id"
                )
            thread_id = _require_thread_id(config)
            checkpoint_ns = _checkpoint_ns(config)
            channel = self.channel_for_config(config)
            want_id = get_checkpoint_id(config)
            before_id = get_checkpoint_id(before) if before else None

            seen: Dict[str, Dict[str, Any]] = {}
            offset = 0
            while True:
                versions = await self.actae.list_states(
                    channel, limit=_PAGE_SIZE, offset=offset
                )
                if not versions:
                    break
                for entry in versions:
                    snapshot = await self.actae.get_state(channel, entry["version"])
                    if snapshot is None:
                        continue
                    envelope = snapshot["state"]
                    self._validate_envelope(envelope)
                    checkpoint_id = envelope.get("checkpoint_id")
                    if checkpoint_id is None or checkpoint_id in seen:
                        continue
                    if want_id is not None and checkpoint_id != want_id:
                        continue
                    seen[checkpoint_id] = envelope
                offset += len(versions)
                if len(versions) < _PAGE_SIZE:
                    break

            ordered = sorted(
                seen.values(), key=lambda e: e["checkpoint_id"], reverse=True
            )
            count = 0
            for envelope in ordered:
                if before_id is not None and envelope["checkpoint_id"] >= before_id:
                    continue
                metadata = (
                    self._unpack(envelope["metadata"])
                    if envelope.get("metadata")
                    else {}
                )
                if filter and not all(
                    metadata.get(key) == value for key, value in filter.items()
                ):
                    continue
                tuple_ = self._tuple_from_envelope(
                    {
                        "configurable": {
                            "thread_id": thread_id,
                            "checkpoint_ns": checkpoint_ns,
                            "checkpoint_id": envelope["checkpoint_id"],
                        }
                    },
                    envelope,
                )
                if tuple_ is not None:
                    yield tuple_
                    count += 1
                    if limit is not None and count >= limit:
                        break

        # ------------------------------------------------------------------
        # Delete — async
        # ------------------------------------------------------------------

        async def adelete_thread(self, thread_id: str) -> None:
            """Delete all checkpoints and writes for a thread.

            Deletes every channel for namespaces this process has written.
            Namespaces written by other processes are not discoverable
            through the Actae API and are left untouched.
            """
            thread_id = str(thread_id)
            for key in [k for k in self._deferred_writes if k[0] == thread_id]:
                del self._deferred_writes[key]
            namespaces = self._thread_namespaces.pop(thread_id, set()) or {""}
            for checkpoint_ns in namespaces:
                channel = self.channel_for_config({
                    "configurable": {
                        "thread_id": thread_id,
                        "checkpoint_ns": checkpoint_ns,
                    }
                })
                offset = 0
                while True:
                    versions = await self.actae.list_states(
                        channel, limit=_PAGE_SIZE, offset=offset
                    )
                    if not versions:
                        break
                    for entry in versions:
                        await self.actae.delete_state(channel, entry["version"])
                    offset += len(versions)
                    if len(versions) < _PAGE_SIZE:
                        break

        # ------------------------------------------------------------------
        # Writes — sync bridge
        # ------------------------------------------------------------------

        def _run_sync(self, coro_factory: Callable[[], Any]) -> Any:
            """Run an async implementation in a fresh loop, when safe."""
            try:
                asyncio.get_running_loop()
            except RuntimeError:
                pass
            else:
                raise ActaeLangGraphSyncError(
                    "sync LangGraph checkpointer methods cannot run inside an "
                    "active event loop; use graph.ainvoke / graph.astream "
                    "(async) instead, or call graph.invoke from a "
                    "non-async context"
                )

            async def _entry() -> Any:
                try:
                    return await coro_factory()
                finally:
                    # The fresh loop is about to close; release its session.
                    closer = getattr(self.actae, "_close_current_loop_session", None)
                    if closer is not None:
                        await closer()

            with self._sync_guard:
                return asyncio.run(_entry())

        def put(
            self,
            config: RunnableConfig,
            checkpoint: Checkpoint,
            metadata: CheckpointMetadata,
            new_versions: dict,
        ) -> RunnableConfig:
            """Sync bridge for ``aput`` (see ``ActaeLangGraphSyncError``)."""
            return self._run_sync(
                lambda: self.aput(config, checkpoint, metadata, new_versions)
            )

        def put_writes(
            self,
            config: RunnableConfig,
            writes: Sequence[Tuple[str, Any]],
            task_id: str,
            task_path: str = "",
        ) -> None:
            """Sync bridge for ``aput_writes``."""
            self._run_sync(lambda: self.aput_writes(config, writes, task_id, task_path))

        # ------------------------------------------------------------------
        # Reads — sync bridge
        # ------------------------------------------------------------------

        def get_tuple(self, config: RunnableConfig) -> Optional[CheckpointTuple]:
            """Sync bridge for ``aget_tuple``."""
            return self._run_sync(lambda: self.aget_tuple(config))

        def list(
            self,
            config: Optional[RunnableConfig],
            *,
            filter: Optional[dict] = None,
            before: Optional[RunnableConfig] = None,
            limit: Optional[int] = None,
        ) -> Iterator[CheckpointTuple]:
            """Sync bridge for ``alist`` (results are prefetched)."""
            results = self._run_sync(
                lambda: _collect_alist(
                    self.alist(config, filter=filter, before=before, limit=limit)
                )
            )
            return iter(results)

        def delete_thread(self, thread_id: str) -> None:
            """Sync bridge for ``adelete_thread``."""
            self._run_sync(lambda: self.adelete_thread(thread_id))

        def fork_thread_sync(
            self,
            config: RunnableConfig,
            *,
            new_thread_id: str,
            reason: Optional[str] = None,
        ) -> RunnableConfig:
            """Sync bridge for ``fork_thread`` (for ``graph.invoke`` users)."""
            return self._run_sync(
                lambda: self.fork_thread(
                    config, new_thread_id=new_thread_id, reason=reason
                )
            )

        # ------------------------------------------------------------------
        # Deprecated compatibility helpers
        # ------------------------------------------------------------------

        async def save_checkpoint(
            self, config: dict, checkpoint: Any, metadata: Any
        ) -> None:
            """Deprecated: use the ``BaseCheckpointSaver`` protocol instead."""
            warnings.warn(_DEPRECATION_MSG, DeprecationWarning, stacklevel=2)
            await self.aput(config, checkpoint, metadata, {})

        async def get_checkpoint(self, config: dict) -> Optional[dict]:
            """Deprecated: use ``aget_tuple`` instead."""
            warnings.warn(_DEPRECATION_MSG, DeprecationWarning, stacklevel=2)
            tuple_ = await self.aget_tuple(config)
            if tuple_ is None:
                return None
            return {
                "config": tuple_.config,
                "checkpoint": tuple_.checkpoint,
                "metadata": tuple_.metadata,
                "parent_config": tuple_.parent_config,
                "pending_writes": tuple_.pending_writes,
            }

else:

    class ActaeCheckpointSaver:  # type: ignore[no-redef]
        """Placeholder raised when ``langgraph`` is not installed.

        The module imports without the optional dependency; constructing the
        saver reports exactly how to install it.
        """

        def __init__(self, *args: Any, **kwargs: Any) -> None:
            raise ImportError(
                "ActaeCheckpointSaver requires langgraph. Install it with: "
                "pip install 'actae-client[langgraph]'"
            )


async def _collect_alist(alist: AsyncIterator[CheckpointTuple]) -> List[CheckpointTuple]:
    return [item async for item in alist]
