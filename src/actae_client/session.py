"""
AgentSession — high-level instrumentation for forking, resuming, and comparing
agent execution pipelines in Actae.

    from actae_client import ActaeClient
    from actae_client.session import AgentSession

    actae = ActaeClient(endpoint="http://localhost:8002", api_key="sk-dev-...")

    def my_state() -> dict:
        return {"memory": agent.memory, "accumulator": agent.results}

    async def main():
        #  First run
        async with AgentSession(actae, name="exp-v1", state_fn=my_state) as session:
            for i, item in enumerate(dataset):
                result = llm.call(item)
                await session.step("inference", input=item, output=result,
                                   metadata={"model": "gpt-4"})

        #  Fork at step 5 with different model — steps 1-5 are NOT re-run.
        session = await AgentSession.resume(
            actae, "exp-v1",
            fork_at_step=5, name="exp-gpt5",
            params={"model": "gpt-5"},
        )
        async with session:
            # The fork inherited steps 1-5's state; seed your agent from it.
            inherited = session.inherited_state or {}
            for item in dataset[5:]:
                result = llm.call(item, model="gpt-5", context=inherited)
                await session.step("inference", input=item, output=result,
                                   metadata={"model": "gpt-5"})

    asyncio.run(main())
"""

import asyncio
import hashlib
import inspect
import json
import logging
import math
import numbers
import platform
import uuid
from typing import Any, Callable, Dict, List, Optional, Protocol



class SessionTransport(Protocol):
    """The small direct/fleet boundary used by ``AgentSession``.

    Both ``ActaeClient`` and ``FleetSessionTransport`` satisfy this protocol;
    lifecycle, fork and deterministic-id logic remains in this one module.
    """
    async def record(self, channel_id: str, event_type: str, payload: Any, **kwargs: Any) -> "Event": ...
    async def replay(self, channel_id: str, **kwargs: Any) -> List["Event"]: ...
    async def save_state(self, channel_id: str, cursor: int, state: Any, **kwargs: Any) -> int: ...
    async def latest_state(self, channel_id: str) -> Optional[Dict[str, Any]]: ...
    async def fork(self, source_channel_id: str, new_channel_id: str, at_cursor: int = 0, **kwargs: Any) -> Any: ...
    async def get_channel_metadata(self, channel_id: str) -> Any: ...
    async def update_metadata(self, channel_id: str, **kwargs: Any) -> Any: ...
    async def resolve_step(self, channel_id: str, step_number: int) -> Any: ...
    async def latest_step_number(self, channel_id: str) -> Optional[int]: ...

from .types import Event
from .errors import (
    APIError,
    ActaeConnectionError,
    ExecutionNotOwnedError,
    NoRestorableCheckpointError,
    RateLimitError,
    SnapshotBoundaryError,
)

logger = logging.getLogger("actae_client.session")

try:
    from actae_client import __version__ as _SDK_VERSION  # type: ignore
except Exception:  # pragma: no cover
    _SDK_VERSION = "unknown"

SESSION_STARTED = "session.started"
SESSION_COMPLETED = "session.completed"

# Reserved execution-ledger key/tool for the optional per-channel session
# ownership fence (``fence_owner``). The ledger's atomic claim + lease +
# claim-token fencing is what makes a second live owner (or a stale one) get
# rejected rather than silently appending events.
SESSION_FENCE_KEY = "__session_lock__"
SESSION_FENCE_TOOL = "agent.session"

# Maximum number of cursors stored in channel metadata (for fork resolution).
# Beyond this, only the most recent cursors are persisted; older steps use the
# replay fallback path in _fork_from_channel.
_MAX_STORED_CURSORS = 1000

# Maximum fork-lineage depth walked when resolving an inherited step (mirrors
# the server's execution-tree depth cap).
MAX_LINEAGE_DEPTH = 10

# Retry configuration for transient failures (connection drops, timeouts, 5xx).
_MAX_RETRIES = 3
_RETRY_BASE_DELAY = 0.5   # seconds
_RETRY_MAX_DELAY = 5.0    # seconds

# Fixed UUIDv5 namespace shared with the Go SDK for deterministic
# operation ids (same byte layout as the Go SDK's actaeNamespace).
_STEP_ID_NAMESPACE = uuid.UUID("3f74e5f1-9b2c-4a7e-8f1d-000000000001")


def _go_float(v: float) -> str:
    """Serialize a float exactly like Go's `encoding/json` float encoder
    (shortest round-trip, `f` format for 1e-6 <= |v| < 1e21, `e` format
    otherwise with Go's exponent cleanup — no leading zeros, sign kept),
    verified empirically against Go for a 14-value matrix covering
    integer-valued floats, -0.0, exponent padding and large exponents.
    NaN/Infinity raise — Go's json.Marshal errors on them."""
    if math.isnan(v) or math.isinf(v):
        raise ValueError(
            "NaN/Infinity in step payload/metadata — Go json.Marshal "
            "rejects it; the record would fail server-side"
        )
    if v == 0.0:
        return "0"  # Go emits "0" for -0.0 too
    r = repr(v)
    if "e" in r:
        mant, exp = r.split("e")
        if 1e-6 <= abs(v) < 1e21:
            digits = mant.replace(".", "")
            e = int(exp)
            if e < 0:
                return "0." + "0" * (-e - 1) + digits
            return digits + "0" * e
        return mant + "e" + exp[0] + exp[1:].lstrip("0")
    if r.endswith(".0"):
        return r[:-2]
    return r


def _go_quote(s: str) -> str:
    """Quote a string like Go's json.Marshal: raw UTF-8, named escapes for
    control characters, plus Go's escaping of `<`, `>`, `&`, U+2028/U+2029."""
    return (
        json.dumps(s, ensure_ascii=False)
        .replace("<", "\\u003c")
        .replace(">", "\\u003e")
        .replace("&", "\\u0026")
        .replace("\u2028", "\\u2028")
        .replace("\u2029", "\\u2029")
    )


def _go_marshal_value(v: Any) -> str:
    """Serialize a value exactly like Go's json.Marshal, for the
    payload/metadata shapes the SDK builds (all keys recursively sorted,
    payload-first outer order). Any `numbers.Real`/`numbers.Integral` is
    accepted (covers numpy float/int scalars) and formatted Go-exactly."""
    if v is None:
        return "null"
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, numbers.Integral):
        return str(int(v))
    if isinstance(v, numbers.Real):
        return _go_float(float(v))
    if isinstance(v, str):
        return _go_quote(v)
    if isinstance(v, dict):
        return (
            "{"
            + ",".join(
                _go_quote(k) + ":" + _go_marshal_value(v[k])
                for k in sorted(v)
            )
            + "}"
        )
    if isinstance(v, (list, tuple)):
        return "[" + ",".join(_go_marshal_value(i) for i in v) + "]"
    raise TypeError(
        "Unsupported value of type %r in step payload/metadata — Go "
        "json.Marshal rejects it" % type(v).__name__
    )


def _canonical_step_content(payload: Dict[str, Any], metadata: Dict[str, Any]) -> str:
    """Byte-identical serialization to the Go SDK's canonicalStepContent:
    `{"payload":{...},"metadata":{...}}` — payload FIRST (Go struct field
    order, not alphabetical), all dict keys recursively sorted, floats
    formatted exactly like Go's float encoder, raw UTF-8 for non-ASCII,
    `<`, `>`, `&` and U+2028/U+2029 escaped."""
    return (
        '{"payload":'
        + _go_marshal_value(payload)
        + ',"metadata":'
        + _go_marshal_value(metadata)
        + "}"
    )
    return (
        text.replace("<", "\\u003c")
        .replace(">", "\\u003e")
        .replace("&", "\\u0026")
        .replace("\u2028", "\\u2028")
        .replace("\u2029", "\\u2029")
    )


def _step_operation_id(
    channel_id: str,
    step_type: str,
    step_num: int,
    payload: Dict[str, Any],
    metadata: Dict[str, Any],
) -> str:
    """Deterministic UUIDv5 operation id for an AgentSession step.

    Derived from (scope, step type, channel, step number, canonical
    payload+metadata) with NUL-separated parts and a 256-byte cap — the
    byte-identical scheme of the Go SDK's ``DeterministicOperationKey``
    (``_canonical_step_content`` mirrors Go's ``json.Marshal`` output). A
    retry — or a crash-recovery re-drive of the same step (same
    step_number, same content) — reproduces the exact same id, which
    Actae's server-side idempotency turns into "return the original event
    instead of duplicating". Different step content derives a distinct id
    and records a new event.
    """
    canonical = _canonical_step_content(payload, metadata)
    name = "\x00".join(
        ["agent-session", step_type, channel_id, str(step_num), canonical]
    )
    name_bytes = name.encode("utf-8")[:256]

    digest = hashlib.sha1(_STEP_ID_NAMESPACE.bytes + name_bytes).digest()
    raw = bytearray(digest[:16])
    raw[6] = (raw[6] & 0x0F) | 0x50  # version 5
    raw[8] = (raw[8] & 0x3F) | 0x80  # variant 10
    return str(uuid.UUID(bytes=bytes(raw)))


class SessionError(Exception):
    """Base error for AgentSession lifecycle violations."""


class SessionCompletedError(SessionError):
    """Raised when attempting to step on an already-completed session."""


class SessionLockedError(SessionError):
    """Raised when a fenced session cannot own (or no longer owns) its channel.

    A session started with ``fence_owner`` claims a per-channel lease through
    the idempotent execution ledger. Starting a second live owner, or continuing
    after another owner reclaimed the lease, raises this instead of silently
    appending events to a channel another worker now owns.
    """


def _is_retryable(error: Exception) -> bool:
    """Return True for transient errors that benefit from a retry."""
    if isinstance(error, ActaeConnectionError):
        return True
    if isinstance(error, RateLimitError):
        # HTTP 429 is transient by definition — the client raises
        # RateLimitError (not APIError) for it, so it must be retried here.
        return True
    if isinstance(error, APIError):
        # Retry on server errors and timeouts, not on auth/client errors
        return error.status_code >= 500 or error.status_code == 429
    # asyncio.TimeoutError is a base class; aiohttp wraps timeouts differently
    if isinstance(error, asyncio.TimeoutError):
        return True
    return False


async def _retry(coro_name: str, coro_fn, session_name: str) -> Any:
    """Call *coro_fn* with exponential backoff on transient failures."""
    last_exc: Optional[Exception] = None
    for attempt in range(1, _MAX_RETRIES + 2):  # N+1 total attempts
        try:
            return await coro_fn()
        except Exception as exc:
            last_exc = exc
            if not _is_retryable(exc) or attempt > _MAX_RETRIES:
                raise
            delay = min(_RETRY_BASE_DELAY * (2 ** (attempt - 1)), _RETRY_MAX_DELAY)
            logger.debug(
                "%s attempt %d/%d failed for session '%s' (%s) — retrying in %.1fs",
                coro_name, attempt, _MAX_RETRIES + 1, session_name, exc, delay,
            )
            await asyncio.sleep(delay)
    raise last_exc  # type: ignore[misc]


async def _safe_latest_step_number(
    actae: "SessionTransport", channel_id: str
) -> Optional[int]:
    """Best-effort ``latest_step_number``; ``None`` when unsupported/unavailable.

    A transport that predates the endpoint (or a fleet transport that has not
    implemented it) must not break resume — callers fall back to a replay scan.
    """
    method = getattr(actae, "latest_step_number", None)
    if method is None:
        return None
    try:
        value = await method(channel_id)
    except Exception:  # noqa: BLE001 - fall back to replay-based recovery
        return None
    # Only a genuine integer is meaningful (guards against mock/duck transports
    # whose method returns something else).
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return value


async def _recover_step_progress(
    actae: "SessionTransport",
    channel_id: str,
    *,
    max_events: int = 50000,
) -> Optional[List[int]]:
    """Reconstruct the per-step cursor list from the durable event log.

    Fallback for servers without ``GET /channels/{id}/steps``; the
    normal resume path uses the O(1) server step index.

    Session metadata is written on lifecycle transitions (start, first step,
    completion, caught crash) — so a **hard** process death (`os._exit`,
    SIGKILL, power loss) can leave it lagging. The event log is authoritative:
    every ``step()`` writes ``step_number`` into its payload in the same
    server transaction as the event. Returns cursors indexed by step
    (position ``i`` == step ``i+1``), or ``None`` when no step events exist
    (caller falls back to metadata).
    """
    replay = getattr(actae, "replay", None)
    if replay is None:
        return None
    by_step: Dict[int, int] = {}
    page_cursor: Optional[int] = None
    seen = 0
    while seen < max_events:
        try:
            batch = await replay(channel_id, cursor=page_cursor, limit=1000)
        except Exception:  # noqa: BLE001 - best-effort recovery, never fatal
            return None
        if not isinstance(batch, (list, tuple)):
            return None
        if not batch:
            break
        for ev in batch:
            payload = ev.payload if isinstance(ev.payload, dict) else {}
            step_number = payload.get("step_number")
            if isinstance(step_number, int) and step_number >= 1:
                by_step.setdefault(step_number, ev.cursor)
        seen += len(batch)
        last_cursor = batch[-1].cursor
        if page_cursor is not None and last_cursor <= page_cursor:
            break
        page_cursor = last_cursor
    if not by_step:
        return None
    return [by_step.get(i, 0) for i in range(1, max(by_step) + 1)]


class AgentSession:
    """Wrapper around an Actae channel that records agent steps as events.

    Tracks step → cursor mapping so you can fork at a user-facing step
    number rather than a raw server cursor.  Saves state snapshots every
    ``snapshot_interval`` steps via an optional ``state_fn`` callback.

    Lifecycle::

        created → started → stepping → completed
                                      ↘ crashed

    Fork precision: step → cursor mappings are stored in channel metadata;
    beyond the most recent 1000 steps only the latest cursors are persisted
    (older steps fall back to a replay-based resolution on ``resume``).

    Threading: *not* thread-safe.  Intended for a single async task.
    The ``asyncio.Lock`` serialises concurrent ``step()`` calls, but
    ``fork()`` and ``resume()`` should not overlap with ``step()``.
    """

    #  Valid statuses for each operation (compiled once) 
    _VALID_STEP_STATUSES = frozenset({"started", "stepping"})
    _VALID_FORK_STATUSES = frozenset({"started", "stepping", "completed", "crashed"})

    def __init__(
        self,
        actae: SessionTransport,
        name: str,
        *,
        display_name: Optional[str] = None,
        state_fn: Optional[Callable[[], Optional[Dict[str, Any]]]] = None,
        snapshot_interval: int = 1,
        params: Optional[Dict[str, Any]] = None,
        fence_owner: Optional[str] = None,
        fence_lease_seconds: int = 60,
    ) -> None:
        """Create a new AgentSession.

        Use ``async with`` to start the session. For crash recovery or
        fork-from-existing, use ``AgentSession.resume()`` instead.

        Args:
            actae: Connected ActaeClient instance.
            name: Session name (used as the channel identifier).
            display_name: Optional human-readable name for the dashboard.
                Falls back to *name* if not set.
            state_fn: Optional callable (sync or async) that returns the
                current agent state dict. Called every
                ``snapshot_interval`` steps. An ``async def`` function is
                supported — it is awaited before saving.
            snapshot_interval: Save a state snapshot every N steps (default 1).
            params: Arbitrary parameters dict stored in metadata.
            fence_owner: Optional owner id. When set, the session claims a
                per-channel write lease through the idempotent execution
                ledger; a second live owner (or a stale owner whose lease was
                reclaimed) raises :class:`SessionLockedError` instead of
                appending events. Off by default (Actae never owns the agent
                loop); turn it on for long-running workers where two processes
                must never write the same channel.
            fence_lease_seconds: Lease duration for the fence (default 60).
                Renewed automatically on every ``step``; the owner must renew
                (i.e. step) within this window or another owner may reclaim it.
        """
        if not name:
            raise ValueError("name is required")

        self._actae = actae
        self._name = name
        self._display_name = display_name
        self._state_fn = state_fn
        self._snapshot_interval = max(1, snapshot_interval)
        self._params = params or {}
        self._fence_owner = fence_owner
        self._fence_lease = max(1, int(fence_lease_seconds))
        self._fence_execution_id: Optional[str] = None
        self._fence_token: Optional[str] = None

        self._channel_id: Optional[str] = None
        self._step_count: int = 0
        self._cursors: List[int] = []
        self._started_at: Optional[str] = None
        self._status: str = "created"
        self._is_resumed: bool = False
        self._lock = asyncio.Lock()
        # State snapshot inherited by a fork session (steps 1..N's data at the
        # fork boundary). None for fresh sessions.
        self._inherited_state: Optional[Dict[str, Any]] = None
        # Fork boundary provenance (filled after fork()/resume(fork_at_step=)):
        # whether the fork is a restorable checkpoint, the requested vs
        # resolved boundary cursors, the source state version/hash and the
        # reproducibility grade. Stays None for non-fork sessions.
        self._boundary_restorable: Optional[bool] = None
        self._requested_boundary_cursor: Optional[int] = None
        self._resolved_boundary_cursor: Optional[int] = None
        self._source_state_version: Optional[int] = None
        self._source_state_sha256: Optional[str] = None
        self._reproducibility: Optional[str] = None
        self._fork_receipt: Optional[Any] = None
        # Fork ergonomics: the opaque intervention descriptor recorded on the
        # fork's metadata (the APPLICATION reads it and changes behavior —
        # Actae stores it, never interprets or executes it) and the
        # side-effect tool policy applied to the child channel.
        self._intervention: Dict[str, Any] = {}
        self._tool_policies: Optional[Dict[str, Any]] = None

    #  Public properties 

    @property
    def channel_id(self) -> Optional[str]:
        """Actae channel ID backing this session (None before ``__aenter__``)."""
        return self._channel_id

    @property
    def name(self) -> str:
        """Session name (used as channel identifier)."""
        return self._name

    @property
    def step_count(self) -> int:
        """Number of steps recorded so far."""
        return self._step_count

    @property
    def status(self) -> str:
        """Current session lifecycle status.

        One of ``"created"``, ``"started"``, ``"stepping"``, ``"completed"``,
        or ``"crashed"``.
        """
        return self._status

    @property
    def cursors(self) -> List[int]:
        """Cursor value for each recorded step (index == step - 1)."""
        return list(self._cursors)

    @property
    def params(self) -> Dict[str, Any]:
        """Session parameters dict (read-only copy)."""
        return dict(self._params)

    @property
    def intervention(self) -> Dict[str, Any]:
        """The opaque intervention descriptor recorded on this fork (read-only).

        Actae stores the descriptor; the **application** must read it and apply
        the change (model, prompt, feature flag, …). Actae never interprets it
        and never executes anything on its behalf.
        """
        return dict(self._intervention)

    @property
    def tool_policies(self) -> Optional[Dict[str, Any]]:
        """The side-effect tool policy applied to this fork's channel, if any.

        ``None`` means the server default (``auto``): an exact inherited tool
        result is replayed; new or changed work runs live.
        """
        return dict(self._tool_policies) if self._tool_policies else None

    @property
    def inherited_state(self) -> Optional[Dict[str, Any]]:
        """State snapshot this session resumed with (steps 1..N's data).

        - After ``resume(..., fork_at_step=N)`` or ``fork(at_step=N)`` this is
          the parent's state at the fork boundary — the server copies the
          parent's saved state at the fork cursor into the fork channel, and
          this returns it so the fork's first step can run against the
          parent's accumulated data.
        - After ``resume(...)`` on a crashed channel this is the last saved
          state before the crash.

        ``None`` for a fresh session.
        """
        if self._inherited_state is None:
            return None
        return dict(self._inherited_state)

    @property
    def boundary_restorable(self) -> Optional[bool]:
        """True when this fork session inherited a restorable state checkpoint.

        ``False`` for lineage-only forks (no snapshot existed at the boundary);
        ``None`` for non-fork sessions. When ``False``, the session was NOT
        primed with exact inherited state and ``resolved_boundary_cursor`` may
        be beyond the requested boundary.
        """
        return self._boundary_restorable

    @property
    def requested_boundary_cursor(self) -> Optional[int]:
        """The fork boundary the caller asked for (0 = latest)."""
        return self._requested_boundary_cursor

    @property
    def resolved_boundary_cursor(self) -> Optional[int]:
        """The snapshot cursor the fork actually inherited.

        Differs from ``requested_boundary_cursor`` when the nearest snapshot
        was at an earlier cursor (or, in ``approximate`` mode, when the fork
        fell back to the latest state).
        """
        return self._resolved_boundary_cursor

    @property
    def source_state_version(self) -> Optional[int]:
        """The source snapshot version copied at fork time (0 = none)."""
        return self._source_state_version

    @property
    def source_state_sha256(self) -> Optional[str]:
        """SHA-256 fingerprint of the copied state (immutable provenance)."""
        return self._source_state_sha256

    @property
    def reproducibility(self) -> Optional[str]:
        """Server-computed reproducibility grade for this fork:
        ``state_exact`` | ``context_exact`` | ``execution_replayable`` |
        ``best_effort``.
        """
        return self._reproducibility

    @property
    def fork_receipt(self) -> Optional[Any]:
        """The immutable fork receipt returned at fork creation."""
        return self._fork_receipt

    #  Status guards 

    def _require_status(self, allowed: frozenset, operation: str) -> None:
        """Raise SessionError if the current status is not in *allowed*."""
        if self._status not in allowed:
            raise SessionError(
                f"Session '{self._name}' is {self._status}, cannot {operation}."
            )

    #  Lifecycle (context manager) 

    async def __aenter__(self) -> "AgentSession":
        """Enter the session context — starts or resumes recording."""
        if self._is_resumed:
            self._status = "started"
            await self._claim_fence()
            await self._update_metadata_status("started")
        else:
            await self._start()
        return self

    async def __aexit__(self, *args: Any) -> None:
        """Exit the session context — marks session as completed or crashed."""
        exc_type = args[0] if args else None
        try:
            if exc_type is not None:
                exc_name = f"{exc_type.__name__}: {args[1]}" if exc_type else "unknown"
                await self._crash(exc_name)
            else:
                await self._complete()
        finally:
            await self._release_fence()

    async def _start(self) -> None:
        # Claim the channel lease BEFORE recording session.started so a locked
        # channel is not polluted by a session that cannot own it.
        if self._fence_owner:
            self._channel_id = self._name
            await self._claim_fence()
        ev = await self._actae.record(
            channel_id=self._name,
            event_type=SESSION_STARTED,
            payload={"session_name": self._name, "params": self._params},
            actor="agent_session",
            metadata={"session_name": self._name, "snapshot_interval": self._snapshot_interval,
                      "has_state_fn": self._state_fn is not None},
        )
        self._channel_id = ev.channel_id
        self._started_at = ev.timestamp
        self._status = "started"
        await self._update_metadata_status("started")

    async def _complete(self) -> None:
        if self._status in ("completed", "crashed"):
            return

        try:
            await self._actae.record(
                channel_id=self._channel_id,
                event_type=SESSION_COMPLETED,
                payload={"session_name": self._name, "total_steps": self._step_count},
                actor="agent_session",
                metadata={"total_steps": self._step_count},
            )
        except Exception:
            logger.exception("Failed to record session.completed for '%s'", self._name)

        self._status = "completed"
        await self._update_metadata_status("completed")

    async def _crash(self, reason: str) -> None:
        logger.warning("Session '%s' crashed: %s", self._name, reason)
        self._status = "crashed"
        try:
            await self._update_metadata_status("crashed", extra={"crash_reason": reason})
        except Exception:
            logger.exception("Failed to update metadata after crash for '%s'", self._name)
        # Record the fork outcome for experiment ranking/audit (best-effort).
        try:
            await self._actae.set_outcome(self._ensure_channel(), "crashed")
        except Exception:
            logger.debug("Failed to record crashed outcome for '%s'", self._name)

    async def _update_metadata_status(self, status: str, extra: Optional[Dict[str, Any]] = None) -> None:
        """Persist session metadata to channel_metadata with retry on
        transient failures.

        The read-modify-write cycle is safe because:
        1. ``asyncio.Lock`` serialises all calls within this session.
        2. Only ONE AgentSession instance owns a given channel; fork
           metadata is written once by the source session and never
           written again by the fork session's ``_start`` (which uses
           a merge that preserves existing fork keys).
        """
        channel_id = self._ensure_channel()

        async def _do_update():
            # Load existing metadata (e.g. from a fork) so we don't overwrite it
            existing: Dict[str, Any] = {}
            try:
                meta_obj = await self._actae.get_channel_metadata(channel_id)
                if meta_obj and meta_obj.experiment_metadata:
                    existing = dict(meta_obj.experiment_metadata)
            except Exception:
                pass

            # Build the cursors list, capped for storage efficiency
            stored_cursors = self._cursors
            if len(stored_cursors) > _MAX_STORED_CURSORS:
                stored_cursors = stored_cursors[-_MAX_STORED_CURSORS:]

            meta: Dict[str, Any] = {
                "session_name": self._name,
                "params": self._params,
                "status": status,
                "started_at": self._started_at,
                "step_count": self._step_count,
                "cursors": stored_cursors,
                "cursors_stored": len(stored_cursors),
            }
            if len(self._cursors) > _MAX_STORED_CURSORS:
                meta["cursors_warning"] = (
                    f"Only last {_MAX_STORED_CURSORS} cursors stored; "
                    f"total steps: {self._step_count}"
                )

            # Merge new keys over existing, preserving fork metadata
            merged = {**existing, **meta}
            if extra:
                merged.update(extra)

            await self._actae.update_metadata(
                channel_id,
                display_name=self._display_name or self._name,
                experiment_metadata=merged,
            )

        await _retry("update_metadata", _do_update, self._name)

    #  Step 

    def _ensure_channel(self) -> str:
        """Return the validated channel_id, raising if None."""
        if self._channel_id is None:
            raise SessionError(
                f"Session '{self._name}' has no channel — enter the context manager first"
            )
        return self._channel_id

    #  Session ownership fence (opt-in via ``fence_owner``) 

    def _fence_transport(self) -> Any:
        transport = self._actae
        if not all(
            hasattr(transport, method)
            for method in ("claim_execution", "heartbeat_execution", "cancel_execution")
        ):
            raise SessionError(
                "fence_owner requires a transport with the execution ledger "
                "(claim_execution / heartbeat_execution / cancel_execution)"
            )
        return transport

    async def _claim_fence(self) -> None:
        """Claim the per-channel write lease; raise if another owner is live."""
        if not self._fence_owner:
            return
        transport = self._fence_transport()
        channel = self._ensure_channel()
        # The lease request identity is deliberately owner-independent: a
        # different owner must reach the lease check (in_progress/reclaimed),
        # not a request-hash mismatch. The owner id is metadata, not identity.
        claim = await transport.claim_execution(
            channel,
            SESSION_FENCE_KEY,
            SESSION_FENCE_TOOL,
            {},
            lease_seconds=self._fence_lease,
        )
        if claim.status in ("in_progress", "replayed"):
            raise SessionLockedError(
                f"channel '{channel}' is owned by another live session "
                f"(fence status={claim.status}); refusing owner '{self._fence_owner}'"
            )
        self._fence_execution_id = claim.execution.id
        self._fence_token = claim.claim_token

    async def _assert_fence(self) -> None:
        """Renew the lease; raise if another owner has taken it over."""
        if not self._fence_owner or self._fence_execution_id is None:
            return
        transport = self._fence_transport()
        try:
            await transport.heartbeat_execution(
                self._fence_execution_id, self._fence_token, lease_seconds=self._fence_lease
            )
        except ExecutionNotOwnedError as exc:
            self._fence_execution_id = None
            self._fence_token = None
            raise SessionLockedError(
                f"session '{self._name}' lost its channel lease to another owner"
            ) from exc

    async def _release_fence(self) -> None:
        if self._fence_execution_id is None:
            return
        transport = self._fence_transport()
        try:
            await transport.cancel_execution(self._fence_execution_id, self._fence_token)
        except Exception:
            logger.debug("Failed to release session fence for '%s'", self._name)
        finally:
            self._fence_execution_id = None
            self._fence_token = None

    async def step(
        self,
        step_type: str,
        *,
        input: Any = None,
        output: Any = None,
        context: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Event:
        """Record a single execution step as an event with retry on
        transient failures.

        Args:
            step_type: Type label for the step (e.g. ``"inference"``, ``"tool_call"``).
            input: Input data for the step.
            output: Output data from the step.
            context: Delta context for this step — the agent state generated
                *at this step* (e.g. new messages, thinking, tool results).
                Stored in the event payload so the dashboard can reconstruct
                the cumulative full context at any node by merging deltas
                across the cursor-linked event chain.
            metadata: Optional key-value metadata attached to the event.

        Each step carries a DETERMINISTIC ``operation_id``: a UUIDv5 over
        NUL-joined (scope, step type, channel, step number, canonical
        payload+metadata) — byte-identical to the Go SDK's
        ``DeterministicOperationKey``. A retry, or a crash-recovery
        re-drive of the same step with the same content, derives the SAME
        id, so the server replays the original event instead of recording
        a duplicate; changed content records a new event.

        Returns the persisted ``Event`` so you can inspect its cursor.
        """
        self._require_status(self._VALID_STEP_STATUSES, "record steps")

        async with self._lock:
            channel_id = self._ensure_channel()
            # Fenced sessions renew (and validate) their channel lease before
            # every step: a stale owner is rejected here, not by a silent
            # duplicate append.
            await self._assert_fence()
            previous_status = self._status
            self._status = "stepping"

            step_num = self._step_count + 1
            event_meta: Dict[str, Any] = dict(metadata) if metadata else {}
            event_meta["step_number"] = step_num

            payload: Dict[str, Any] = {
                "step_number": step_num,
                "input": input,
                "output": output,
            }
            if context is not None:
                payload["context"] = context

            # A DETERMINISTIC per-step operation_id makes the record
            # idempotent: if the first attempt persisted but its response
            # was lost, the retry — or a crash-recovery re-drive of the
            # same step (same step_number, same content) — derives the SAME
            # id, so the server replays the original event instead of
            # recording a duplicate. Step content that differs derives a
            # distinct id and records a new event. The derivation (UUIDv5
            # over NUL-joined scope/type/channel/step_number/canonical
            # content, 256-byte cap) is byte-identical to the Go SDK's
            # DeterministicOperationKey (Go is the reference; the TS SDK
            # reuses one random id per step call — retry-safe, not
            # crash-deterministic).
            step_operation_id = _step_operation_id(
                channel_id, step_type, step_num, payload, event_meta,
            )

            async def _record():
                return await self._actae.record(
                    channel_id=channel_id,
                    event_type=step_type,
                    payload=payload,
                    actor="agent_session",
                    metadata=event_meta,
                    operation_id=step_operation_id,
                    step_number=step_num,
                )

            ev = await _retry("step", _record, self._name)

            self._step_count = step_num
            self._cursors.append(ev.cursor)

            if self._state_fn and self._step_count % self._snapshot_interval == 0:
                try:
                    state = self._state_fn()
                    if inspect.isawaitable(state):
                        state = await state
                    if state is not None:
                        await self._actae.save_state(channel_id, ev.cursor, state)
                except Exception:
                    logger.exception(
                        "state_fn raised on step %d — skipping snapshot",
                        self._step_count,
                    )

            if self._step_count % self._snapshot_interval == 0 and previous_status != "stepping":
                await self._update_metadata_status("stepping")

            return ev

    #  Fork 

    async def fork(
        self,
        at_step: int,
        name: str,
        *,
        display_name: Optional[str] = None,
        params: Optional[Dict[str, Any]] = None,
        state_fn: Optional[Callable[[], Optional[Dict[str, Any]]]] = None,
        reason: Optional[str] = None,
        boundary_mode: str = "exact",
        manifest: Optional[Dict[str, Any]] = None,
        intervention: Optional[Dict[str, Any]] = None,
        tool_policies: Optional[Dict[str, Any]] = None,
    ) -> "AgentSession":
        """Fork this session at a given step into a new experiment fork.

        Returns an *unstarted* ``AgentSession`` — call ``async with`` on it to
        begin recording events on the fork channel.

        Boundary policy (``boundary_mode``):
        - ``"exact"`` (default): the fork must inherit a restorable checkpoint
          at-or-before the step's cursor. When no snapshot exists there,
          :class:`NoRestorableCheckpointError` is raised instead of silently
          inheriting later state. Save state on every step (``state_fn`` +
          ``snapshot_interval=1``) to make every step forkable.
        - ``"approximate"``: on a missing snapshot, fall back to the latest
          saved state and report the drift via ``resolved_boundary_cursor`` /
          ``boundary_restorable`` — the inherited state is NOT an exact
          checkpoint and may contain data from after the requested step.
        - ``"lineage_only"``: create the fork with no state copy
          (``boundary_restorable=False``) so event-only channels still fork.

        The immutable fork receipt is available on the returned session via
        ``fork_receipt``; provenance is also exposed through
        ``boundary_restorable``, ``requested_boundary_cursor``,
        ``resolved_boundary_cursor``, ``source_state_version``,
        ``source_state_sha256`` and ``reproducibility``.

        Side-effect-aware forking (``tool_policies``): an opaque
        ``{"tool_name": "replay"|"block"|"live"|"auto", "*": default}`` map.
        ``auto`` (the default) replays an exact inherited tool result and runs
        new or changed work live; ``replay``/``block`` refuse on a miss or
        mismatch (``fork_tool_blocked``); ``live`` is today's behavior. This
        keeps a fork from silently re-firing a production side effect.

        ``intervention``: an opaque descriptor of the intended change (e.g.
        ``{"model": "new-model", "prompt": "..."}``) recorded on the fork's
        metadata. **The application must read ``session.intervention`` and
        apply the change** — Actae stores it but never interprets or executes
        it. Use it together with ``tool_policies`` so "change the decision"
        cannot re-fire an inherited effect.
        """
        self._require_status(self._VALID_FORK_STATUSES, "fork")
        if not self._channel_id:
            raise SessionError("Session has no channel — was it started?")
        if not name:
            raise ValueError("fork name is required")

        # Resolve the owning channel + cursor for this step, walking up the
        # lineage if `at_step` is an inherited step of a fork session.
        source_channel, cursor = await self._resolve_step_owner(self._channel_id, at_step)

        merged_params = dict(self._params)
        if params:
            merged_params.update(params)

        experiment_metadata = {
            "session_name": name,
            "params": merged_params,
            "forked_from": source_channel,
            "forked_from_channel": self._channel_id,
            "forked_at_step": at_step,
            "forked_at_cursor": cursor,
            "status": "created",
        }
        if intervention:
            experiment_metadata["intervention"] = dict(intervention)

        await self._perform_fork(
            source_channel,
            name,
            cursor,
            at_step=at_step,
            display_name=display_name or name,
            reason=reason or f"Forked from {self._name} at step {at_step}",
            experiment_metadata=experiment_metadata,
            boundary_mode=boundary_mode,
            manifest=manifest,
            tool_policies=tool_policies,
        )

        session = AgentSession(
            self._actae,
            name,
            display_name=display_name,
            state_fn=state_fn or self._state_fn,
            snapshot_interval=self._snapshot_interval,
            params=merged_params,
        )
        session._channel_id = name
        # Copy the fork provenance from this (source) session so the RETURNED
        # fork session exposes the receipt + boundary metadata directly.
        session._fork_receipt = self._fork_receipt
        session._boundary_restorable = self._boundary_restorable
        session._requested_boundary_cursor = self._requested_boundary_cursor
        session._resolved_boundary_cursor = self._resolved_boundary_cursor
        session._source_state_version = self._source_state_version
        session._source_state_sha256 = self._source_state_sha256
        session._reproducibility = self._reproducibility
        session._intervention = dict(intervention or {})
        session._tool_policies = (
            dict(tool_policies) if tool_policies else session._tool_policies
        )
        # Prime the fork session: continue at step `at_step + 1` with the
        # inherited state snapshot (steps 1..N's data), so refining step N+1
        # does not require re-running 1..N.
        await session._prime_fork_session(self._channel_id, at_step)
        return session

    async def _perform_fork(
        self,
        source_channel: str,
        child_name: str,
        cursor: int,
        *,
        at_step: int,
        display_name: str,
        reason: str,
        experiment_metadata: Dict[str, Any],
        boundary_mode: str,
        manifest: Optional[Dict[str, Any]],
        tool_policies: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Perform the server fork, applying the strict/approximate/lineage_only
        boundary policy. Records the immutable fork receipt + provenance on
        this session.
        """
        if boundary_mode not in ("exact", "approximate", "lineage_only"):
            raise SessionError(
                f"invalid boundary_mode '{boundary_mode}' "
                "(expected 'exact' | 'approximate' | 'lineage_only')"
            )

        effective_manifest = self._build_manifest(manifest)

        async def _call(c: int):
            result = await self._actae.fork(
                source_channel,
                child_name,
                c,
                display_name=display_name,
                reason=reason,
                experiment_metadata=experiment_metadata,
                manifest=effective_manifest,
                tool_policies=tool_policies,
            )
            # Older servers / mocks may return metadata-shaped data instead of
            # a full receipt — normalize so the session sees the same shape.
            if not hasattr(result, "requested_cursor"):
                from .types import ForkReceipt

                return ForkReceipt.from_metadata(result)
            return result

        try:
            receipt = await _call(cursor)
        except SnapshotBoundaryError:
            if boundary_mode == "exact":
                raise NoRestorableCheckpointError(
                    f"No restorable checkpoint at step {at_step} (cursor {cursor}) on "
                    f"channel '{source_channel}'. No saved snapshot exists at or before the "
                    f"boundary. Save state on every step you care about (provide a "
                    f"state_fn with snapshot_interval=1), or fork with "
                    f"boundary_mode='approximate' (inherit nearest/latest, drift reported) "
                    f"or boundary_mode='lineage_only' (create the fork with no state copy)."
                )
            logger.warning(
                "no saved state at step %d cursor %d on '%s' — boundary_mode=%s forking "
                "from the latest state (inherited state will NOT be an exact checkpoint)",
                at_step,
                cursor,
                source_channel,
                boundary_mode,
            )
            receipt = await _call(0)
            # The server recorded the fallback as a "latest" request; surface
            # the user's true intended boundary as the requested cursor so
            # session provenance reflects the drift honestly.
            receipt.requested_cursor = cursor

        self._fork_receipt = receipt
        self._boundary_restorable = receipt.restorable
        self._requested_boundary_cursor = receipt.requested_cursor
        self._resolved_boundary_cursor = receipt.resolved_cursor
        self._source_state_version = receipt.source_state_version
        self._source_state_sha256 = receipt.source_state_sha256
        self._reproducibility = receipt.reproducibility
        self._tool_policies = getattr(receipt, "tool_policies", None)

        if (
            self._requested_boundary_cursor
            and self._resolved_boundary_cursor
            and self._resolved_boundary_cursor > self._requested_boundary_cursor
        ):
            logger.warning(
                "fork '%s' resolved to cursor %s which is BEYOND the requested %s — "
                "the inherited state is not an exact checkpoint (temporal contamination "
                "risk). Inspect session.resolved_boundary_cursor before trusting the fork.",
                child_name,
                self._resolved_boundary_cursor,
                self._requested_boundary_cursor,
            )

    @staticmethod
    def _build_manifest(caller_manifest: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """Build the fork manifest: caller-supplied fields merged over the
        auto-detected environment fingerprint. The server grades
        `context_exact` only when model + environment + dependencies + seed
        are all present — so a caller must supply the full manifest to claim
        context-exact reproducibility.
        """
        auto: Dict[str, Any] = {
            "environment": {
                "python": platform.python_version(),
                "platform": platform.platform(),
            },
            "sdk": _SDK_VERSION,
        }
        if caller_manifest:
            merged = dict(auto)
            merged.update(caller_manifest)
            return merged
        return auto

    async def require_approval(
        self,
        summary: str,
        *,
        details: Optional[Dict[str, Any]] = None,
        timeout_seconds: Optional[float] = None,
        poll_interval: float = 1.0,
        requester: str = "agent",
    ) -> Dict[str, Any]:
        """Pause the loop for a durable human approval, then return the verdict.

        Records ``approval.requested`` on this session's channel, waits for a
        matching ``approval.decided`` (poll-based; the agent process need not
        stay alive for the request to survive a restart), and on timeout records
        ``approval.expired``. Returns
        ``{"request_id", "approved", "decision", "event"}`` where ``decision``
        is the raw event payload (or ``None`` on timeout).

        The application decides what an approval means; Actae only records it.
        """
        channel = self._ensure_channel()
        start = await self._actae.latest_cursor(channel) or 0
        request_id = await self._actae.request_approval(
            channel,
            summary=summary,
            details=details,
            timeout_seconds=timeout_seconds,
            requester=requester,
        )
        event = await self._actae.wait_for_approval(
            channel,
            request_id,
            timeout_seconds=timeout_seconds,
            poll_interval=poll_interval,
            start_cursor=start,
        )
        payload = dict(event.payload) if event is not None else None
        return {
            "request_id": request_id,
            "approved": bool(payload and payload.get("decision") == "approved"),
            "decision": payload,
            "event": event,
        }

    async def set_outcome(
        self, outcome: str, *, score: Optional[float] = None
    ) -> None:
        """Record this fork's outcome (promoted | rejected | inconclusive |
        crashed) and an optional numeric result score.
        """
        channel = self._ensure_channel()
        await self._actae.set_outcome(channel, outcome, score=score)

    async def promote(self) -> Dict[str, Any]:
        """Promote this winning fork into its parent (append-only merge).
        Requires the session to be a fork of a parent channel.
        """
        channel = self._ensure_channel()
        return await self._actae.promote_channel(channel)

    async def create_experiment(
        self,
        name: str,
        *,
        description: Optional[str] = None,
        baseline_channel_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Create an experiment group with this session as the baseline (when
        ``baseline_channel_id`` is omitted, this session is used).
        """
        baseline = baseline_channel_id or self._ensure_channel()
        return await self._actae.create_experiment(
            name, description=description, baseline_channel_id=baseline
        )

    async def add_to_experiment(
        self,
        group_id: str,
        *,
        role: str = "variant",
        declared_delta: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Add this session's channel to an experiment group as a variant."""
        channel = self._ensure_channel()
        return await self._actae.add_experiment_member(
            group_id, channel, role=role, declared_delta=declared_delta
        )

    #  Resume 

    @classmethod
    async def resume(
        cls,
        actae: SessionTransport,
        channel_id: str,
        *,
        fork_at_step: Optional[int] = None,
        name: Optional[str] = None,
        params: Optional[Dict[str, Any]] = None,
        state_fn: Optional[Callable[[], Optional[Dict[str, Any]]]] = None,
        snapshot_interval: int = 1,
        boundary_mode: str = "exact",
        manifest: Optional[Dict[str, Any]] = None,
        intervention: Optional[Dict[str, Any]] = None,
        tool_policies: Optional[Dict[str, Any]] = None,
    ) -> "AgentSession":
        """Resume a previously-run (or crashed) session.

        **Crash recovery** (no ``fork_at_step``)::

            session = AgentSession.resume(actae, "exp-v1")
            # If "exp-v1" crashed, resumes from the last step.
            # If "exp-v1" completed, raises SessionCompletedError.

        **Fork-from-existing** (``fork_at_step`` set)::

            session = await AgentSession.resume(
                actae, "exp-v1",
                fork_at_step=5, name="exp-v2", params={"model": "gpt-5"},
            )
            # Forks "exp-v1" at step 5 → channel "exp-v2". The fork session
            # inherits steps 1-5's state (session.inherited_state), continues
            # at step 6, and records its steps on "exp-v2" — steps 1-5 are
            # NOT re-run. Use `async with session:` to start it.

        ``boundary_mode`` (default ``"exact"``) and ``manifest`` behave as in
        :meth:`fork` — see its docstring for the strict/approximate/lineage_only
        boundary policy.
        """
        meta = await actae.get_channel_metadata(channel_id)
        if meta is None:
            raise SessionError(f"Channel '{channel_id}' not found (no metadata)")

        exp_meta: Dict[str, Any] = meta.experiment_metadata or {}
        status: str = exp_meta.get("status", "unknown")

        if fork_at_step is not None:
            if not name:
                raise SessionError("'name' is required when fork_at_step is set")
            session = cls(actae, name, display_name=meta.display_name, state_fn=state_fn, snapshot_interval=snapshot_interval, params=params)
            session._channel_id = name
            session._intervention = dict(intervention or {})
            await session._fork_from_channel(
                channel_id,
                fork_at_step,
                boundary_mode=boundary_mode,
                manifest=manifest,
                intervention=intervention,
                tool_policies=tool_policies,
            )
            return session
        if status == "completed":
            raise SessionCompletedError(
                f"Session '{channel_id}' is already completed. "
                f"Pass fork_at_step to fork from it."
            )
        elif status == "crashed":
            logger.info(
                "Resuming crashed session '%s' (last step: %s)",
                channel_id, exp_meta.get("step_count", "?"),
            )

        session_params = dict(exp_meta.get("params") or {})
        if params:
            session_params.update(params)

        step_count: int = exp_meta.get("step_count", 0)
        cursors: List[int] = list(exp_meta.get("cursors") or [])
        # A hard process death skips the SDK's metadata write, so metadata can
        # lag real progress (historically back to step 1). Recover the true
        # progress from the server-owned step index (O(1)) and only override
        # metadata when it is AHEAD of it — retention-purged events must never
        # make a resume go backwards.
        last_step = await _safe_latest_step_number(actae, channel_id)
        if last_step is None:
            # Older server without the /steps route: fall back to a
            # paged replay of the durable event log.
            recovered_cursors = await _recover_step_progress(actae, channel_id)
            if recovered_cursors is not None and len(recovered_cursors) > step_count:
                cursors = recovered_cursors
                step_count = len(recovered_cursors)
        elif last_step > step_count:
            step_count = last_step
            if len(cursors) < step_count:
                cursors += [0] * (step_count - len(cursors))

        session = cls(
            actae, channel_id,
            display_name=meta.display_name,
            state_fn=state_fn, snapshot_interval=snapshot_interval, params=session_params,
        )
        session._channel_id = channel_id
        session._step_count = step_count
        session._cursors = cursors
        session._started_at = exp_meta.get("started_at")
        session._status = "started"
        session._is_resumed = True
        # Preserve a fork's intervention descriptor across crash recovery so a
        # resumed fork still knows what the application intended to change.
        session._intervention = dict(exp_meta.get("intervention") or {})
        # Crash recovery: expose the last saved state so the caller can
        # continue from where the run stopped.
        try:
            snap = await actae.latest_state(channel_id)
            if snap is not None:
                session._inherited_state = snap.get("state")
        except Exception:
            session._inherited_state = None
        return session

    @classmethod
    async def resume_from_fork(
        cls,
        transport: SessionTransport,
        receipt: Any,
        **kwargs: Any,
    ) -> "AgentSession":
        """Resume the exact child named by a fork receipt on any transport.

        The receipt, rather than a "latest fork" lookup, is the stable
        hand-off between creation and continued execution.
        """
        child = getattr(receipt, "child_channel_id", None)
        if child is None and isinstance(receipt, dict):
            child = receipt.get("child_channel_id") or receipt.get("channel_id")
        if not child:
            raise SessionError("fork receipt has no child_channel_id")
        return await cls.resume(transport, child, **kwargs)

    async def _resolve_step_owner(
        self, channel_id: str, at_step: int
    ) -> "tuple[str, int]":
        """Resolve the channel that owns ``at_step`` and its cursor.

        Walks up the fork lineage: a fork inherits its first
        ``forked_at_step`` steps from its parent, so forking a fork at an
        inherited step must resolve against the ancestor that recorded it.
        Returns ``(owner_channel, cursor)`` where cursor is the owner's
        per-channel cursor for ``at_step`` (from its metadata ``cursors``
        list, falling back to a position-based replay resolution).

        Raises ``SessionError`` when the step cannot be resolved.
        """
        if at_step < 1:
            raise SessionError(f"fork_at_step must be >= 1, got {at_step}")

        # Server-owned step index first: exact, durable and lineage-aware
        # (the server walks `parent_channel_id` using the recorded
        # `forked_at_step`). Falls back to the legacy client-side resolution
        # when the step isn't indexed (channels created before the index).
        try:
            resolved = await self._actae.resolve_step(channel_id, at_step)
            if resolved is not None:
                return resolved[0], resolved[1]
        except Exception:
            pass

        # A live session records its own cursors in memory; those are exact
        # for its own steps and avoid a server round-trip. Inherited steps of
        # a fork session have cursor 0 in memory (pre-filled) and must be
        # resolved through the lineage below.
        if at_step <= len(self._cursors):
            local_cursor = self._cursors[at_step - 1]
            if local_cursor > 0:
                return channel_id, local_cursor

        # Walk up to the owning channel.
        owner = channel_id
        for _ in range(MAX_LINEAGE_DEPTH):
            meta = await self._actae.get_channel_metadata(owner)
            exp = (meta.experiment_metadata or {}) if meta else {}
            forked_at_step = int(exp.get("forked_at_step", 0) or 0)
            if forked_at_step == 0 or at_step > forked_at_step:
                break  # this channel recorded step `at_step` itself
            parent = (meta.parent_channel_id or "") if meta else ""
            if not parent or parent == owner:
                break  # no parent to delegate to
            owner = parent

        # Resolve the cursor on the owner.
        cursor: Optional[int] = None
        try:
            owner_meta = await self._actae.get_channel_metadata(owner)
            if owner_meta and owner_meta.experiment_metadata:
                cursors: List[int] = owner_meta.experiment_metadata.get("cursors", [])
                if cursors and at_step <= len(cursors):
                    cursor = cursors[at_step - 1]
        except Exception:
            pass

        if cursor is None:
            # Fallback: replay the owner's events and resolve by position.
            source_events: List[Event] = []
            page_cursor: Optional[int] = None
            while True:
                try:
                    batch = await self._actae.replay(
                        owner, cursor=page_cursor, limit=1000
                    )
                except APIError:
                    break
                source_events.extend(batch)
                if not batch or at_step <= len(source_events):
                    break
                if page_cursor is not None and batch[-1].cursor <= page_cursor:
                    break
                page_cursor = batch[-1].cursor

            source_events = [
                e
                for e in source_events
                if e.event_type not in (
                    "fork.started",
                    "session.started",
                    "session.completed",
                )
            ]
            if not source_events:
                raise SessionError(
                    f"Channel '{owner}' has no events — cannot resolve step {at_step}"
                )
            if at_step > len(source_events):
                raise SessionError(
                    f"Step {at_step} exceeds the {len(source_events)} steps "
                    f"recorded on channel '{owner}'"
                )
            for i in range(1, len(source_events)):
                if source_events[i].cursor <= source_events[i - 1].cursor:
                    raise SessionError(
                        f"Channel '{owner}' has non-monotonic cursors "
                        f"(event {i}: cursor {source_events[i].cursor} <= "
                        f"cursor {source_events[i - 1].cursor}). "
                        f"Position-based step resolution is unreliable for this "
                        f"channel. Fork at a raw cursor instead."
                    )
            cursor = source_events[at_step - 1].cursor

        return owner, cursor

    async def _fork_from_channel(
        self,
        source_channel_id: str,
        at_step: int,
        *,
        boundary_mode: str = "exact",
        manifest: Optional[Dict[str, Any]] = None,
        intervention: Optional[Dict[str, Any]] = None,
        tool_policies: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Fork an existing channel's state into this session's channel.

        Resolves the step → cursor mapping by:
        1. Walking up the fork lineage to the channel that actually *owns*
           the requested step. A fork inherits its first ``forked_at_step``
           steps from its parent, so forking a fork at an inherited step must
           resolve against the ancestor that recorded it — otherwise the
           fork's own event log (which only has its own steps) would map the
           position wrongly.
        2. Loading the owning channel's metadata (``cursors`` list).
        3. Falling back to replay if no ``cursors`` list is available (the
           channel was created outside AgentSession).

        The replay fallback validates cursor monotonicity before trusting the
        position-based mapping.  If validation fails, a clear error is raised
        instead of silently producing wrong results.
        """
        if at_step < 1:
            raise SessionError(f"fork_at_step must be >= 1, got {at_step}")

        # Resolve the owning channel + cursor (walks up the lineage for
        # inherited steps; falls back to replay when no cursors metadata).
        source_channel_id, cursor = await self._resolve_step_owner(source_channel_id, at_step)

        reason = f"Forked from {source_channel_id} at step {at_step} (cursor {cursor})"
        fork_metadata = {
            "session_name": self._name,
            "params": self._params,
            "forked_from": source_channel_id,
            "forked_at_step": at_step,
            "forked_at_cursor": cursor,
            "status": "created",
        }
        if intervention:
            fork_metadata["intervention"] = dict(intervention)

        await self._perform_fork(
            source_channel_id,
            self._name,
            cursor,
            at_step=at_step,
            display_name=self._name,
            reason=reason,
            experiment_metadata=fork_metadata,
            boundary_mode=boundary_mode,
            manifest=manifest,
            tool_policies=tool_policies,
        )
        self._intervention = dict(intervention or {})

        await self._prime_fork_session(source_channel_id, at_step)

    async def _prime_fork_session(
        self, source_channel_id: str, at_step: int
    ) -> None:
        """Prime this fork session so it continues at ``at_step + 1`` with the
        parent's inherited state — the heart of "fork at step 5, refine step 6"
        without re-running steps 1-5.

        - Sets ``_step_count = at_step`` so the next ``step()`` records as
          step ``at_step + 1`` on the fork channel.
        - Loads the fork channel's inherited state snapshot (the server copied
          the parent's state at the fork boundary into the child) and exposes
          it via ``inherited_state`` so the caller can seed the fork's live
          state (steps 1..N's data) before running step N+1.

        The caller's ``state_fn`` is intentionally NOT wrapped: it must
        reflect the fork's evolving live state, and ``inherited_state`` is the
        seed the caller folds into that live state once.
        """
        self._step_count = at_step
        # `_cursors` is indexed by step number (position i == step i+1).
        # The fork inherits steps 1..at_step (no physical events on this
        # channel), so pre-fill those slots; the fork's own steps append
        # their per-channel cursors as they run, keeping step→cursor
        # resolution and nested forks correct.
        self._cursors = [0] * at_step

        inherited: Optional[Dict[str, Any]] = None
        try:
            snap = await self._actae.latest_state(self._name)
            if snap is not None:
                inherited = snap.get("state")
        except Exception:
            inherited = None

        # Never silently prime a fork with contaminated state: when the fork
        # resolved BEYOND the requested boundary (an approximate fallback to
        # the latest state), the inherited snapshot contains data from after
        # the fork point. Refuse to expose it as steps 1..N's data. (requested
        # 0 = "latest" is self-consistent and never contaminated.)
        if (
            self._requested_boundary_cursor
            and self._resolved_boundary_cursor is not None
            and self._resolved_boundary_cursor > self._requested_boundary_cursor
        ):
            logger.warning(
                "fork '%s' inherited cursor %s but requested %s — NOT priming "
                "with the (contaminated) inherited state. inherited_state is None; "
                "set boundary_mode='approximate' explicitly if you intend to continue "
                "from the latest state.",
                self._name,
                self._resolved_boundary_cursor,
                self._requested_boundary_cursor,
            )
            inherited = None

        self._inherited_state = inherited

    #  Step ↔ Cursor mapping 

    def _cursor_for_step(self, step_number: int) -> int:
        """Map a user-facing step number (1-indexed) to a server cursor."""
        if not isinstance(step_number, int):
            raise SessionError(
                f"step_number must be int, got {type(step_number).__name__}"
            )
        if step_number < 1:
            raise SessionError(f"step_number must be >= 1, got {step_number}")

        if not self._cursors:
            raise SessionError(
                f"Session '{self._name}' has no recorded steps yet "
                f"— cannot resolve step {step_number}"
            )

        if step_number > len(self._cursors):
            raise SessionError(
                f"Step {step_number} exceeds the {len(self._cursors)} steps "
                f"recorded on session '{self._name}'"
            )

        return self._cursors[step_number - 1]

    #  Utility 

    def __repr__(self) -> str:
        return (
            f"AgentSession(name={self._name!r}, channel={self._channel_id!r}, "
            f"status={self._status!r}, steps={self._step_count})"
        )
