"""Actae HTTP and WebSocket client.

Usage:
    from actae_client import ActaeClient

    async with ActaeClient(
        api_key="sk-dev-0000000000000000000000",
        endpoint="http://localhost:8002",
    ) as actae:
        event = await actae.record(
            "my-channel", "agent.step",
            {"input": "Hello", "output": "Hi"},
            actor="my-agent",
        )
        print(f"Recorded at cursor {event.cursor}")

    # WebSocket subscription:
    async with ActaeClient(api_key="sk-...", endpoint="http://localhost:8002") as actae:
        actae.on_message(lambda topic, event: print(topic, event))
        await actae.subscribe("my-channel")
        await asyncio.sleep(10)
"""

import asyncio
import json
import logging
import os
import random
import re
import ssl
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Union

import aiohttp

from ._utils import bearer_auth
from .errors import (
    AuthError,
    ActaeConnectionError,
    APIError,
    RateLimitError,
    SnapshotBoundaryError,
    VersionConflictError,
    ConsumerError,
    IdempotencyKeyMismatchError,
    ExecutionNotOwnedError,
    ExecutionNotFoundError,
    IdempotencyConflictError,
    ChannelConflictError,
    CounterfactualBlockedError,
    ForkToolBlockedError,
)
from .deterministic import deterministic_operation_key
from .types import (
    Event,
    HealthStatus,
    ReadinessResult,
    MetricsSnapshot,
    AuthResult,
    UserInfo,
    ChannelMetadata,
    ForkInfo,
    TransitionResult,
    GroupInfo,
    GroupOffset,
    ClaimedWork,
    Wakeup,
    ExecutionInfo,
    ExecutionClaim,
    ForkReceipt,
    _unwrap_sonic,
    ExecutionGroup, ExecutionGroupMember, MemberLease, GroupMessage,
)

logger = logging.getLogger("actae_client")

# Durable human-in-the-loop approval events (see docs/HUMAN_APPROVAL.md).
APPROVAL_REQUESTED = "approval.requested"
APPROVAL_DECIDED = "approval.decided"
APPROVAL_EXPIRED = "approval.expired"


def _jittered_delay(delay: float, jitter_range: float = 0.5) -> float:
    """Decorrelated jitter: ``delay ± jitter_range`` (default ±50%).

    Keeps the exponential backoff shape while spreading simultaneous
    reconnects so a fleet of clients does not hammer the server in a
    thundering herd after an outage. The lower bound is never below
    ``delay * (1 - jitter_range)`` and never below a small floor, so
    retries still make progress.
    """
    low = max(delay * (1.0 - jitter_range), 0.05)
    high = delay * (1.0 + jitter_range)
    return random.uniform(low, high)

# Sentinel pushed into active ``stream()`` queues when the WebSocket drops.
_STREAM_END = object()

_MTLS_HINT = (
    "If your instance enforces mTLS: issue a client certificate on the "
    "server with scripts/issue-client-cert.sh, then place ca.crt, client.crt "
    "and client.key into ~/.actae (or the ACTAE_CONFIG_DIR directory) — the "
    "SDK picks them up automatically — or pass client_cert/client_key/ca_cert "
    "explicitly."
)


def _config_dir() -> Path:
    """The per-user Actae config directory (overridable via ACTAE_CONFIG_DIR)."""
    base = os.environ.get("ACTAE_CONFIG_DIR")
    if base:
        return Path(base)
    return Path.home() / ".actae"


def _discover_mtls_files() -> Optional[Dict[str, str]]:
    """Find the default mTLS identity in the config dir, if present.

    Looks for ``ca.crt`` (trust the server), ``client.crt`` and
    ``client.key`` (this client's identity) in ``~/.actae`` — exactly what
    ``scripts/issue-client-cert.sh --install`` drops there. Returns None
    when any file is missing, so plain-TLS or public-CA setups are
    unaffected.
    """
    cfg = _config_dir()
    ca = cfg / "ca.crt"
    cert = cfg / "client.crt"
    key = cfg / "client.key"
    if all(p.is_file() for p in (ca, cert, key)):
        return {
            "ca_cert": str(ca),
            "client_cert": str(cert),
            "client_key": str(key),
        }
    return None


def _to_rfc3339(run_at: Union[str, datetime]) -> str:
    """Normalize a wake-up schedule time to an RFC 3339 string.

    Accepts either an ISO-8601/RFC 3339 string (passed through) or a
    timezone-aware ``datetime`` (serialized as UTC). Naive datetimes are
    rejected — scheduling in an ambiguous local timezone silently firing at
    the wrong moment is a worse failure than a loud error.
    """
    if isinstance(run_at, str):
        return run_at
    if isinstance(run_at, datetime):
        if run_at.tzinfo is None:
            raise ValueError(
                "run_at datetime must be timezone-aware, e.g. "
                "datetime.now(timezone.utc)"
            )
        return run_at.astimezone(timezone.utc).isoformat()
    raise TypeError(
        f"run_at must be an RFC 3339 string or a timezone-aware datetime, "
        f"got {type(run_at).__name__}"
    )


class _APIFacade:
    """A read-only view over a subset of ``ActaeClient`` methods.

    Each facade delegates to the underlying client instance, so
    ``client.events.record(...)`` is exactly ``client.record(...)``.
    Facades group the API by concern to make the surface discoverable.
    """

    __slots__ = ("_client", "_members")

    def __init__(self, client: "ActaeClient", members: Iterable[str]) -> None:
        object.__setattr__(self, "_client", client)
        object.__setattr__(self, "_members", frozenset(members))

    def __getattr__(self, name: str) -> Any:
        if name in object.__getattribute__(self, "_members"):
            return getattr(object.__getattribute__(self, "_client"), name)
        raise AttributeError(f"{type(self).__name__} has no attribute {name!r}")

    def __dir__(self) -> List[str]:
        return sorted(object.__getattribute__(self, "_members"))

    def __repr__(self) -> str:
        return f"<{type(self).__name__} of {object.__getattribute__(self, '_client')!r}>"


_CHANNEL_ID_RE = re.compile(r"^[A-Za-z0-9._:\-]{1,256}$")


def validate_channel_id(channel_id: str) -> None:
    """Validate a channel id against the shared server grammar (audit item 10).

    Channel ids must be 1..=256 bytes using only ``[A-Za-z0-9._:-]`` — no
    slashes, whitespace, or control characters — so every channel created is
    always addressable as a single URL path segment. Human-readable names
    belong in metadata/``display_name``, not the id.

    Raises:
        ValueError: If ``channel_id`` violates the grammar.
    """
    if not isinstance(channel_id, str) or not _CHANNEL_ID_RE.match(channel_id):
        raise ValueError(
            "invalid channel_id %r — use 1-256 chars of [A-Za-z0-9._:-] "
            "(no slashes or whitespace)" % (channel_id,)
        )


class Capabilities:
    """Parsed key scopes from ``GET /api/v1/auth/capabilities``.

    Mirrors the server's `ApiKeyPermissions` pattern matching so a client can
    ask *before* attempting an operation (selling point 5):
    ``caps.can_read("project-a.session-1")``.
    """

    __slots__ = ("read", "write", "delete", "admin", "auth_method", "key_id", "key_source", "expires_at")

    def __init__(self, data: Dict[str, Any]) -> None:
        perms = data.get("permissions") or {}
        self.read: List[str] = perms.get("read") or []
        self.write: List[str] = perms.get("write") or []
        self.delete: List[str] = perms.get("delete") or []
        self.admin: List[str] = perms.get("admin") or []
        self.auth_method = data.get("auth_method")
        self.key_id = data.get("key_id")
        self.key_source = data.get("key_source")
        self.expires_at = data.get("expires_at")

    @staticmethod
    def _matches(pattern: str, resource: str) -> bool:
        if pattern == "*":
            return True
        if pattern.endswith("*"):
            return resource.startswith(pattern[:-1])
        return pattern == resource

    def can(self, action: str, channel: str) -> bool:
        """True if `channel` is authorized for `action` (read/write/delete/admin)."""
        for pattern in getattr(self, action, ()) or ():
            if self._matches(pattern, channel):
                return True
        # admin patterns grant every action
        for pattern in self.admin:
            if self._matches(pattern, channel):
                return True
        return False

    def can_read(self, channel: str) -> bool:
        return self.can("read", channel)

    def can_write(self, channel: str) -> bool:
        return self.can("write", channel)

    def can_publish(self, channel: str) -> bool:
        return self.can("write", channel)

    def can_fork(self, channel: str) -> bool:
        return self.can("write", channel)


class ActaeClient:
    """Actae client for event recording, replay, and WebSocket messaging.

    This is the primary entry point for interacting with Actae. Use it as an
    async context manager to automatically manage the WebSocket lifecycle:

        async with ActaeClient(api_key="sk-...", endpoint="http://localhost:8002") as actae:
            await actae.record(...)
            await actae.subscribe(...)

    Supports optional mTLS via ``client_cert`` / ``client_key`` / ``ca_cert``,
    WebSocket auto-reconnect, and rate-limit-aware retry.
    """

    @classmethod
    def from_env(cls, **overrides: Any) -> "ActaeClient":
        """Build a client from the standard self-hosted/cloud environment.

        Reads ``ACTAE_URL`` (with ``ACTAE_ENDPOINT`` as a compatibility
        alias), optional ``ACTAE_WS_URL``, and ``ACTAE_API_KEY``. Explicit
        keyword overrides win, which keeps TLS and timeout configuration
        available without manually copying endpoint credentials.
        """
        endpoint = overrides.pop("endpoint", None) or os.environ.get("ACTAE_URL") or os.environ.get("ACTAE_ENDPOINT")
        ws_endpoint = overrides.pop("ws_endpoint", None) or os.environ.get("ACTAE_WS_URL", "")
        api_key = overrides.pop("api_key", None) or os.environ.get("ACTAE_API_KEY")
        if not api_key:
            raise ValueError("ACTAE_API_KEY is required")
        if not endpoint and not ws_endpoint:
            raise ValueError("ACTAE_URL (or ACTAE_WS_URL) is required")
        return cls(
            api_key=api_key,
            endpoint=endpoint or "",
            ws_endpoint=ws_endpoint,
            **overrides,
        )

    def __init__(
        self,
        *,
        api_key: str,
        endpoint: str = "",
        ws_endpoint: str = "",
        ssl_context: Optional[ssl.SSLContext] = None,
        client_cert: Optional[str] = None,
        client_key: Optional[str] = None,
        ca_cert: Optional[str] = None,
        timeout: float = 30.0,
        auto_reconnect: bool = True,
        echo_self: bool = False,
    ) -> None:
        """Create an Actae client.

        At least one of ``endpoint`` or ``ws_endpoint`` is required. If only
        ``endpoint`` is given, the WebSocket endpoint is derived by replacing
        the protocol scheme (``https://`` → ``wss://``, ``http://`` → ``ws://``).

        Args:
            api_key: API key for authentication.
            endpoint: HTTP API base URL (e.g. ``http://localhost:8002``).
            ws_endpoint: WebSocket URL (auto-derived from ``endpoint`` if omitted).
            ssl_context: Pre-configured SSL context (for custom CAs or mTLS).
            client_cert: Path to client certificate file (for mTLS).
            client_key: Path to client private key file (for mTLS).
            ca_cert: Path to CA certificate file (for self-signed certs).
            timeout: Timeout in seconds for HTTP and WebSocket operations.
            auto_reconnect: Whether to automatically reconnect on WebSocket drop.
            echo_self: Deliver events this client publishes to its own
                ``on_message`` callbacks and ``stream()`` iterators. The
                server never echoes a broadcast back to the connection that
                published it, so without this flag a single client that both
                subscribes and publishes never sees its own events. Default
                ``False`` (wire parity with the Go SDK).

        Raises:
            ValueError: If ``api_key`` is empty or neither endpoint is provided.
        """
        if not api_key:
            raise ValueError("api_key is required")
        if not endpoint and not ws_endpoint:
            raise ValueError("at least one of endpoint or ws_endpoint is required")

        self._api_key = api_key
        self._endpoint = endpoint.rstrip("/") if endpoint else ""
        if ws_endpoint:
            self._ws_endpoint = ws_endpoint.rstrip("/")
        elif self._endpoint:
            derived = self._endpoint.replace("https://", "wss://").replace("http://", "ws://")
            # Actae serves WebSocket at /ws; append it when the HTTP endpoint
            # does not already point at it.
            self._ws_endpoint = derived if derived.endswith("/ws") else f"{derived}/ws"
        else:
            self._ws_endpoint = ""
        self._timeout = timeout
        self._auto_reconnect = auto_reconnect
        self._echo_self = echo_self

        # Build SSL context from files if provided, otherwise use passed context or default.
        # When nothing is configured explicitly and the endpoint is TLS, look
        # for the default mTLS identity in the config dir (~/.actae) so the
        # SDK works with zero certificate configuration.
        if not (client_cert or client_key or ca_cert or ssl_context):
            tls_endpoint = (
                self._endpoint.startswith("https://")
                or self._ws_endpoint.startswith("wss://")
            )
            discovered = _discover_mtls_files() if tls_endpoint else None
            if discovered:
                client_cert = discovered["client_cert"]
                client_key = discovered["client_key"]
                ca_cert = discovered["ca_cert"]
            self._mtls_auto_discovered = bool(discovered)
            self._mtls_hint_enabled = tls_endpoint and not discovered
        else:
            self._mtls_auto_discovered = False
            self._mtls_hint_enabled = False

        if client_cert or client_key or ca_cert:
            _ctx = ssl.create_default_context(cafile=ca_cert) if ca_cert else ssl.create_default_context()
            if client_cert:
                _ctx.load_cert_chain(client_cert, client_key)
            self._ssl = _ctx
        else:
            self._ssl = ssl_context

        self._session: Optional[aiohttp.ClientSession] = None
        # HTTP sessions are bound to the event loop they were created on.
        # Keyed by id() of the loop so sync adapters (e.g. the LangGraph
        # checkpointer bridge) can run HTTP calls in a fresh loop without
        # breaking the primary async session.
        self._sessions_by_loop: Dict[int, aiohttp.ClientSession] = {}
        self._ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self._reader_task: Optional[asyncio.Task] = None
        self._reconnect_task: Optional[asyncio.Task] = None
        self._connected = False
        self._authenticated = False
        self._connection_id: Optional[str] = None
        self._subscribed: Dict[str, int] = {}
        self._subscribed_events: Dict[str, List[asyncio.Event]] = {}
        self._close_event: Optional[asyncio.Event] = None
        self._auth_event: Optional[asyncio.Event] = None
        self._auth_error: Optional[str] = None
        self._lock = asyncio.Lock()

        # Ack tracking: the server echoes the publish's request_id in the
        # Ack, so ack delivery is correlated per-publish (audit item 8).
        # A stale ack for a request that already timed out has no pending
        # future and is discarded instead of being stolen by the next
        # publish. `_ack_queue` remains as the FIFO fallback for servers
        # that do not echo request_id.
        self._ack_queue: asyncio.Queue = asyncio.Queue()
        self._pending_acks: Dict[str, "asyncio.Future[Any]"] = {}

        self._on_message_cbs: List[Callable[[str, Event], None]] = []
        self._on_error_cb: Optional[Callable[[str], None]] = None
        self._on_subscribed_cb: Optional[Callable[[str, Optional[int]], None]] = None
        self._on_disconnected_cb: Optional[Callable[[], None]] = None
        self._on_reconnect_cb: Optional[Callable[[], None]] = None

        self._stream_queues: Dict[str, List[asyncio.Queue]] = {}
        self._publish_lock = asyncio.Lock()

        # Per-topic delivered-event watermark, per subscription window. The
        # server registers a connection for live delivery BEFORE running the
        # subscribe-with-cursor catch-up replay, so an event persisted in
        # that window is delivered both in the replay AND live. Cursors on a
        # topic are strictly increasing per connection, so a broadcast at or
        # below this watermark is a duplicate and is dropped. Cleared on
        # every subscribe() so an explicit re-subscribe re-delivers.
        self._delivered_cursor: Dict[str, int] = {}

        self._reconnect_failures: int = 0
        self._max_reconnect_failures: int = 10

        # Namespaced facades — the full API stays on this class; the facades
        # group methods by concern for discoverability:
        #     client.events.record(...)  ==  client.record(...)
        self.events = _APIFacade(self, (
            "record", "replay", "query", "transition", "get_cursor", "latest_cursor",
        ))
        self.state = _APIFacade(self, (
            "save_state", "latest_state", "list_states", "get_state", "delete_state",
        ))
        self.channels = _APIFacade(self, (
            "list_channels", "fork", "get_channel_metadata",
            "list_forks", "get_fork_tree", "update_metadata",
            "diff_states", "decision_trail",
        ))
        self.executions = _APIFacade(self, (
            "claim_execution", "complete_execution", "fail_execution",
            "heartbeat_execution", "cancel_execution", "get_execution",
            "list_executions", "delete_execution",
        ))
        self.groups = _APIFacade(self, (
            "create_group", "list_groups", "delete_group", "join_group",
            "claim_work", "ack_work", "heartbeat", "group_offsets",
        ))
        self.wakeups = _APIFacade(self, (
            "schedule_wakeup", "list_wakeups", "get_wakeup", "cancel_wakeup",
        ))
        self.health = _APIFacade(self, (
            "health_check", "readiness_check", "get_metrics_text", "get_metrics_json",
        ))
        self.auth = _APIFacade(self, (
            "signup", "login", "logout", "get_me", "capabilities",
        ))
        self.ws = _APIFacade(self, (
            "connect", "disconnect", "subscribe", "unsubscribe", "publish",
            "stream", "on_message", "on_error", "on_subscribed",
            "on_disconnected", "on_reconnect",
        ))

    async def __aenter__(self) -> "ActaeClient":
        await self.connect()
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.disconnect()

    # ------------------------------------------------------------------ #
    # Callback registration
    # ------------------------------------------------------------------ #

    def on_message(self, callback: Callable[[str, Event], None]) -> None:
        """Register a callback for received WebSocket messages.

        The callback receives ``(topic: str, event: Event)`` for every
        broadcast message published on a subscribed topic.

        Multiple callbacks may be registered; all are invoked in
        registration order.
        """
        self._on_message_cbs.append(callback)

    def on_error(self, callback: Callable[[str], None]) -> None:
        """Register a callback for WebSocket error messages.

        The callback receives the error message string. Fires on subscription
        errors and server-side error frames.

        Only one callback can be registered at a time.
        """
        self._on_error_cb = callback

    def on_subscribed(self, callback: Callable[[str, Optional[int]], None]) -> None:
        """Register a callback for successful subscription confirmations.

        The callback receives ``(topic: str, cursor: int | None)`` where
        ``cursor`` is the current topic cursor at subscription time.

        Only one callback can be registered at a time.
        """
        self._on_subscribed_cb = callback

    def on_disconnected(self, callback: Callable[[], None]) -> None:
        """Register a callback for WebSocket disconnection events.

        Fires when the WebSocket connection drops (before auto-reconnect).

        Only one callback can be registered at a time.
        """
        self._on_disconnected_cb = callback

    def on_reconnect(self, callback: Callable[[], None]) -> None:
        """Register a callback for successful WebSocket reconnection events.

        Fires after auto-reconnect completes and all topics have been
        resubscribed.

        Only one callback can be registered at a time.
        """
        self._on_reconnect_cb = callback

    # ------------------------------------------------------------------ #
    # HTTP API
    # ------------------------------------------------------------------ #

    async def record(
        self,
        channel_id: str,
        event_type: str,
        payload: Any,
        *,
        actor: str,
        agent_id: Optional[str] = None,
        user_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        operation_id: Optional[str] = None,
        step_number: Optional[int] = None,
        dependencies: Optional[List[Dict[str, str]]] = None,
    ) -> Event:
        """Record a new event on a channel.

        ``POST /api/v1/events/record``

        Args:
            channel_id: Target channel identifier.
            event_type: User-defined event type (e.g. ``"agent.step"``).
            payload: Event payload (must be JSON-serializable).
            actor: Actor identifier (who/what created this event).
            agent_id: Optional agent identifier.
            user_id: Optional user identifier.
            metadata: Optional metadata dict attached to the event.
            operation_id: Optional idempotency key. Retrying with the same
                ``(channel_id, operation_id)`` returns the original persisted
                event instead of inserting a duplicate — pass a stable UUID
                to make request retries safe against lost responses.
            step_number: Optional 1-indexed step number recorded in the
                server-owned step index (``channel_steps``), making step →
                cursor resolution and ``fork_at_step`` exact server-side.

        Returns:
            The persisted ``Event`` with its server-assigned cursor and ID.
        """
        validate_channel_id(channel_id)
        body: Dict[str, Any] = {
            "channel_id": channel_id,
            "event_type": event_type,
            "payload": payload,
            "metadata": {
                "actor": actor,
                "agent_id": agent_id,
                "user_id": user_id,
                "metadata": metadata,
            },
        }
        if operation_id is not None:
            body["operation_id"] = operation_id
        if step_number is not None:
            body["step_number"] = step_number
        if dependencies:
            body["dependencies"] = dependencies
        result = await self._request("POST", "/api/v1/events/record", json=body)
        event = Event.from_record(result["event"])
        # The record response omits metadata/agent_id/user_id; fill them
        # from what we sent so the returned Event is self-consistent
        # (Go SDK parity).
        event.metadata = metadata
        event.agent_id = agent_id
        event.user_id = user_id
        return event

    async def replay(
        self,
        channel_id: str,
        *,
        cursor: Optional[int] = None,
        limit: int = 100,
        event_type: Optional[str] = None,
    ) -> List[Event]:
        """Replay events from a channel, starting at an optional cursor.

        ``GET /api/v1/events/replay/{channel_id}``

        Args:
            channel_id: Channel to replay.
            cursor: Exclusive start cursor — events with a (global) cursor
                strictly greater than this value are returned. If ``None``,
                replays from the beginning. For a page of events *up to and
                including* cursor N, pass ``N - 1``.
            limit: Maximum number of events to return (max 10000).
            event_type: Filter by event type.

        Returns:
            List of ``Event`` objects in cursor order.
        """
        validate_channel_id(channel_id)
        params: Dict[str, str] = {"limit": str(limit)}
        if cursor is not None:
            params["cursor"] = str(cursor)
        if event_type is not None:
            params["event_type"] = event_type
        result = await self._request(
            "GET", f"/api/v1/events/replay/{channel_id}", params=params
        )
        return [Event.from_replay(e) for e in result["events"]]

    async def causal_graph(self, event_id: str, *, max_nodes: int = 1000) -> Dict[str, Any]:
        """Return the bounded transitive causal ancestry for an event."""
        return await self._request("GET", f"/api/v1/events/{event_id}/causal-graph", params={"max_nodes": str(max_nodes)})

    async def query(
        self,
        *,
        channel_ids: Optional[List[str]] = None,
        event_type: Optional[str] = None,
        actor: Optional[str] = None,
        cursor_start: Optional[int] = None,
        cursor_end: Optional[int] = None,
        from_time: Optional[str] = None,
        to_time: Optional[str] = None,
        limit: int = 100,
        offset: Optional[int] = None,
    ) -> List[Event]:
        """Query events across channels with flexible filters.

        ``POST /api/v1/events/query``

        Args:
            channel_ids: Filter by specific channel IDs.
            event_type: Filter by event type.
            actor: Filter by actor identifier.
            cursor_start: Minimum cursor (inclusive).
            cursor_end: Maximum cursor (inclusive).
            from_time: ISO 8601 timestamp lower bound.
            to_time: ISO 8601 timestamp upper bound.
            limit: Maximum events to return (default 100).
            offset: Pagination offset.

        Returns:
            List of matching ``Event`` objects.
        """
        body: Dict[str, Any] = {"limit": limit}
        if channel_ids is not None:
            body["channel_ids"] = channel_ids
        if event_type is not None:
            body["event_type"] = event_type
        if actor is not None:
            body["actor"] = actor
        if cursor_start is not None:
            body["cursor_start"] = cursor_start
        if cursor_end is not None:
            body["cursor_end"] = cursor_end
        if from_time is not None:
            body["from"] = from_time
        if to_time is not None:
            body["to"] = to_time
        if offset is not None:
            body["offset"] = offset
        result = await self._request("POST", "/api/v1/events/query", json=body)
        return [Event.from_query(e) for e in result["events"]]

    async def get_cursor(self, channel_id: str) -> Optional[int]:
        """Get the latest cursor value for a channel.

        ``GET /api/v1/events/cursor/{channel_id}``

        Returns the highest cursor recorded on the channel, or ``None`` if the
        channel has no events.
        """
        validate_channel_id(channel_id)
        params = {}
        result = await self._request(
            "GET", f"/api/v1/events/cursor/{channel_id}", params=params
        )
        return result.get("latest_cursor")

    async def latest_cursor(self, channel_id: str) -> Optional[int]:
        """Alias for ``get_cursor`` — returns the latest cursor for a channel."""
        return await self.get_cursor(channel_id)

    async def save_state(
        self,
        channel_id: str,
        cursor: int,
        state: Any,
        *,
        expected_version: Optional[int] = None,
        expected_cursor: Optional[int] = None,
        operation_id: Optional[str] = None,
        dependencies: Optional[List[Dict[str, str]]] = None,
    ) -> int:
        """Save a cursor-aligned context snapshot for agent resume.

        ``POST /api/v1/state/{channel_id}``

        Each call creates a new immutable version. Returns the assigned
        version number. The state blob is opaque to Actae — the framework
        owns serialization. On restart, call ``latest_state()`` to retrieve.

        Args:
            channel_id: Target channel.
            cursor: Current cursor on the channel (for alignment).
            state: State blob (must be JSON-serializable).
            expected_version: Optimistic-concurrency guard — reject unless
                the channel's latest state version matches. ``0`` means
                "no snapshot exists yet".
            expected_cursor: Optimistic-concurrency guard — reject unless
                the channel's latest state cursor matches.

        Returns:
            The version number of the newly created snapshot.

        Raises:
            VersionConflictError: A guard did not match the latest state.
        """
        validate_channel_id(channel_id)
        body: Dict[str, Any] = {
            "channel_id": channel_id,
            "cursor": cursor,
            "state": state,
        }
        if expected_version is not None:
            body["expected_version"] = expected_version
        if expected_cursor is not None:
            body["expected_cursor"] = expected_cursor
        if operation_id is not None:
            body["operation_id"] = operation_id
        if dependencies:
            body["dependencies"] = dependencies
        result = await self._request("POST", f"/api/v1/state/{channel_id}", json=body)
        return result["version"]

    async def transition(
        self,
        channel_id: str,
        event_type: str,
        payload: Any,
        state: Any,
        *,
        actor: str = "system",
        agent_id: Optional[str] = None,
        user_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        operation_id: Optional[str] = None,
        expected_version: Optional[int] = None,
        expected_cursor: Optional[int] = None,
    ) -> TransitionResult:
        """Atomically persist an event AND its resulting state snapshot.

        ``POST /api/v1/events/transition``

        The event and the state snapshot commit in a single server-side
        transaction — either both exist or neither does. The snapshot's
        cursor equals the event's ``cursor`` (the per-channel cursor).

        Args:
            channel_id: Target channel.
            event_type: Type of the event (e.g. ``"agent.started"``).
            payload: Event payload (must be JSON-serializable).
            state: State snapshot written atomically with the event.
            actor: Event actor label.
            agent_id: Optional agent instance ID.
            user_id: Optional user ID.
            metadata: Optional structured event metadata.
            operation_id: Optional idempotency key. Retrying with the same
                ``(channel_id, operation_id)`` and identical request returns
                the original event + state version instead of writing a
                second transition; a different payload under the same id is
                ``409 idempotency_conflict``. Matches the server contract and
                ``record``/``publish``.
            expected_version: Optimistic-concurrency guard (see ``save_state``).
            expected_cursor: Optimistic-concurrency guard (see ``save_state``).

        Returns:
            ``TransitionResult`` with the persisted ``Event`` and the new
            ``state_version``.

        Raises:
            VersionConflictError: A guard did not match the latest state.
            IdempotencyConflictError: The operation id was reused with a
                different request.
        """
        body: Dict[str, Any] = {
            "channel_id": channel_id,
            "type": event_type,
            "payload": payload,
            "state": state,
            "metadata": {"actor": actor},
        }
        if agent_id is not None:
            body["metadata"]["agent_id"] = agent_id
        if user_id is not None:
            body["metadata"]["user_id"] = user_id
        if metadata:
            body["metadata"]["metadata"] = metadata
        if operation_id is not None:
            body["operation_id"] = operation_id
        if expected_version is not None:
            body["expected_version"] = expected_version
        if expected_cursor is not None:
            body["expected_cursor"] = expected_cursor

        result = await self._request("POST", "/api/v1/events/transition", json=body)
        event = Event.from_record(result["event"])
        return TransitionResult(event=event, state_version=int(result["state_version"]))

    async def latest_state(self, channel_id: str) -> Optional[Dict[str, Any]]:
        """Get the latest saved state snapshot for a channel.

        ``GET /api/v1/state/{channel_id}``

        Returns ``{"cursor": int, "state": ...}`` or ``None`` if no state
        has been saved on this channel.
        """
        validate_channel_id(channel_id)
        params = {}
        result = await self._request(
            "GET", f"/api/v1/state/{channel_id}", params=params
        )
        if result.get("cursor") is None:
            return None
        return {"cursor": result["cursor"], "state": _unwrap_sonic(result["state"])}

    async def list_states(
        self, channel_id: str, *, limit: int = 100, offset: int = 0
    ) -> List[Dict[str, Any]]:
        """List state version history for a channel (metadata only, no blobs).

        ``GET /api/v1/state/{channel_id}/versions``

        Returns entries like ``{"version": 1, "cursor": 42, "timestamp": "..."}``.
        """
        validate_channel_id(channel_id)
        params: Dict[str, str] = {
            "limit": str(limit),
            "offset": str(offset),
        }
        result = await self._request(
            "GET", f"/api/v1/state/{channel_id}/versions", params=params
        )
        return result.get("versions", [])

    async def get_state(
        self, channel_id: str, version: int
    ) -> Optional[Dict[str, Any]]:
        """Get a specific state snapshot by version number.

        ``GET /api/v1/state/{channel_id}/version/{version}``

        Returns ``{"cursor": int, "version": int, "state": ...}`` or ``None``
        if the version does not exist.
        """
        validate_channel_id(channel_id)
        params = {}
        result = await self._request(
            "GET", f"/api/v1/state/{channel_id}/version/{version}", params=params
        )
        if result.get("cursor") is None:
            return None
        return {
            "cursor": result["cursor"],
            "state": result["state"],
            "version": result.get("version"),
        }

    async def delete_state(self, channel_id: str, version: int) -> None:
        """Delete a specific state snapshot version.

        ``DELETE /api/v1/state/{channel_id}/version/{version}``
        """
        validate_channel_id(channel_id)
        await self._request(
            "DELETE", f"/api/v1/state/{channel_id}/version/{version}"
        )

    async def list_channels(self) -> List[str]:
        """List all active channel IDs known to Actae.

        ``GET /api/v1/channels``

        Returns a list of channel ID strings (not metadata — use
        ``get_channel_metadata()`` for details on a specific channel).
        """
        result = await self._request("GET", "/api/v1/channels")
        return result.get("channels", [])

    async def fork(
        self,
        source_channel_id: str,
        new_channel_id: str,
        at_cursor: int = 0,
        *,
        display_name: Optional[str] = None,
        reason: Optional[str] = None,
        experiment_metadata: Optional[Dict[str, Any]] = None,
        operation_id: Optional[str] = None,
        manifest: Optional[Dict[str, Any]] = None,
        tool_policies: Optional[Dict[str, Any]] = None,
        expected_version: Optional[int] = None,
        expected_cursor: Optional[int] = None,
    ) -> ForkReceipt:
        """Fork a channel at a cursor into a new experiment fork.

        ``POST /api/v1/channels/fork``

        Creates a logical fork (fork) from a source channel at a specific
        cursor point.

        Boundary semantics:
        - ``at_cursor=0`` forks from the channel's **latest saved state**
          (the child starts empty when the channel has no state).
        - ``at_cursor>0`` forks from the **latest snapshot whose
          cursor <= at_cursor**. If no snapshot exists at or before that
          cursor, ``SnapshotBoundaryError`` is raised.

        The child's first event is a ``fork.started`` lineage marker
        (``cursor=1``) whose ``depends_on`` points at the parent's
        boundary event.

        Idempotency: pass the same ``operation_id`` (or reuse
        ``new_channel_id``) to replay a fork — the server returns the
        original result without duplicating state or events. When omitted,
        a UUID is generated automatically per call.

        Optimistic guard: ``expected_version`` / ``expected_cursor`` make
        "fork latest" mean "latest as of the state I observed" — the fork
        is rejected with ``VersionConflictError`` if the source advanced.

        Returns an immutable :class:`ForkReceipt` carrying the requested and
        resolved boundaries, the source state version + SHA-256 fingerprint,
        restorability and the reproducibility grade.

        Raises:
            SnapshotBoundaryError: ``at_cursor > 0`` and no saved snapshot
                exists at or before it.
            IdempotencyConflictError: the same ``operation_id`` was used
                with a different request.
            ChannelConflictError: ``new_channel_id`` already exists under a
                different source or boundary.
            VersionConflictError: an optimistic guard was supplied and the
                source state advanced.
        """
        validate_channel_id(source_channel_id)
        validate_channel_id(new_channel_id)
        payload: Dict[str, Any] = {
            "source_channel_id": source_channel_id,
            "new_channel_id": new_channel_id,
            "at_cursor": at_cursor,
            "operation_id": operation_id or str(uuid.uuid4()),
        }
        if display_name:
            payload["display_name"] = display_name
        if reason:
            payload["reason"] = reason
        if experiment_metadata:
            payload["experiment_metadata"] = experiment_metadata
        if manifest:
            payload["manifest"] = manifest
        if tool_policies:
            payload["tool_policies"] = tool_policies
        if expected_version is not None:
            payload["expected_version"] = expected_version
        if expected_cursor is not None:
            payload["expected_cursor"] = expected_cursor
        data = await self._request("POST", "/api/v1/channels/fork", json=payload)
        return ForkReceipt.from_dict(data)

    async def get_fork_receipt(self, channel_id: str) -> ForkReceipt:
        """Fetch the immutable fork receipt for a channel.

        ``GET /api/v1/channels/{channel_id}/receipt``

        The receipt is the provenance record for the fork: source identity,
        requested/resolved boundary, source state version + SHA-256
        fingerprint, restorability and reproducibility grade.
        """
        data = await self._request("GET", f"/api/v1/channels/{channel_id}/receipt")
        return ForkReceipt.from_dict(data)

    async def resolve_step(
        self, channel_id: str, step_number: int
    ) -> Optional[tuple[str, int, Optional[str]]]:
        """Resolve a step number to its owning channel + cursor via the
        server-owned step index.

        ``GET /api/v1/channels/{channel_id}/steps/{step_number}``

        Walks the fork lineage server-side: a fork inherits its first
        ``forked_at_step`` steps from its parent, so step N of a fork
        resolves against the ancestor that recorded it.

        Returns ``(owner_channel, cursor, event_id)`` or ``None`` when the
        step is not indexed (e.g. a channel created before the index existed
        — callers fall back to replay-based resolution).
        """
        try:
            data = await self._request(
                "GET", f"/api/v1/channels/{channel_id}/steps/{step_number}"
            )
            return (data["channel_id"], data["cursor"], data.get("event_id"))
        except APIError as e:
            if e.status_code == 404:
                return None
            raise

    async def latest_step_number(self, channel_id: str) -> Optional[int]:
        """Highest step number recorded on a channel (0 = none).

        ``GET /api/v1/channels/{channel_id}/steps``

        O(1) via the server-owned step index. Used by ``AgentSession.resume``
        to recover true progress after a hard process death. Returns ``None``
        when the endpoint is unavailable (older server), so callers can fall
        back to replay-based recovery.
        """
        try:
            data = await self._request(
                "GET", f"/api/v1/channels/{channel_id}/steps"
            )
            return int(data.get("last_step_number", 0))
        except APIError as e:
            if e.status_code == 404:
                return None
            raise

    # ── Experiment platform (Phase 9) ──────────────────────────────────

    async def create_experiment(
        self,
        name: str,
        *,
        description: Optional[str] = None,
        baseline_channel_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Create an experiment group (baseline + variants).

        ``POST /api/v1/experiments``
        """
        payload: Dict[str, Any] = {"name": name}
        if description:
            payload["description"] = description
        if baseline_channel_id:
            payload["baseline_channel_id"] = baseline_channel_id
        return await self._request("POST", "/api/v1/experiments", json=payload)

    async def list_experiments(self) -> List[Dict[str, Any]]:
        """List experiment groups. ``GET /api/v1/experiments``"""
        data = await self._request("GET", "/api/v1/experiments")
        return data if isinstance(data, list) else data.get("experiments", [])

    async def get_experiment(self, group_id: str) -> Dict[str, Any]:
        """Get an experiment group with members. ``GET /api/v1/experiments/{id}``"""
        return await self._request("GET", f"/api/v1/experiments/{group_id}")

    async def add_experiment_member(
        self,
        group_id: str,
        channel_id: str,
        *,
        role: str = "variant",
        declared_delta: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Add a fork to an experiment group.

        ``POST /api/v1/experiments/{group_id}/members``
        """
        payload: Dict[str, Any] = {"channel_id": channel_id, "role": role}
        if declared_delta:
            payload["declared_delta"] = declared_delta
        return await self._request(
            "POST", f"/api/v1/experiments/{group_id}/members", json=payload
        )

    async def rank_experiment(self, group_id: str) -> Dict[str, Any]:
        """Rank an experiment's members by result score (desc, nulls last).

        ``GET /api/v1/experiments/{group_id}/rank``
        """
        return await self._request("GET", f"/api/v1/experiments/{group_id}/rank")

    # ── Fork outcomes / promotion / GC (Phases 9, 11) ────────────────

    async def set_outcome(
        self,
        channel_id: str,
        outcome: str,
        *,
        score: Optional[float] = None,
    ) -> Dict[str, Any]:
        """Record a fork outcome (promoted | rejected | inconclusive |
        crashed) and an optional numeric result score.

        ``PATCH /api/v1/channels/{channel_id}/outcome``
        """
        payload: Dict[str, Any] = {"outcome": outcome}
        if score is not None:
            payload["score"] = score
        return await self._request(
            "PATCH", f"/api/v1/channels/{channel_id}/outcome", json=payload
        )

    async def promote_channel(self, channel_id: str) -> Dict[str, Any]:
        """Promote a winning fork into its parent (append-only merge).

        ``POST /api/v1/channels/{channel_id}/promote``
        """
        return await self._request("POST", f"/api/v1/channels/{channel_id}/promote")

    async def delete_channel(self, channel_id: str, recursive: bool = False) -> Dict[str, Any]:
        """Soft-delete a channel (optionally the whole subtree) and purge its
        rows. ``DELETE /api/v1/channels/{channel_id}?recursive=true|false``
        """
        params = {"recursive": "true" if recursive else "false"}
        return await self._request(
            "DELETE", f"/api/v1/channels/{channel_id}", params=params
        )

    # ── "What changed?" comparison (Phase 10) ──────────────────────────

    async def compare_channels(
        self, left_channel_id: str, right_channel_id: str
    ) -> Dict[str, Any]:
        """Combined "what changed?" comparison between two forks.

        ``GET /api/v1/channels/compare?left={left}&right={right}``

        Returns the state diff plus the fork-manifest diff, tool-call diff,
        output diff and latency/token/cost metrics delta.
        """
        params = {"left": left_channel_id, "right": right_channel_id}
        return await self._request("GET", "/api/v1/channels/compare", params=params)

    async def diff_states(self, left_channel_id: str, right_channel_id: str) -> Dict[str, Any]:
        """Structural diff of two channels' latest saved states.

        ``GET /api/v1/channels/diff?left={left}&right={right}``

        Walks both JSON states recursively and reports exactly what each
        fork knew that the other did not at their point of divergence —
        the counterfactual-debugging primitive: fork two fixes from a
        failing run, then diff their states to see which one fixed it.

        Returns::

            {
              "left_channel_id": ...,
              "right_channel_id": ...,
              "left":  {"channel_id": ..., "cursor": ..., "state": ...},
              "right": {"channel_id": ..., "cursor": ..., "state": ...},
              "common": {"channel_id": <lca>, ...} | None,
              "left_diverged_at_cursor": int | None,
              "right_diverged_at_cursor": int | None,
              "entries": [
                {"path": ["messages", "0", "role"], "kind": "added"|"removed"|"changed",
                 "left": ..., "right": ...},
                ...
              ],
              "truncated": bool,
              "entry_count_total": int,
              "max_entries": int,
            }

        ``kind`` convention: ``added`` = right-only, ``removed`` = left-only,
        ``changed`` = present on both with different values. ``common`` is
        the lowest common ancestor state when both channels share lineage,
        else ``None``. ``left_diverged_at_cursor`` /
        ``right_diverged_at_cursor`` are the per-side fork boundaries where
        each fork departed the shared chain (``None`` when a side IS the
        ancestor). Siblings forked at different cursors are asymmetric and
        both values are reported. ``truncated`` is true when ``entries`` is
        capped at ``max_entries`` — never trust a diff as complete without
        checking it.

        Args:
            left_channel_id: First channel (the baseline).
            right_channel_id: Second channel (the experiment).

        Returns:
            The structured diff dict (see above).
        """
        validate_channel_id(left_channel_id)
        validate_channel_id(right_channel_id)
        params = {"left": left_channel_id, "right": right_channel_id}
        data = await self._request("GET", "/api/v1/channels/diff", params=params)
        data["left"] = _unwrap_sonic(data.get("left"))
        data["right"] = _unwrap_sonic(data.get("right"))
        if data.get("common") is not None:
            data["common"] = _unwrap_sonic(data["common"])
        for entry in data.get("entries", []):
            entry["left"] = _unwrap_sonic(entry.get("left"))
            entry["right"] = _unwrap_sonic(entry.get("right"))
        return data

    async def decision_trail(self, channel_id: str) -> Dict[str, Any]:
        """Get the full decision trail for a channel (lineage as audit).

        ``GET /api/v1/channels/{channel_id}/trail``

        Returns the lineage chain back to the ``origin_run_id`` root run,
        the boundary state the channel forked from, and the idempotent
        tool-execution ledger rows that happened on this channel. This is
        the compliance surface: every decision in a run traces back to a
        fork point, with the tool calls that produced each state.

        Returns::

            {
              "channel_id": ...,
              "origin_run_id": ...,
              "ancestry": [
                {"channel_id": ..., "parent_channel_id": ...|None,
                 "forked_at_cursor": int, "display_name": ...|None, "reason": ...|None},
                ...
              ],
              "boundary": {"channel_id": ..., "cursor": int, "state": ...} | None,
              "executions": [
                {"id": ..., "channel_id": ..., "key_name": ..., "tool_name": ...,
                 "status": ..., "params": ..., "result": ..., "error": ...,
                 "started_cursor": ..., "completed_cursor": ...},
                ...
              ],
            }

        Args:
            channel_id: The channel whose decision trail to fetch.

        Returns:
            The decision trail dict (see above).
        """
        validate_channel_id(channel_id)
        data = await self._request("GET", f"/api/v1/channels/{channel_id}/trail")
        if data.get("boundary") is not None:
            data["boundary"] = _unwrap_sonic(data["boundary"])
        for execution in data.get("executions", []):
            execution["params"] = _unwrap_sonic(execution.get("params"))
            execution["result"] = _unwrap_sonic(execution.get("result"))
            execution["error"] = _unwrap_sonic(execution.get("error"))
        return data

    async def get_channel_metadata(self, channel_id: str) -> Optional[ChannelMetadata]:
        """Get metadata for a channel.

        ``GET /api/v1/channels/{channel_id}/metadata``

        Returns ``None`` if the channel does not exist (HTTP 404).
        """
        validate_channel_id(channel_id)
        try:
            data = await self._request("GET", f"/api/v1/channels/{channel_id}/metadata")
            return self._parse_channel_metadata(data)
        except APIError as e:
            if e.status_code == 404:
                return None
            raise

    async def list_forks(self, channel_id: str) -> List[ChannelMetadata]:
        """List direct children (forks) of a channel.

        ``GET /api/v1/channels/{channel_id}/forks``

        Returns metadata for each channel that was forked directly from the
        given channel.
        """
        validate_channel_id(channel_id)
        data = await self._request("GET", f"/api/v1/channels/{channel_id}/forks")
        return [
            self._parse_channel_metadata(b) for b in data.get("forks", [])
        ]

    @staticmethod
    def _parse_channel_metadata(data: Dict[str, Any]) -> ChannelMetadata:
        """Parse a channel-metadata dict (from /fork, /metadata, /forks or
        /metadata PUT) into a :class:`ChannelMetadata`, unwrapping sonic-rs
        JSON blobs for ``experiment_metadata`` and ``manifest``.
        """
        return ChannelMetadata(**{
            **data,
            "experiment_metadata": _unwrap_sonic(data.get("experiment_metadata")),
            "manifest": _unwrap_sonic(data.get("manifest")),
            "tool_policies": _unwrap_sonic(data.get("tool_policies")),
            "resolved_cursor": data.get(
                "resolved_cursor",
                data.get("resolved_state_cursor", 0),
            ),
        })

    async def get_fork_tree(self, root_channel_id: str) -> ForkInfo:
        """Get the full execution tree rooted at a channel.

        ``GET /api/v1/channels/{root_channel_id}/fork-tree``

        Returns a recursive ``ForkInfo`` structure representing all forks
        and their descendants. Useful for visualizing experiment lineages.
        """
        validate_channel_id(root_channel_id)
        data = await self._request("GET", f"/api/v1/channels/{root_channel_id}/fork-tree")
        return self._parse_fork_info(data)

    # ── Idempotent Tool Executions ──────────────────────────────────

    async def claim_execution(
        self,
        channel_id: str,
        key_name: str,
        tool_name: str,
        params: Optional[Any] = None,
        *,
        dedup_fields: Optional[List[str]] = None,
        lease_seconds: Optional[int] = None,
        emit_replay_event: bool = False,
        actor: Optional[str] = None,
    ) -> ExecutionClaim:
        """Claim an idempotent tool execution (create-or-replay).

        ``POST /api/v1/executions/claim``

        The idempotency unit is ``(channel_id, key_name)``. A completed
        execution with a matching request hash replays its persisted result
        (``ExecutionClaim.status == "replayed"``) without re-running the
        tool. A mismatch raises ``IdempotencyKeyMismatchError``.

        Args:
            channel_id: Channel the execution belongs to.
            key_name: Client-chosen idempotency key, stable across retries.
            tool_name: Logical tool name (e.g. ``"charge_customer"``).
            params: Tool input parameters (JSON-serializable).
            dedup_fields: Restrict request identity to these top-level
                ``params`` keys, excluding volatile fields (timestamps,
                nonces, attempt ids) from the request hash.
            lease_seconds: Lease duration while running (default 60, max 86400).
            emit_replay_event: Record a one-time ``tool.replayed`` marker
                event when a completed execution is replayed.
            actor: Actor label stamped on tool lifecycle events.

        Returns:
            ``ExecutionClaim`` with ``status`` of ``claimed`` / ``replayed`` /
            ``reclaimed`` / ``in_progress``. When ``claimed``/``reclaimed``,
            pass the returned ``claim_token`` to ``complete_execution`` /
            ``fail_execution`` / ``heartbeat_execution``.
        """
        validate_channel_id(channel_id)
        body: Dict[str, Any] = {
            "channel_id": channel_id,
            "key_name": key_name,
            "tool_name": tool_name,
        }
        if params is not None:
            body["params"] = params
        if dedup_fields is not None:
            body["dedup_fields"] = dedup_fields
        if lease_seconds is not None:
            body["lease_seconds"] = lease_seconds
        if emit_replay_event:
            body["emit_replay_event"] = True
        if actor is not None:
            body["actor"] = actor
        data = await self._request("POST", "/api/v1/executions/claim", json=body)
        return ExecutionClaim.from_response(data)

    async def complete_execution(
        self,
        execution_id: str,
        claim_token: Optional[str] = None,
        result: Optional[Any] = None,
    ) -> ExecutionInfo:
        """Complete a claimed execution with its persisted result.

        ``POST /api/v1/executions/{execution_id}/complete``

        Ownership is enforced via the ``claim_token`` returned by
        ``claim_execution``; a stale token raises
        ``ExecutionNotOwnedError``. A duplicate complete (retry after an
        ambiguous failure) is idempotent and returns the stored execution.
        """
        body: Dict[str, Any] = {}
        if claim_token is not None:
            body["claim_token"] = claim_token
        if result is not None:
            body["result"] = result
        data = await self._request(
            "POST", f"/api/v1/executions/{execution_id}/complete", json=body
        )
        return ExecutionInfo.from_response(data["execution"])

    async def fail_execution(
        self,
        execution_id: str,
        message: str,
        *,
        claim_token: Optional[str] = None,
        error_type: Optional[str] = None,
        stack: Optional[str] = None,
    ) -> ExecutionInfo:
        """Mark a claimed execution failed with a structured error.

        ``POST /api/v1/executions/{execution_id}/fail``

        Emits a ``tool.failed`` lifecycle event carrying the error. A stale
        ``claim_token`` raises ``ExecutionNotOwnedError``; a duplicate fail
        is idempotent.
        """
        body: Dict[str, Any] = {"message": message}
        if claim_token is not None:
            body["claim_token"] = claim_token
        if error_type is not None:
            body["error_type"] = error_type
        if stack is not None:
            body["stack"] = stack
        data = await self._request(
            "POST", f"/api/v1/executions/{execution_id}/fail", json=body
        )
        return ExecutionInfo.from_response(data["execution"])

    async def heartbeat_execution(
        self,
        execution_id: str,
        claim_token: Optional[str] = None,
        lease_seconds: Optional[int] = None,
    ) -> ExecutionInfo:
        """Extend a running execution's lease.

        ``POST /api/v1/executions/{execution_id}/heartbeat``

        Call periodically while the tool is still working, before the lease
        (default 60s) lapses. A stale token raises ``ExecutionNotOwnedError``.
        """
        body: Dict[str, Any] = {}
        if claim_token is not None:
            body["claim_token"] = claim_token
        if lease_seconds is not None:
            body["lease_seconds"] = lease_seconds
        data = await self._request(
            "POST", f"/api/v1/executions/{execution_id}/heartbeat", json=body
        )
        return ExecutionInfo.from_response(data["execution"])

    async def cancel_execution(
        self,
        execution_id: str,
        claim_token: Optional[str] = None,
    ) -> ExecutionInfo:
        """Cancel a running execution without storing a result.

        ``POST /api/v1/executions/{execution_id}/cancel``

        Marks the execution ``cancelled`` (no lifecycle event emitted). A
        later ``claim_execution`` with the same key reclaims it.
        """
        body: Dict[str, Any] = {}
        if claim_token is not None:
            body["claim_token"] = claim_token
        data = await self._request(
            "POST", f"/api/v1/executions/{execution_id}/cancel", json=body
        )
        return ExecutionInfo.from_response(data["execution"])

    async def get_execution(self, execution_id: str) -> ExecutionInfo:
        """Fetch a single execution by id.

        ``GET /api/v1/executions/{execution_id}``

        Raises ``ExecutionNotFoundError`` when the execution does not exist.
        """
        data = await self._request("GET", f"/api/v1/executions/{execution_id}")
        return ExecutionInfo.from_response(data["execution"])

    async def list_executions(
        self, channel_id: str, limit: int = 50
    ) -> List[ExecutionInfo]:
        """List executions for a channel, newest first.

        ``GET /api/v1/executions?channel_id={channel_id}&limit={limit}``

        Args:
            channel_id: Channel to list executions for.
            limit: Maximum number of executions (1–100).
        """
        data = await self._request(
            "GET",
            "/api/v1/executions",
            params={"channel_id": channel_id, "limit": limit},
        )
        return [ExecutionInfo.from_response(e) for e in data.get("executions", [])]

    async def delete_execution(self, execution_id: str) -> bool:
        """Delete an execution record.

        ``DELETE /api/v1/executions/{execution_id}``

        Returns ``True`` when the record was removed; raises
        ``ExecutionNotFoundError`` when it did not exist.
        """
        await self._request("DELETE", f"/api/v1/executions/{execution_id}")
        return True

    def _parse_fork_info(self, data: dict) -> ForkInfo:
        """Recursively parse fork tree JSON."""
        return ForkInfo(
            channel_id=data["channel_id"],
            display_name=data.get("display_name"),
            reason=data.get("reason"),
            forked_at_cursor=data.get("forked_at_cursor", 0),
            event_count=data.get("event_count", 0),
            latest_cursor=data.get("latest_cursor"),
            children=[self._parse_fork_info(c) for c in data.get("children", [])],
        )

    async def update_metadata(
        self,
        channel_id: str,
        *,
        display_name: Optional[str] = None,
        reason: Optional[str] = None,
        experiment_metadata: Optional[Dict[str, Any]] = None,
    ) -> ChannelMetadata:
        """Update channel metadata (display_name, reason, experiment_metadata).

        ``PUT /api/v1/channels/{channel_id}/metadata``

        Only specified fields are updated; omitted fields retain their
        existing values.
        """
        validate_channel_id(channel_id)
        payload: Dict[str, Any] = {"channel_id": channel_id}
        if display_name is not None:
            payload["display_name"] = display_name
        if reason is not None:
            payload["reason"] = reason
        if experiment_metadata is not None:
            payload["experiment_metadata"] = experiment_metadata
        data = await self._request(
            "PUT", f"/api/v1/channels/{channel_id}/metadata", json=payload
        )
        return self._parse_channel_metadata(data)

    async def health_check(self) -> HealthStatus:
        """Check the health of the Actae server.

        ``GET /healthz``

        Returns component-level health status.
        """
        result = await self._request("GET", "/healthz")
        return HealthStatus.from_response(result)

    async def readiness_check(self) -> ReadinessResult:
        """Check whether the Actae server is ready to accept traffic.

        ``GET /readyz``

        Returns database and capacity readiness indicators.
        """
        result = await self._request("GET", "/readyz")
        return ReadinessResult.from_response(result)

    async def get_metrics_text(self) -> str:
        """Get Prometheus-format metrics from the server.

        ``GET /metrics``

        Returns raw Prometheus text format.
        """
        result = await self._request("GET", "/metrics")
        return result

    async def get_metrics_json(self) -> MetricsSnapshot:
        """Get structured JSON metrics from the server.

        ``GET /metrics.json``

        Returns a ``MetricsSnapshot`` with WebSocket connection stats,
        message throughput, and back-pressure data.
        """
        result = await self._request("GET", "/metrics.json")
        return MetricsSnapshot.from_response(result)

    # ------------------------------------------------------------------ #
    # Auth API (signup / login / logout / me)
    # ------------------------------------------------------------------ #

    async def signup(
        self,
        email: str,
        password: str,
        name: Optional[str] = None,
    ) -> AuthResult:
        """Create a new user account.

        ``POST /api/v1/auth/signup``

        Args:
            email: User email address.
            password: User password.
            name: Optional display name.

        Returns:
            ``AuthResult`` with user profile and JWT token.
        """
        body: Dict[str, Any] = {
            "email": email,
            "password": password,
        }
        if name:
            body["name"] = name
        result = await self._request("POST", "/api/v1/auth/signup", json=body)
        return AuthResult.from_response(result)

    async def login(self, email: str, password: str) -> AuthResult:
        """Authenticate with email and password.

        ``POST /api/v1/auth/login``

        Returns an ``AuthResult`` containing a JWT token for subsequent
        authenticated requests (e.g., ``get_me()``, ``logout()``).
        """
        body: Dict[str, Any] = {
            "email": email,
            "password": password,
        }
        result = await self._request("POST", "/api/v1/auth/login", json=body)
        return AuthResult.from_response(result)

    async def logout(self, jwt_token: str) -> None:
        """Invalidate a JWT token (server-side logout).

        ``POST /api/v1/auth/logout``

        Requires a valid JWT token in the Authorization header.
        """
        headers = bearer_auth(jwt_token)
        await self._request("POST", "/api/v1/auth/logout", headers=headers)

    async def get_me(self, jwt_token: str) -> UserInfo:
        """Get the current user's profile.

        ``GET /api/v1/auth/me``

        Requires a valid JWT token from ``login()``.
        """
        headers = bearer_auth(jwt_token)
        result = await self._request("GET", "/api/v1/auth/me", headers=headers)
        return UserInfo.from_dict(result.get("user", {}))

    async def capabilities(self) -> Capabilities:
        """Return the caller's parsed key scopes.

        ``GET /api/v1/auth/capabilities``

        Lets a least-privilege client ask *before* acting (selling point 5):

            caps = await client.capabilities()
            if caps.can_publish("run-123"):
                await client.publish("run-123", {...})

        The returned :class:`Capabilities` mirrors the server's
        ``ApiKeyPermissions`` pattern matching (``*``, trailing ``*`` prefix,
        exact), so ``can_read/can_write/can_publish/can_fork`` are a faithful
        local preview of server-side authorization.

        Raises:
            ActaeConnectionError: Transport error.
            APIError: Non-2xx response (e.g. 401 without valid credentials).
        """
        result = await self._request("GET", "/api/v1/auth/capabilities")
        return Capabilities(result)

    # ------------------------------------------------------------------ #
    # Consumer groups (durable workers)
    # ------------------------------------------------------------------ #

    async def create_group(
        self,
        group_id: str,
        channel_id: str,
        *,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> GroupInfo:
        """Create a durable consumer group bound to a channel.

        ``POST /api/v1/groups``

        Consumers join the group, claim event batches (at-least-once,
        lease-based), and ack processed work. Offsets and leases are
        persisted — a worker restart resumes from its last acked offset.
        """
        validate_channel_id(channel_id)
        body: Dict[str, Any] = {"group_id": group_id, "channel_id": channel_id}
        if metadata:
            body["metadata"] = metadata
        data = await self._request("POST", "/api/v1/groups", json=body)
        return GroupInfo.from_response(data)

    async def list_groups(self, *, channel_id: Optional[str] = None) -> List[GroupInfo]:
        """List consumer groups, optionally filtered by channel.

        ``GET /api/v1/groups?channel_id=...``
        """
        params: Dict[str, Any] = {}
        if channel_id:
            params["channel_id"] = channel_id
        data = await self._request("GET", "/api/v1/groups", params=params)
        return [GroupInfo.from_response(g) for g in data.get("groups", [])]

    async def delete_group(self, group_id: str) -> None:
        """Delete a consumer group (cascades consumers and offsets).

        ``DELETE /api/v1/groups/{group_id}``
        """
        await self._request("DELETE", f"/api/v1/groups/{group_id}")

    async def join_group(
        self,
        group_id: str,
        consumer_id: str,
        *,
        lease_seconds: int = 60,
    ) -> GroupOffset:
        """Register (or renew) a consumer's lease and durable offset row.

        ``POST /api/v1/groups/{group_id}/join``

        Args:
            group_id: The consumer group to join.
            consumer_id: Unique consumer identifier (stable across restarts).
            lease_seconds: Lease duration; the consumer must heartbeat or
                re-join before it lapses or its claims become reclaimable.
        """
        data = await self._request(
            "POST",
            f"/api/v1/groups/{group_id}/join",
            json={"consumer_id": consumer_id, "lease_seconds": lease_seconds},
        )
        return GroupOffset.from_response(data)

    async def claim_work(
        self,
        group_id: str,
        consumer_id: str,
        *,
        limit: int = 100,
    ) -> ClaimedWork:
        """Claim a batch of events for a consumer.

        ``POST /api/v1/groups/{group_id}/work``

        At-least-once semantics: the batch is reserved for this consumer
        until its lease expires. Commit progress with ``ack_work``; the
        durable offset advances only on ack, so crashed workers' unacked
        work is redelivered after lease expiry.

        Raises:
            ConsumerError: The consumer has no active lease (join first).
        """
        data = await self._request(
            "POST",
            f"/api/v1/groups/{group_id}/work",
            json={"consumer_id": consumer_id, "limit": limit},
        )
        return ClaimedWork.from_response(data)

    async def ack_work(self, group_id: str, consumer_id: str, cursor: int) -> None:
        """Acknowledge processed work up to and including ``cursor``.

        ``POST /api/v1/groups/{group_id}/ack``

        Advances the consumer's durable committed offset. The consumer must
        hold an active lease.

        ``cursor`` is a *channel* cursor (see ``GroupOffset``) and commits a
        watermark: it must be the highest **contiguously processed** cursor.
        Acking a higher cursor than you actually processed skips the
        intervening events permanently — they will not be redelivered.
        """
        await self._request(
            "POST",
            f"/api/v1/groups/{group_id}/ack",
            json={"consumer_id": consumer_id, "cursor": cursor},
        )

    async def heartbeat(
        self,
        group_id: str,
        consumer_id: str,
        *,
        lease_seconds: int = 60,
    ) -> None:
        """Extend a consumer's lease.

        ``POST /api/v1/groups/{group_id}/heartbeat``

        Raises:
            ConsumerError: The consumer is unknown or its lease lapsed
                (re-join to reactivate).
        """
        await self._request(
            "POST",
            f"/api/v1/groups/{group_id}/heartbeat",
            json={"consumer_id": consumer_id, "lease_seconds": lease_seconds},
        )

    async def group_offsets(self, group_id: str) -> List[GroupOffset]:
        """Per-consumer durable offsets for a group.

        ``GET /api/v1/groups/{group_id}/offsets``
        """
        data = await self._request("GET", f"/api/v1/groups/{group_id}/offsets")
        return [GroupOffset.from_response(o) for o in data.get("offsets", [])]

    # ------------------------------------------------------------------ #
    # Execution groups (durable distributed agent coordination)
    # ------------------------------------------------------------------ #

    async def create_execution_group(self, group_id: str, *, metadata: Optional[Dict[str, Any]] = None) -> ExecutionGroup:
        validate_channel_id(group_id)
        data = await self._request("POST", "/api/v1/execution-groups", json={"group_id": group_id, "metadata": metadata or {}})
        return ExecutionGroup.from_response(data)

    def execution_group(self, group_id: str) -> "GroupSession":
        """Return the high-level durable coordination session for ``group_id``."""
        from .groups import GroupSession
        validate_channel_id(group_id)
        return GroupSession(self, group_id)

    def group(self, group_id: str) -> "GroupSession":
        """Alias for :meth:`execution_group` matching the protocol vocabulary."""
        return self.execution_group(group_id)

    async def list_execution_groups(self) -> List[ExecutionGroup]:
        data = await self._request("GET", "/api/v1/execution-groups")
        return [ExecutionGroup.from_response(v) for v in data.get("groups", [])]

    async def add_execution_group_member(self, group_id: str, member_id: str, channel_id: str, *, role: Optional[str] = None, metadata: Optional[Dict[str, Any]] = None) -> ExecutionGroupMember:
        validate_channel_id(member_id); validate_channel_id(channel_id)
        body: Dict[str, Any] = {"member_id": member_id, "channel_id": channel_id, "metadata": metadata or {}}
        if role is not None: body["role"] = role
        data = await self._request("POST", f"/api/v1/execution-groups/{group_id}/members", json=body)
        return ExecutionGroupMember.from_response(data)

    async def execution_group_members(self, group_id: str) -> List[ExecutionGroupMember]:
        data = await self._request("GET", f"/api/v1/execution-groups/{group_id}/members")
        return [ExecutionGroupMember.from_response(v) for v in data.get("members", [])]

    async def claim_member(self, group_id: str, member_id: str, owner_id: str, *, lease_seconds: int = 60) -> MemberLease:
        data = await self._request("POST", f"/api/v1/execution-groups/{group_id}/members/{member_id}/claim", json={"owner_id": owner_id, "lease_seconds": lease_seconds})
        return MemberLease.from_response(data)

    async def heartbeat_member(self, group_id: str, member_id: str, owner_id: str, generation: int, *, lease_seconds: int = 60) -> MemberLease:
        data = await self._request("POST", f"/api/v1/execution-groups/{group_id}/members/{member_id}/heartbeat", json={"owner_id": owner_id, "generation": generation, "lease_seconds": lease_seconds})
        return MemberLease.from_response(data)

    async def release_member(self, group_id: str, member_id: str, owner_id: str, generation: int) -> None:
        await self._request("POST", f"/api/v1/execution-groups/{group_id}/members/{member_id}/release", json={"owner_id": owner_id, "generation": generation})

    async def send_group_message(self, group_id: str, from_member_id: str, to_member_id: str, message_type: str, payload: Any, *, causal_context: Optional[Dict[str, Any]] = None, operation_id: Optional[str] = None) -> GroupMessage:
        body: Dict[str, Any] = {"from_member_id": from_member_id, "to_member_id": to_member_id, "type": message_type, "payload": payload}
        if causal_context is not None: body["causal_context"] = causal_context
        if operation_id is not None: body["operation_id"] = operation_id
        data = await self._request("POST", f"/api/v1/execution-groups/{group_id}/messages", json=body)
        return GroupMessage.from_response(data)

    async def group_messages(self, group_id: str, member_id: str, *, after: Optional[str] = None, limit: int = 100) -> List[GroupMessage]:
        params: Dict[str, Any] = {"limit": limit}
        if after is not None: params["after"] = after
        data = await self._request("GET", f"/api/v1/execution-groups/{group_id}/members/{member_id}/messages", params=params)
        return [GroupMessage.from_response(v) for v in data.get("messages", [])]

    async def acknowledge_group_message(self, message_id: str, owner_id: str, generation: int) -> GroupMessage:
        data = await self._request("POST", f"/api/v1/execution-group-messages/{message_id}/ack", json={"owner_id": owner_id, "generation": generation})
        return GroupMessage.from_response(data)

    async def fork_execution_group(self, fork_group_id: str, source_group_id: str, intervention_member_id: str, intervention_cursor: int, *, member_policies: Dict[str, str], tool_policies: Dict[str, str]) -> Dict[str, Any]:
        """Create (or idempotently recover) an immutable group-fork receipt."""
        body = {"fork_group_id": fork_group_id, "source_group_id": source_group_id, "intervention_member_id": intervention_member_id, "intervention_cursor": intervention_cursor, "member_policies": member_policies, "tool_policies": tool_policies}
        data = await self._request("POST", "/api/v1/execution-group-forks", json=body)
        return _unwrap_sonic(data["fork"])

    async def get_execution_group_fork(self, fork_group_id: str) -> Dict[str, Any]:
        data = await self._request("GET", f"/api/v1/execution-group-forks/{fork_group_id}")
        return _unwrap_sonic(data["fork"])

    async def promote_execution_group_fork_member(self, fork_group_id: str, member_id: str) -> Dict[str, Any]:
        data = await self._request("POST", f"/api/v1/execution-group-forks/{fork_group_id}/members/{member_id}/promote", json={})
        return _unwrap_sonic(data["fork"])

    # ------------------------------------------------------------------ #
    # Persisted wake-ups
    # ------------------------------------------------------------------ #

    async def schedule_wakeup(
        self,
        channel_id: str,
        run_at: Union[str, datetime],
        *,
        payload: Optional[Dict[str, Any]] = None,
    ) -> Wakeup:
        """Schedule a wake-up for a channel.

        ``POST /api/v1/scheduler/wakeups``

        At or after ``run_at`` (RFC 3339), the server fires a
        ``scheduler.wakeup`` event on the channel. Subscribers receive it
        live over WebSocket; missed consumers can replay it. Wake-ups are
        persisted and survive restarts.

        Args:
            channel_id: Channel that receives the ``scheduler.wakeup`` event.
            run_at: When to fire — an RFC 3339 string (e.g.
                ``"2026-08-05T12:00:00Z"``) or a timezone-aware ``datetime``
                (naive datetimes raise ``ValueError``).
            payload: Optional JSON-serializable payload attached to the event.
        """
        validate_channel_id(channel_id)
        body: Dict[str, Any] = {"channel_id": channel_id, "run_at": _to_rfc3339(run_at)}
        if payload:
            body["payload"] = payload
        data = await self._request("POST", "/api/v1/scheduler/wakeups", json=body)
        return Wakeup.from_response(data)

    async def list_wakeups(
        self,
        *,
        channel_id: Optional[str] = None,
        status: Optional[str] = None,
    ) -> List[Wakeup]:
        """List wake-ups, optionally filtered by channel and status.

        ``GET /api/v1/scheduler/wakeups?channel_id=...&status=...``
        """
        params: Dict[str, Any] = {}
        if channel_id:
            params["channel_id"] = channel_id
        if status:
            params["status"] = status
        data = await self._request("GET", "/api/v1/scheduler/wakeups", params=params)
        return [Wakeup.from_response(w) for w in data.get("wakeups", [])]

    async def get_wakeup(self, wakeup_id: str) -> Optional[Wakeup]:
        """Fetch a single wake-up.

        ``GET /api/v1/scheduler/wakeups/{id}``
        """
        try:
            data = await self._request("GET", f"/api/v1/scheduler/wakeups/{wakeup_id}")
        except APIError as exc:
            if exc.status_code == 404:
                return None
            raise
        return Wakeup.from_response(data)

    async def cancel_wakeup(self, wakeup_id: str) -> bool:
        """Cancel a pending wake-up.

        ``DELETE /api/v1/scheduler/wakeups/{id}``

        Returns ``True`` when cancelled; ``False`` when the wake-up was
        already fired, failed, or cancelled.
        """
        try:
            await self._request("DELETE", f"/api/v1/scheduler/wakeups/{wakeup_id}")
            return True
        except APIError as exc:
            if exc.status_code == 409:
                return False
            raise

    # ------------------------------------------------------------------ #
    # Durable human approval (human-in-the-loop)
    # ------------------------------------------------------------------ #

    async def request_approval(
        self,
        channel_id: str,
        *,
        summary: str,
        details: Optional[Dict[str, Any]] = None,
        timeout_seconds: Optional[float] = None,
        requester: str = "agent",
        request_id: Optional[str] = None,
    ) -> str:
        """Record an ``approval.requested`` event and return its request id.

        ``POST /api/v1/events/record`` (+ ``POST /api/v1/scheduler/wakeups``
        when ``timeout_seconds`` is set).

        The agent process does not need to stay alive: the request is durable,
        and — when a timeout is supplied — a persisted wake-up fires a
        ``scheduler.wakeup`` event on the channel at the deadline. A human (or
        another service) resolves it with :meth:`decide_approval`.

        Args:
            channel_id: Channel the approval belongs to.
            summary: One-line human-readable description of what is being asked.
            details: Optional structured context for the approver.
            timeout_seconds: When set, schedule a durable wake-up at the deadline.
            requester: Actor label stamped on the event.
            request_id: Stable id for retries; generated when omitted.

        Returns:
            The ``request_id`` to pass to :meth:`wait_for_approval` /
            :meth:`decide_approval`.
        """
        validate_channel_id(channel_id)
        request_id = request_id or str(uuid.uuid4())
        payload: Dict[str, Any] = {"request_id": request_id, "summary": summary}
        if details is not None:
            payload["details"] = details
        if timeout_seconds is not None:
            payload["timeout_seconds"] = timeout_seconds
        await self.record(
            channel_id,
            APPROVAL_REQUESTED,
            payload,
            actor=requester,
            operation_id=request_id,
        )
        if timeout_seconds is not None:
            deadline = datetime.now(timezone.utc) + timedelta(seconds=float(timeout_seconds))
            await self.schedule_wakeup(
                channel_id,
                deadline,
                payload={"approval_request_id": request_id, "kind": "approval_timeout"},
            )
        return request_id

    async def decide_approval(
        self,
        channel_id: str,
        request_id: str,
        *,
        decision: str,
        actor: str,
        reason: Optional[str] = None,
        operation_id: Optional[str] = None,
    ) -> Event:
        """Record the human decision (``approved`` | ``rejected``).

        ``POST /api/v1/events/record`` with an ``approval.decided`` event. The
        agent (or any worker) discovers it by replay/stream — there is no
        server-side callback and Actae never resumes anything itself.
        """
        validate_channel_id(channel_id)
        if decision not in ("approved", "rejected"):
            raise ValueError("decision must be 'approved' or 'rejected'")
        # No volatile field (e.g. a timestamp) belongs in the payload: the
        # deterministic operation id must make an identical decision replay,
        # and the event's own `timestamp` records when it was committed.
        payload = {"request_id": request_id, "decision": decision}
        if reason is not None:
            payload["reason"] = reason
        # A deterministic operation id keyed by (request_id, decision) makes a
        # retried/duplicated decision idempotent: the same decision returns the
        # original event. A *different* decision derives a different id and is
        # recorded as a new event (the application applies last-wins).
        op_id = operation_id or deterministic_operation_key(
            "approval", decision, request_id
        )
        return await self.record(
            channel_id,
            APPROVAL_DECIDED,
            payload,
            actor=actor,
            operation_id=op_id,
        )

    async def wait_for_approval(
        self,
        channel_id: str,
        request_id: str,
        *,
        timeout_seconds: Optional[float] = None,
        poll_interval: float = 1.0,
        start_cursor: int = 0,
    ) -> Optional[Event]:
        """Wait for the decision on ``request_id`` (durable, poll-based).

        Replays the channel from ``start_cursor`` until an ``approval.decided``
        event for ``request_id`` appears. On timeout, records an
        ``approval.expired`` event and returns ``None``. Polling is
        deliberately simple and restart-safe; use WebSocket ``stream()`` for
        lower-latency notification.
        """
        validate_channel_id(channel_id)
        loop = asyncio.get_running_loop()
        deadline = (
            loop.time() + float(timeout_seconds) if timeout_seconds is not None else None
        )
        cursor = start_cursor
        while True:
            events = await self.replay(channel_id, cursor=cursor, limit=500)
            for ev in events:
                cursor = max(cursor, ev.cursor)
                if ev.event_type == APPROVAL_DECIDED and ev.payload.get("request_id") == request_id:
                    return ev
            if deadline is not None and loop.time() >= deadline:
                await self.record(
                    channel_id,
                    APPROVAL_EXPIRED,
                    {"request_id": request_id},
                    actor="system",
                    operation_id=None,
                )
                return None
            await asyncio.sleep(poll_interval)

    # ------------------------------------------------------------------ #
    # WebSocket connection
    # ------------------------------------------------------------------ #

    async def connect(self) -> None:
        """Open a WebSocket connection to the Actae server.

        Performs authentication with the configured API key. Safe to call
        multiple times — subsequent calls are no-ops if already connected.

        Raises:
            ActaeConnectionError: If the WebSocket handshake fails.
            AuthError: If API key authentication times out or is rejected.
        """
        async with self._lock:
            if self._connected:
                return
            await self._do_connect()

    async def _do_connect(self) -> None:
        session_created = False
        loop = asyncio.get_running_loop()
        if not self._session or self._session.closed:
            connector = aiohttp.TCPConnector(ssl=self._ssl)
            self._session = aiohttp.ClientSession(connector=connector)
            session_created = True
        elif isinstance(self._session, aiohttp.ClientSession) and (
            getattr(self._session, "_loop", None) is not loop
        ):
            # Session bound to a different (stale) loop: recreate. Foreign
            # session objects (e.g. mocks in tests) are left untouched.
            connector = aiohttp.TCPConnector(ssl=self._ssl)
            self._session = aiohttp.ClientSession(connector=connector)
            session_created = True

        try:
            # The heartbeat must be strictly shorter than the per-receive
            # timeout below: aiohttp's receive timeout is per receive() call,
            # so on a quiet topic (no events for `timeout` seconds) the
            # deadline can fire just before the next pong arrives, killing
            # an otherwise healthy connection and causing reconnect churn.
            # heartbeat=timeout/2 keeps pongs well inside the window.
            heartbeat = min(30.0, max(5.0, self._timeout / 2))
            self._ws = await self._session.ws_connect(
                self._ws_endpoint,
                heartbeat=heartbeat,
                max_msg_size=1048576,
                timeout=aiohttp.ClientWSTimeout(
                    ws_receive=self._timeout,
                    ws_close=self._timeout,
                ),
            )
        except Exception as exc:
            # Clean up the session we just created, so it doesn't leak
            if session_created and self._session and not self._session.closed:
                await self._session.close()
                self._session = None
            raise ActaeConnectionError(
                f"WebSocket connection failed: {exc}"
                + (f" {_MTLS_HINT}" if getattr(self, "_mtls_hint_enabled", False) else "")
            ) from exc

        self._close_event = asyncio.Event()
        self._auth_event = asyncio.Event()
        self._auth_error: Optional[str] = None
        self._authenticated = False
        self._connection_id = None

        self._reader_task = asyncio.create_task(self._reader())

        await self._ws.send_json({
            "type": "auth",
            "api_key": self._api_key,
        })

        try:
            await asyncio.wait_for(self._auth_event.wait(), timeout=self._timeout)
        except asyncio.TimeoutError:
            self._auth_error = self._auth_error or "Authentication timed out"
            await self._disconnect_ws()
            if session_created and self._session and not self._session.closed:
                await self._session.close()
                self._session = None
            raise AuthError(self._auth_error)
        if self._auth_error:
            await self._disconnect_ws()
            if session_created and self._session and not self._session.closed:
                await self._session.close()
                self._session = None
            raise AuthError(self._auth_error)

        self._connected = True

    async def disconnect(self) -> None:
        """Close the WebSocket connection and clean up resources.

        Cancels the reader and reconnect tasks, closes the WebSocket and
        HTTP session. Safe to call multiple times.
        """
        async with self._lock:
            self._auto_reconnect = False
            await self._disconnect_ws()
            if self._session and not self._session.closed:
                await self._session.close()
                self._session = None
            loop = asyncio.get_running_loop()
            for session in list(self._sessions_by_loop.values()):
                if session and not session.closed:
                    bound_loop = getattr(session, "_loop", None)
                    if bound_loop is loop:
                        await session.close()
                    elif bound_loop is not None:
                        # Session belongs to a background loop (the sync
                        # facade's per-thread loop). Closing it here would
                        # raise a cross-loop RuntimeError, so schedule the
                        # close on its own loop instead. If that loop is
                        # already gone (the sync facade thread exited), the
                        # async close can never run — force-close the
                        # connector synchronously (`_close` is the sync
                        # variant; `close` is a coroutine) so nothing leaks.
                        if bound_loop.is_closed():
                            try:
                                connector = getattr(session, "_connector", None)
                                if connector is not None and not getattr(connector, "closed", True):
                                    connector._close()
                                session._closed = True
                            except Exception:
                                pass
                            continue
                        try:
                            coro = session.close()
                            asyncio.run_coroutine_threadsafe(coro, bound_loop)
                        except RuntimeError:
                            # Loop died between the check and the schedule —
                            # never await the coroutine, but close it so it
                            # doesn't leak an "never awaited" RuntimeWarning.
                            coro.close()
            self._sessions_by_loop.clear()

    async def _close_current_loop_session(self) -> None:
        """Close the HTTP session bound to the current event loop.

        Used by sync framework adapters (e.g. the LangGraph checkpointer)
        that run their async implementation in a fresh event loop: the loop
        dies right after the run, so its session must be closed while the
        loop is still alive.
        """
        loop = asyncio.get_running_loop()
        session = self._sessions_by_loop.pop(id(loop), None)
        if (
            session is not None
            and not session.closed
            and getattr(session, "_loop", None) is loop
        ):
            await session.close()

    async def _disconnect_ws(self) -> None:
        if self._close_event:
            self._close_event.set()
        if self._reconnect_task:
            self._reconnect_task.cancel()
            try:
                await self._reconnect_task
            except asyncio.CancelledError:
                pass
            self._reconnect_task = None
        if self._reader_task:
            self._reader_task.cancel()
            try:
                await self._reader_task
            except asyncio.CancelledError:
                pass
            self._reader_task = None
        # Terminate any active stream() iterators.
        for queues in self._stream_queues.values():
            for queue in queues:
                queue.put_nowait(_STREAM_END)
        # Fail any in-flight publish waits so they cannot hang forever, and
        # drop their waiters so a future connection's ack cannot match them
        # (audit item 8).
        for future in self._pending_acks.values():
            if not future.done():
                future.cancel()
        self._pending_acks.clear()
        if self._ws and not self._ws.closed:
            await self._ws.close()
            self._ws = None
        self._connected = False
        self._authenticated = False

    # ------------------------------------------------------------------ #
    # WebSocket subscribe / publish
    # ------------------------------------------------------------------ #

    async def subscribe(self, topic: str, cursor: Optional[int] = None, *, wait: bool = True) -> None:
        """Subscribe to a topic (channel) for real-time events.

        Sends a ``subscribe`` message over the WebSocket. Once subscribed,
        broadcast events on the topic are delivered to the ``on_message``
        callbacks and any active ``stream()`` iterators.

        Args:
            topic: Channel/topic to subscribe to.
            cursor: Optional starting cursor. Events with a (global) cursor
                strictly greater than this value are replayed before live
                events — the same exclusive semantics as ``replay()``.
            wait: If True, waits for server confirmation (up to the client's
                ``timeout``) before returning. Defaults to True so you never
                miss events right after subscribing.

        Raises:
            ActaeConnectionError: If not connected, or the subscription
                confirmation times out (when ``wait=True``).
        """
        validate_channel_id(topic)
        if not self._connected or not self._ws:
            raise ActaeConnectionError("Not connected")
        msg: Dict[str, Any] = {"type": "subscribe", "topic": topic}
        if cursor is not None:
            msg["cursor"] = cursor
        # Fresh subscription window: the catch-up replay may legitimately
        # re-deliver events that were already delivered during a previous
        # subscription to the same topic.
        self._delivered_cursor.pop(topic, None)
        if wait:
            ev = asyncio.Event()
            # Multiple concurrent subscribers on the same topic each get
            # their own waiter; a single confirmation must wake them all.
            self._subscribed_events.setdefault(topic, []).append(ev)
        await self._ws.send_json(msg)
        if wait:
            try:
                await asyncio.wait_for(ev.wait(), timeout=self._timeout)
            except asyncio.TimeoutError:
                waiters = self._subscribed_events.get(topic)
                if waiters and ev in waiters:
                    waiters.remove(ev)
                    if not waiters:
                        self._subscribed_events.pop(topic, None)
                raise ActaeConnectionError(f"Subscription confirmation for {topic} timed out")

    async def unsubscribe(self, topic: str) -> None:
        """Unsubscribe from a topic.

        Sends an ``unsubscribe`` message over the WebSocket. No more events
        will be delivered for this topic.

        Raises:
            ActaeConnectionError: If not connected.
        """
        if not self._connected or not self._ws:
            raise ActaeConnectionError("Not connected")
        await self._ws.send_json({"type": "unsubscribe", "topic": topic})

    async def publish(self, topic: str, payload: Any, *, operation_id: Optional[str] = None) -> Event:
        """Publish an event over WebSocket. Returns the persisted Event.

        Sends a broadcast; the server persists the event and sends back an
        Ack with the full event (cursor, channel_cursor alias, timestamp, payload).
        No separate HTTP replay round-trip is needed.

        Publishes are serialized internally so each call waits for its own
        Ack (concurrent publishes cannot steal each other's responses).

        Args:
            topic: Channel/topic to publish to.
            payload: Event payload (must be JSON-serializable).
            operation_id: Optional idempotency key. Retrying with the same
                ``(topic, operation_id)`` replays the original persisted
                event instead of duplicating — pass a stable UUID to make
                retries after ack timeouts safe (mirrors ``record``).

        Returns:
            The persisted ``Event`` with server-assigned cursor and ID.

        Raises:
            ActaeConnectionError: If not connected.
            APIError: If no Ack is received within the configured timeout.
        """
        validate_channel_id(topic)
        if not self._connected or not self._ws:
            raise ActaeConnectionError("Not connected")

        async with self._publish_lock:
            request_id = str(uuid.uuid4())
            frame: Dict[str, Any] = {
                "type": "broadcast",
                "topic": topic,
                "payload": payload,
                "request_id": request_id,
            }
            if operation_id is not None:
                frame["operation_id"] = operation_id

            # Register the waiter BEFORE sending so the ack can never arrive
            # before the mapping exists. The server echoes request_id in the
            # Ack; a stale ack for a timed-out request has no future and is
            # dropped in _reader (audit item 8).
            loop = asyncio.get_running_loop()
            ack_future: "asyncio.Future[Any]" = loop.create_future()
            self._pending_acks[request_id] = ack_future
            try:
                await self._ws.send_json(frame)

                # Wait for the server's Ack — it now carries the full event
                try:
                    ack_data = await asyncio.wait_for(ack_future, timeout=self._timeout)
                except asyncio.TimeoutError:
                    raise APIError(500, "No Ack received from server after publish")
            finally:
                self._pending_acks.pop(request_id, None)

        event = Event(
            id=ack_data.get("id", ""),
            channel_id=ack_data.get("channel_id", topic),
            event_type=ack_data.get("event_type", "broadcast"),
            payload=_unwrap_sonic(ack_data.get("payload", payload)),
            actor=ack_data.get("actor", ""),
            cursor=ack_data.get("cursor", 0),
            channel_cursor=ack_data.get("channel_cursor"),
            timestamp=ack_data.get("timestamp", ""),
        )
        if self._echo_self:
            self._deliver_local(topic, event)
        return event

    async def stream(self, topic: str, cursor: Optional[int] = None) -> Any:
        """Asynchronously iterate over live events on a topic.

        Subscribes to ``topic`` and yields each ``Event`` as it arrives::

            async with ActaeClient(...) as actae:
                async for event in actae.stream("my-channel"):
                    print(event.event_type, event.payload)

        The iterator ends when the WebSocket disconnects. Use it instead of
        ``on_message`` when you want per-event async processing.

        Args:
            topic: Channel/topic to stream events from.
            cursor: Optional starting cursor. Events with a (global) cursor
                strictly greater than this value are replayed before live
                events — the same exclusive semantics as ``replay()``.

        Yields:
            ``Event`` objects as they are broadcast on the topic.

        Raises:
            ActaeConnectionError: If not connected.
        """
        if not self._connected or not self._ws:
            raise ActaeConnectionError("Not connected")
        queue: asyncio.Queue = asyncio.Queue()
        self._stream_queues.setdefault(topic, []).append(queue)
        try:
            await self.subscribe(topic, cursor=cursor)
            while True:
                item = await queue.get()
                if item is _STREAM_END:
                    return
                yield item
        finally:
            queues = self._stream_queues.get(topic)
            if queues:
                try:
                    queues.remove(queue)
                except ValueError:
                    pass
                if not queues:
                    self._stream_queues.pop(topic, None)

    # ------------------------------------------------------------------ #
    # WebSocket reader
    # ------------------------------------------------------------------ #

    async def _reader(self) -> None:
        ws = self._ws
        if not ws:
            return
        try:
            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    try:
                        data = json.loads(msg.data)
                    except json.JSONDecodeError:
                        logger.warning("Invalid JSON from server: %s", msg.data[:200])
                        continue
                    try:
                        await self._dispatch(data)
                    except Exception:
                        logger.exception("Dispatch failed for message: %s", msg.data[:200])
                elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                    break
        except asyncio.CancelledError:
            return
        except Exception as exc:
            logger.error("WebSocket reader error: %s", exc)
        finally:
            was_connected = self._connected
            self._connected = False
            if was_connected:
                # Terminate any active stream() iterators.
                for queues in self._stream_queues.values():
                    for queue in queues:
                        queue.put_nowait(_STREAM_END)
                if self._on_disconnected_cb:
                    try:
                        self._on_disconnected_cb()
                    except Exception:
                        logger.exception("disconnected callback failed")
                if self._auto_reconnect:
                    self._reconnect_task = asyncio.create_task(self._reconnect_loop())

    @staticmethod
    def _normalize_msg(data: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize externally-tagged Actae messages to SDK format.

        The Actae server serializes enums as externally-tagged JSON::

            {"Connection": {"event": "connected", "payload": {...}}}
            {"Broadcast": {"topic": "...", "payload": {...}}}
            {"Error": {"message": "..."}}

        The SDK expects flat format::

            {"type": "connection", "event": "connected", "payload": {...}}
            {"type": "broadcast", "topic": "...", "payload": {...}}
            {"type": "error", "message": "..."}

        This method converts between the two.
        """
        TYPE_MAP = {
            "Connection": "connection",
            "Broadcast": "broadcast",
            "Subscription": "subscription",
            "Ack": "ack",
            "CursorSync": "cursor_sync",
            "Error": "error",
            "Nack": "nack",
        }
        for key, type_name in TYPE_MAP.items():
            inner = data.get(key)
            if isinstance(inner, dict):
                inner["type"] = type_name
                return inner
        return data


    def _deliver_local(self, topic: str, event: Event) -> None:
        """Fan an event out to this client's local consumers.

        Used for server broadcasts (via ``_dispatch``) and, when
        ``echo_self`` is set, for events this client publishes itself (the
        server never echoes a broadcast back to its publisher).
        """
        for cb in list(self._on_message_cbs):
            try:
                cb(topic, event)
            except Exception:
                logger.exception("message callback failed")
        for queue in self._stream_queues.get(topic, ()):
            queue.put_nowait(event)

    async def _dispatch(self, data: Dict[str, Any]) -> None:
        data = self._normalize_msg(data)
        msg_type = data.get("type")

        if msg_type == "connection":
            event = data.get("event")
            payload = data.get("payload", {})
            if event == "connected":
                self._connection_id = payload.get("connection_id")
            elif event == "authenticated":
                self._authenticated = True
                self._connection_id = payload.get("connection_id", self._connection_id)
                if self._auth_event:
                    self._auth_event.set()
            elif event == "pong":
                pass

        elif msg_type == "broadcast":
            topic = data.get("topic", "")
            raw_payload = data.get("payload", {})
            if isinstance(raw_payload, dict) and "event" in raw_payload:
                event = Event.from_broadcast(raw_payload["event"])
            else:
                event = Event.from_broadcast({
                    "id": "",
                    "payload": raw_payload,
                    "type": "broadcast",
                })
            # Drop catch-up duplicates (see _delivered_cursor): the server
            # can deliver one event both in a subscribe replay and live.
            if (
                event.cursor
                and topic in self._delivered_cursor
                and event.cursor <= self._delivered_cursor[topic]
            ):
                return
            if event.cursor:
                self._delivered_cursor[topic] = event.cursor
            # Track the last-seen cursor per topic so an auto-reconnect can
            # resubscribe at this watermark (exclusive semantics) instead of
            # the stale subscription-time cursor — otherwise every event
            # since the first subscribe would be redelivered. Only track
            # topics we are actually subscribed to: an in-flight broadcast
            # delivered after an unsubscribe must not resurrect the topic
            # in the resubscribe set.
            if event.cursor and topic in self._subscribed:
                self._subscribed[topic] = event.cursor
            self._deliver_local(topic, event)

        elif msg_type == "subscription":
            event = data.get("event")
            payload = data.get("payload", {})
            if event == "subscribed":
                topic = data.get("topic", payload.get("topic", ""))
                cursor = data.get("cursor", payload.get("cursor"))
                self._subscribed[topic] = cursor if cursor is not None else 0
                waiters = self._subscribed_events.pop(topic, None)
                if waiters:
                    for w in waiters:
                        w.set()
                if self._on_subscribed_cb:
                    try:
                        self._on_subscribed_cb(topic, cursor)
                    except Exception:
                        logger.exception("subscribed callback failed")
            elif event == "unsubscribed":
                topic = payload.get("topic", "")
                self._subscribed.pop(topic, None)
            elif event == "error":
                error_msg = payload.get("error", "Subscription failed")
                logger.error("Subscription error: %s", error_msg)
                if self._on_error_cb:
                    try:
                        self._on_error_cb(error_msg)
                    except Exception:
                        logger.exception("error callback failed")

        elif msg_type == "cursor_sync":
            topic = data.get("topic", "")
            cursor = data.get("cursor", 0)
            self._subscribed[topic] = cursor

        elif msg_type == "ack":
            # Correlate by request_id (audit item 8): a publish registers a
            # future under its request_id and only its own ack resolves it.
            # A request_id-bearing ack with no pending future is a STALE ack
            # for a timed-out/aborted publish and is discarded. Acks without
            # request_id (legacy servers) fall back to the shared FIFO queue.
            rid = data.get("request_id") or None
            if rid is not None:
                future = self._pending_acks.pop(rid, None)
                if future is not None and not future.done():
                    future.set_result(data)
            else:
                self._ack_queue.put_nowait(data)

        elif msg_type == "error":
            error_msg = data.get("message", "Unknown WebSocket error")
            if (
                not self._authenticated
                and self._auth_event is not None
                and not self._auth_event.is_set()
            ):
                # Server-side rejection of our auth attempt: wake the
                # connect() wait with the real reason instead of a timeout.
                self._auth_error = error_msg
                self._auth_event.set()
            logger.error("WebSocket error: %s", error_msg)
            if self._on_error_cb:
                try:
                    self._on_error_cb(error_msg)
                except Exception:
                    logger.exception("error callback failed")

    # ------------------------------------------------------------------ #
    # Reconnection
    # ------------------------------------------------------------------ #

    async def _reconnect_loop(self) -> None:
        delay = 0.5
        max_delay = 30.0
        while self._auto_reconnect and not self._close_event.is_set():
            try:
                # Decorrelated jitter (±50%) breaks the thundering herd when
                # many SDK clients reconnect at once (e.g. after a server or
                # network outage ends).
                await asyncio.sleep(_jittered_delay(delay))
            except asyncio.CancelledError:
                return
            try:
                async with self._lock:
                    if self._connected or not self._auto_reconnect:
                        return
                    await self._do_connect()
                subscribed = list(self._subscribed.items())
                for topic, cursor in subscribed:
                    try:
                        await self.subscribe(topic, cursor=cursor if cursor else None, wait=True)
                    except Exception as exc:
                        logger.warning("Resubscribe to %s failed: %s", topic, exc)
                self._reconnect_failures = 0
                logger.info("Reconnected successfully")
                if self._on_reconnect_cb:
                    try:
                        self._on_reconnect_cb()
                    except Exception:
                        logger.exception("on_reconnect callback failed")
                return
            except Exception:
                self._reconnect_failures += 1
                if self._reconnect_failures >= self._max_reconnect_failures:
                    logger.error(
                        "Reconnect failed %d times (max %d), giving up",
                        self._reconnect_failures, self._max_reconnect_failures,
                    )
                    self._auto_reconnect = False
                    return
                delay = min(delay * 2, max_delay)

    # ------------------------------------------------------------------ #
    # HTTP request helper
    # ------------------------------------------------------------------ #

    async def _request(
        self,
        method: str,
        path: str,
        **kwargs: Any,
    ) -> Any:
        loop = asyncio.get_running_loop()
        # Legacy single-session path: honour an explicitly assigned session
        # (e.g. `client._session = mock` in tests) when it is bound to the
        # current loop or is a foreign object (mock) assumed loop-agnostic.
        legacy = getattr(self, "_session", None)
        if legacy is not None and not getattr(legacy, "closed", False):
            if isinstance(legacy, aiohttp.ClientSession):
                legacy_ok = getattr(legacy, "_loop", None) is loop
            else:
                legacy_ok = True
        else:
            legacy_ok = False
        if legacy_ok:
            session = legacy
        else:
            session = self._sessions_by_loop.get(id(loop))
            if (
                session is None
                or session.closed
                or getattr(session, "_loop", None) is not loop
            ):
                if session is not None:
                    # Session belongs to a dead/different loop; drop it so the
                    # connector is garbage-collected and recreated here.
                    self._sessions_by_loop.pop(id(loop), None)
                session = aiohttp.ClientSession(
                    connector=aiohttp.TCPConnector(ssl=self._ssl),
                )
                self._sessions_by_loop[id(loop)] = session

        headers = kwargs.pop("headers", {})
        headers.setdefault("x-api-key", self._api_key)

        url = f"{self._endpoint}{path}"
        timeout_obj = aiohttp.ClientTimeout(total=self._timeout)

        try:
            async with session.request(
                method, url, headers=headers, timeout=timeout_obj, **kwargs
            ) as resp:
                # Handle text-based responses (e.g., /metrics Prometheus output)
                content_type = resp.headers.get("Content-Type", "")
                if "text/" in content_type or "application/text" in content_type:
                    body = await resp.text()
                else:
                    body = await resp.json()
                if resp.status >= 400:
                    if isinstance(body, dict):
                        status_code = body.get("status", "")
                        error_msg = body.get("error", body.get("message", ""))
                        if resp.status == 429:
                            retry_after = body.get("retry_after_seconds", 60)
                            raise RateLimitError(retry_after, error_msg)
                        if resp.status == 401:
                            raise AuthError(error_msg or "Authentication failed")
                        if resp.status == 409:
                            if status_code == "snapshot_boundary_required":
                                raise SnapshotBoundaryError(error_msg)
                            if status_code == "version_conflict":
                                raise VersionConflictError(error_msg)
                            if status_code == "consumer_not_found":
                                raise ConsumerError(error_msg)
                            if status_code == "idempotency_key_mismatch":
                                raise IdempotencyKeyMismatchError(error_msg)
                            if status_code == "execution_not_owned":
                                raise ExecutionNotOwnedError(error_msg)
                            if status_code == "idempotency_conflict":
                                raise IdempotencyConflictError(error_msg)
                            if status_code == "channel_conflict":
                                raise ChannelConflictError(error_msg)
                            if status_code == "counterfactual_blocked":
                                raise CounterfactualBlockedError(error_msg)
                            if status_code == "fork_tool_blocked":
                                raise ForkToolBlockedError(error_msg)
                        if resp.status == 404 and "execution_not_found" in error_msg:
                            raise ExecutionNotFoundError(error_msg)
                    else:
                        error_msg = ""
                    raise APIError(resp.status, error_msg)
                return body
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            # Transport-level failure (connect refused, DNS, TLS handshake,
            # read timeout, premature close) — wrap it so `except ActaeError`
            # catches every SDK failure, and the AgentSession retry logic
            # treats it as transient.
            raise ActaeConnectionError(
                f"HTTP request to {path} failed: {exc}"
            ) from exc

    # ------------------------------------------------------------------ #
    # Synchronous facade — for non-async codebases
    # ------------------------------------------------------------------ #

    def record_sync(
        self,
        channel_id: str,
        event_type: str,
        payload: Any,
        *,
        actor: str,
        agent_id: Optional[str] = None,
        user_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        operation_id: Optional[str] = None,
    ) -> Event:
        """Synchronous variant of :meth:`record` for non-async code."""
        from ._sync import run_sync
        return run_sync(self.record(
            channel_id, event_type, payload,
            actor=actor, agent_id=agent_id, user_id=user_id, metadata=metadata,
            operation_id=operation_id,
        ))

    def replay_sync(
        self,
        channel_id: str,
        *,
        cursor: Optional[int] = None,
        limit: int = 100,
        event_type: Optional[str] = None,
    ) -> List[Event]:
        """Synchronous variant of :meth:`replay` for non-async code."""
        from ._sync import run_sync
        return run_sync(self.replay(channel_id, cursor=cursor, limit=limit, event_type=event_type))

    def query_sync(
        self,
        *,
        channel_ids: Optional[List[str]] = None,
        event_type: Optional[str] = None,
        actor: Optional[str] = None,
        cursor_start: Optional[int] = None,
        cursor_end: Optional[int] = None,
        from_time: Optional[str] = None,
        to_time: Optional[str] = None,
        limit: int = 100,
        offset: Optional[int] = None,
    ) -> List[Event]:
        """Synchronous variant of :meth:`query` for non-async code."""
        from ._sync import run_sync
        return run_sync(self.query(
            channel_ids=channel_ids, event_type=event_type, actor=actor,
            cursor_start=cursor_start, cursor_end=cursor_end,
            from_time=from_time, to_time=to_time, limit=limit, offset=offset,
        ))

    def get_cursor_sync(self, channel_id: str) -> Optional[int]:
        """Synchronous variant of :meth:`get_cursor` for non-async code."""
        from ._sync import run_sync
        return run_sync(self.get_cursor(channel_id))

    def latest_state_sync(self, channel_id: str) -> Optional[Dict[str, Any]]:
        """Synchronous variant of :meth:`latest_state` for non-async code."""
        from ._sync import run_sync
        return run_sync(self.latest_state(channel_id))

    def save_state_sync(
        self,
        channel_id: str,
        cursor: int,
        state: Any,
        *,
        expected_version: Optional[int] = None,
        expected_cursor: Optional[int] = None,
    ) -> int:
        """Synchronous variant of :meth:`save_state` for non-async code."""
        from ._sync import run_sync
        return run_sync(self.save_state(
            channel_id, cursor, state,
            expected_version=expected_version, expected_cursor=expected_cursor,
        ))

    def transition_sync(
        self,
        channel_id: str,
        event_type: str,
        payload: Any,
        state: Any,
        *,
        actor: str = "system",
        agent_id: Optional[str] = None,
        user_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        operation_id: Optional[str] = None,
        expected_version: Optional[int] = None,
        expected_cursor: Optional[int] = None,
    ) -> TransitionResult:
        """Synchronous variant of :meth:`transition` for non-async code."""
        from ._sync import run_sync
        return run_sync(self.transition(
            channel_id, event_type, payload, state,
            actor=actor, agent_id=agent_id, user_id=user_id, metadata=metadata,
            operation_id=operation_id,
            expected_version=expected_version, expected_cursor=expected_cursor,
        ))

    def fork_sync(
        self,
        source_channel_id: str,
        new_channel_id: str,
        at_cursor: int = 0,
        *,
        display_name: Optional[str] = None,
        reason: Optional[str] = None,
        experiment_metadata: Optional[Dict[str, Any]] = None,
        operation_id: Optional[str] = None,
        manifest: Optional[Dict[str, Any]] = None,
        tool_policies: Optional[Dict[str, Any]] = None,
        expected_version: Optional[int] = None,
        expected_cursor: Optional[int] = None,
    ) -> ForkReceipt:
        """Synchronous variant of :meth:`fork` for non-async code."""
        from ._sync import run_sync
        return run_sync(self.fork(
            source_channel_id, new_channel_id, at_cursor,
            display_name=display_name, reason=reason,
            experiment_metadata=experiment_metadata, operation_id=operation_id,
            manifest=manifest, tool_policies=tool_policies,
            expected_version=expected_version,
            expected_cursor=expected_cursor,
        ))

    def diff_states_sync(self, left_channel_id: str, right_channel_id: str) -> Dict[str, Any]:
        """Synchronous variant of :meth:`diff_states` for non-async code."""
        from ._sync import run_sync
        return run_sync(self.diff_states(left_channel_id, right_channel_id))

    def decision_trail_sync(self, channel_id: str) -> Dict[str, Any]:
        """Synchronous variant of :meth:`decision_trail` for non-async code."""
        from ._sync import run_sync
        return run_sync(self.decision_trail(channel_id))

    def disconnect_sync(self) -> None:
        """Close the WebSocket and all HTTP sessions from sync code.

        Counterpart of :meth:`disconnect` for code that only uses the
        ``*_sync`` facade. Call it at process shutdown to avoid leaking
        aiohttp connectors:
        """
        from ._sync import run_sync
        run_sync(self.disconnect())
