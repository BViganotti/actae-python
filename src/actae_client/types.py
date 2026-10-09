from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

_SONIC_NUMBER_KEY = "$sonic_rs::private::JsonNumber"


def _unwrap_sonic(value: Any) -> Any:
    """Recursively replace sonic-rs number wrappers with native Python int/float.

    The Actae server serializes large JSON numbers via `sonic_rs::to_string`
    which wraps them in ``{"$sonic_rs::private::JsonNumber": "12345"}``.
    This function unwraps them so callers get Python-native types.

    Malformed wrappers never leak the internal wrapper key to callers: the
    raw string is returned instead, so downstream code always sees data it
    can handle.
    """
    if isinstance(value, dict):
        if _SONIC_NUMBER_KEY in value and len(value) == 1:
            raw = value[_SONIC_NUMBER_KEY]
            if not isinstance(raw, str):
                return raw
            try:
                if raw.startswith("-"):
                    raw = raw[1:]
                    sign = -1
                else:
                    sign = 1
                if "." in raw:
                    return sign * float(raw)
                return sign * int(raw)
            except (ValueError, OverflowError):
                return value[_SONIC_NUMBER_KEY]
        return {k: _unwrap_sonic(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_unwrap_sonic(e) for e in value]
    return value


@dataclass
class ReadinessResult:
    """Server readiness probe result from ``GET /readyz``.

    Indicates whether the Actae instance is ready to accept traffic,
    including its database and capacity status.
    """

    status: str                                    #: Overall readiness: ``"ok"``, ``"degraded"``, or ``"unavailable"``
    timestamp: str                                 #: ISO 8601 timestamp of the check
    instance_id: str                               #: Unique server instance identifier
    check_duration_ms: int                         #: Duration of the readiness check in milliseconds
    database_ready: bool                           #: Whether the PostgreSQL connection is healthy
    capacity_available: bool                       #: Whether the server has capacity for new connections
    uptime_seconds: int                            #: Server uptime in seconds
    connection_utilization: float                  #: Fraction of max connections currently in use (0.0–1.0)
    active_connections: int                        #: Current number of active WebSocket connections
    max_connections: int                           #: Maximum allowed WebSocket connections

    @classmethod
    def from_response(cls, data: Dict[str, Any]) -> "ReadinessResult":
        checks = data.get("readiness_checks", {})
        return cls(
            status=data.get("status", "unknown"),
            timestamp=data.get("timestamp", ""),
            instance_id=data.get("instance_id", ""),
            check_duration_ms=data.get("check_duration_ms", 0),
            database_ready=checks.get("database_ready", False),
            capacity_available=checks.get("capacity_available", False),
            uptime_seconds=checks.get("uptime_seconds", 0),
            connection_utilization=checks.get("connection_utilization", 0.0),
            active_connections=checks.get("active_connections", 0),
            max_connections=checks.get("max_connections", 0),
        )


@dataclass
class MetricsSnapshot:
    """Live server metrics snapshot from ``GET /metrics.json``.

    Provides a point-in-time view of server load, WebSocket connections,
    and message throughput.
    """

    status: str                                    #: Server status (``"ok"`` or ``"unavailable"``)
    timestamp: str                                 #: ISO 8601 timestamp of the snapshot
    uptime_seconds: int                            #: Server uptime in seconds
    websocket_connections: int                     #: Current number of active WebSocket connections
    topic_count: int                               #: Number of distinct topics with active subscriptions
    messages_sent: int                             #: Total messages pushed from server to clients
    messages_received: int                         #: Total messages received from clients
    total_messages: int                            #: Sum of sent + received
    back_pressure: Dict[str, Any]                  #: Per-topic back-pressure stats (queue depths, lag)

    @classmethod
    def from_response(cls, data: Dict[str, Any]) -> "MetricsSnapshot":
        ws = data.get("websocket", {})
        bp = ws.get("back_pressure", {})
        return cls(
            status=data.get("status", "unknown"),
            timestamp=data.get("timestamp", ""),
            uptime_seconds=data.get("uptime_seconds", 0),
            websocket_connections=ws.get("connections", 0),
            topic_count=ws.get("topics", 0),
            messages_sent=ws.get("messages_sent", 0),
            messages_received=ws.get("messages_received", 0),
            total_messages=ws.get("total_messages", 0),
            back_pressure=bp,
        )


@dataclass
class UserInfo:
    """User profile returned by auth and admin endpoints."""

    id: str                                        #: Unique user identifier (UUID)
    email: str                                     #: User email address
    email_verified: bool                           #: Whether the email has been verified
    name: Optional[str]                            #: Display name (may be null)
    image: Optional[str]                           #: URL to avatar image (may be null)
    created_at: Optional[str] = None               #: ISO 8601 timestamp of account creation

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "UserInfo":
        return cls(
            id=data["id"],
            email=data["email"],
            email_verified=data.get("email_verified", False),
            name=data.get("name"),
            image=data.get("image"),
            created_at=data.get("created_at"),
        )


@dataclass
class AuthResult:
    """Result returned by ``signup()`` and ``login()``."""

    user: UserInfo                                 #: Authenticated user profile
    token: str                                     #: JWT token for subsequent auth'd requests

    @classmethod
    def from_response(cls, data: Dict[str, Any]) -> "AuthResult":
        return cls(
            user=UserInfo.from_dict(data.get("user", {})),
            token=data.get("token", ""),
        )


@dataclass
class ChannelMetadata:
    """Metadata for a channel or fork in the Actae execution tree.

    Returned by ``fork()``, ``get_channel_metadata()``, and ``list_forks()``.
    """

    channel_id: str                                #: Unique channel identifier
    parent_channel_id: Optional[str]               #: Source channel if this is a fork, null for root channels
    origin_run_id: str                             #: Original root channel in the execution tree
    forked_at_cursor: int                          #: RESOLVED fork boundary cursor (snapshot actually inherited)
    forked_at: str                                 #: ISO 8601 timestamp of the fork operation
    display_name: Optional[str] = None             #: Human-readable display name
    reason: Optional[str] = None                   #: Reason for the fork (e.g., "Changed model to GPT-5")
    experiment_metadata: Optional[Dict[str, Any]] = None  #: Arbitrary metadata (used by AgentSession)
    requested_at_cursor: int = 0                   #: The boundary the caller asked for (0 = latest)
    resolved_cursor: int = 0                       #: Alias of resolved_state_cursor (snapshot actually inherited)
    resolved_state_cursor: int = 0                 #: The snapshot cursor the child actually inherited
    resolved_event_id: Optional[str] = None        #: Parent boundary event the fork.started depends on
    source_state_version: int = 0                  #: Source snapshot version copied at fork time (0 = none)
    source_state_sha256: Optional[str] = None      #: SHA-256 fingerprint of the copied state
    restorable: bool = False                       #: True when a state snapshot was copied (restorable checkpoint)
    manifest: Optional[Dict[str, Any]] = None      #: Immutable fork manifest supplied at fork time
    reproducibility: Optional[str] = None          #: state_exact | context_exact | execution_replayable | best_effort
    tool_policies: Optional[Dict[str, Any]] = None  #: Side-effect policy map for a forked channel (None = auto)
    outcome: Optional[str] = None                  #: promoted | rejected | inconclusive | crashed
    result_score: Optional[float] = None           #: Recorded result score (higher is better)
    deleted_at: Optional[str] = None               #: ISO 8601 soft-delete marker (retention/GC)


@dataclass
class ForkReceipt:
    """The immutable fork receipt returned at creation and queryable via
    ``GET /channels/{id}/receipt``.

    This is the provenance record the dashboard and audit narrative are
    built around: source identity, requested/resolved boundary, source state
    version + fingerprint, restorability and reproducibility grade.
    """

    fork_id: str                                   #: The child channel id (the fork identity)
    source_channel_id: Optional[str]               #: The channel forked FROM
    child_channel_id: str                          #: The child channel id
    requested_cursor: int                          #: The boundary the caller asked for (0 = latest)
    resolved_cursor: int                           #: The snapshot cursor actually inherited
    resolved_event_id: Optional[str] = None        #: Parent boundary event id
    source_state_version: int = 0                  #: Source snapshot version copied (0 = none)
    source_state_sha256: Optional[str] = None      #: SHA-256 of the copied state
    restorable: bool = False                       #: True when a state snapshot was copied
    replayed: bool = False                         #: True when served from the idempotency replay path
    manifest: Optional[Dict[str, Any]] = None      #: The fork manifest
    reproducibility: Optional[str] = None          #: Reproducibility grade
    tool_policies: Optional[Dict[str, Any]] = None  #: Side-effect policy map for the child (None = auto)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ForkReceipt":
        return cls(
            fork_id=data.get("fork_id", data.get("channel_id", "")),
            source_channel_id=data.get("source_channel_id"),
            child_channel_id=data.get("child_channel_id", data.get("channel_id", "")),
            requested_cursor=data.get("requested_cursor", data.get("requested_at_cursor", 0)),
            resolved_cursor=data.get("resolved_cursor", data.get("resolved_state_cursor", 0)),
            resolved_event_id=data.get("resolved_event_id"),
            source_state_version=data.get("source_state_version", 0),
            source_state_sha256=data.get("source_state_sha256"),
            restorable=bool(data.get("restorable", False)),
            replayed=bool(data.get("replayed", False)),
            manifest=_unwrap_sonic(data.get("manifest")),
            reproducibility=data.get("reproducibility"),
            tool_policies=_unwrap_sonic(data.get("tool_policies")),
        )

    @classmethod
    def from_metadata(cls, meta: "ChannelMetadata") -> "ForkReceipt":
        """Build a receipt from a :class:`ChannelMetadata` (used when a server
        or mock returns metadata-shaped data instead of the full receipt)."""
        return cls(
            fork_id=meta.channel_id,
            source_channel_id=meta.parent_channel_id,
            child_channel_id=meta.channel_id,
            requested_cursor=meta.requested_at_cursor,
            resolved_cursor=meta.resolved_cursor,
            resolved_event_id=meta.resolved_event_id,
            source_state_version=meta.source_state_version,
            source_state_sha256=meta.source_state_sha256,
            restorable=meta.restorable,
            replayed=False,
            manifest=meta.manifest,
            reproducibility=meta.reproducibility,
            tool_policies=getattr(meta, "tool_policies", None),
        )


@dataclass
class ForkInfo:
    """Recursive execution tree node returned by ``get_fork_tree()``."""

    channel_id: str                                #: Channel identifier for this fork
    display_name: Optional[str]                    #: Human-readable display name
    reason: Optional[str]                          #: Reason for this fork fork
    forked_at_cursor: int                          #: Server cursor at fork point
    event_count: int                               #: Number of events in this fork
    latest_cursor: Optional[int]                   #: Latest cursor in this fork (null if empty)
    children: List['ForkInfo'] = field(default_factory=list)  #: Child forks (forks of this fork)


@dataclass
class Event:
    """A single event persisted in an Actae channel.

    Events are the core unit of data in Actae. Every mutation — an agent step,
    a tool call, a state transition — is recorded as an event with a
    monotonic cursor that determines ordering.
    """

    id: str                                        #: Unique event identifier (UUID v7)
    channel_id: str                                #: Channel this event belongs to
    event_type: str                                #: User-defined type (e.g. ``"agent.step"``, ``"tool.call"``)
    payload: Any                                   #: Event payload (JSON-serializable)
    actor: str                                     #: Actor identifier (e.g. ``"agent_session"``, ``"system"``)
    cursor: int                                    #: Gapless per-channel monotonic cursor (≥1, never skips)
    channel_cursor: Optional[int]                  #: Deprecated alias of ``cursor`` (same value)
    timestamp: str                                 #: ISO 8601 timestamp of event persistence
    agent_id: Optional[str] = None                 #: Agent identifier (if actor is an agent)
    user_id: Optional[str] = None                  #: User identifier (if actor is a user)
    metadata: Optional[Dict[str, Any]] = None      #: Arbitrary event metadata
    depends_on: Optional[str] = None               #: ID of the causally-depended-on parent event

    @classmethod
    def from_record(cls, data: Dict[str, Any]) -> "Event":
        """Parse the response body from ``POST /api/v1/events/record``."""
        return cls(
            id=data["id"],
            channel_id=data.get("channel_id", ""),
            event_type=data["type"],
            payload=_unwrap_sonic(data["payload"]),
            actor=data["actor"],
            cursor=data["cursor"],
            channel_cursor=data.get("channel_cursor"),
            timestamp=data["timestamp"],
            depends_on=data.get("depends_on"),
        )

    @classmethod
    def from_replay(cls, data: Dict[str, Any]) -> "Event":
        """Parse an event from ``GET /api/v1/events/replay/{channel_id}``."""
        return cls(
            id=data["id"],
            channel_id=data.get("channel_id", ""),
            event_type=data["type"],
            payload=_unwrap_sonic(data["payload"]),
            actor=data["actor"],
            cursor=data["cursor"],
            channel_cursor=data.get("channel_cursor"),
            timestamp=data["timestamp"],
            agent_id=data.get("agent_id"),
            metadata=data.get("metadata"),
            depends_on=data.get("depends_on"),
        )

    @classmethod
    def from_query(cls, data: Dict[str, Any]) -> "Event":
        """Parse an event from ``POST /api/v1/events/query``."""
        return cls(
            id=data["id"],
            channel_id=data.get("channel_id", ""),
            event_type=data["type"],
            payload=_unwrap_sonic(data["payload"]),
            actor=data["actor"],
            cursor=data["cursor"],
            channel_cursor=data.get("channel_cursor"),
            timestamp=data["timestamp"],
            agent_id=data.get("agent_id"),
            metadata=data.get("metadata"),
            depends_on=data.get("depends_on"),
        )

    @classmethod
    def from_broadcast(cls, data: Dict[str, Any]) -> "Event":
        """Parse an event from a WebSocket broadcast message."""
        return cls(
            id=data["id"],
            channel_id=data.get("channel_id", ""),
            event_type=data.get("event_type") or data.get("type", "broadcast"),
            payload=_unwrap_sonic(data["payload"]),
            actor=data.get("actor", ""),
            cursor=data.get("cursor", 0),
            channel_cursor=data.get("channel_cursor"),
            timestamp=data.get("timestamp", ""),
            metadata=data.get("metadata"),
            depends_on=data.get("depends_on"),
        )


@dataclass
class ExecutionGroup:
    group_id: str
    status: str
    metadata: Dict[str, Any]
    created_at: str
    updated_at: str

    @classmethod
    def from_response(cls, data: Dict[str, Any]) -> "ExecutionGroup":
        data = data.get("group", data)
        return cls(data["group_id"], data.get("status", "active"), _unwrap_sonic(data.get("metadata") or {}), data.get("created_at", ""), data.get("updated_at", ""))


@dataclass
class ExecutionGroupMember:
    group_id: str
    member_id: str
    channel_id: str
    role: Optional[str]
    metadata: Dict[str, Any]
    owner_id: Optional[str]
    generation: int
    lease_until: Optional[str]

    @classmethod
    def from_response(cls, data: Dict[str, Any]) -> "ExecutionGroupMember":
        data = data.get("member", data)
        return cls(data["group_id"], data["member_id"], data["channel_id"], data.get("role"), _unwrap_sonic(data.get("metadata") or {}), data.get("owner_id"), int(data.get("generation", 0)), data.get("lease_until"))


@dataclass
class MemberLease:
    group_id: str
    member_id: str
    owner_id: str
    generation: int
    lease_until: str

    @classmethod
    def from_response(cls, data: Dict[str, Any]) -> "MemberLease":
        data = data.get("lease", data)
        return cls(data["group_id"], data["member_id"], data["owner_id"], int(data["generation"]), data["lease_until"])


@dataclass
class GroupMessage:
    message_id: str
    group_id: str
    from_member_id: str
    to_member_id: str
    message_type: str
    payload: Any
    causal_context: Optional[Dict[str, Any]]
    source_event_id: str
    delivery_event_id: str
    status: str
    acknowledged_at: Optional[str]

    @classmethod
    def from_response(cls, data: Dict[str, Any]) -> "GroupMessage":
        data = data.get("message", data)
        return cls(data["message_id"], data["group_id"], data["from_member_id"], data["to_member_id"], data.get("type", ""), _unwrap_sonic(data.get("payload")), _unwrap_sonic(data.get("causal_context")) if data.get("causal_context") is not None else None, data["source_event_id"], data["delivery_event_id"], data.get("status", "delivered"), data.get("acknowledged_at"))

    @classmethod
    def from_websocket_event(cls, event: Event) -> "GroupMessage":
        """Build a delivery from the low-latency ``message.received`` event."""
        envelope = event.payload if isinstance(event.payload, dict) else {}
        source = envelope.get("from") if isinstance(envelope.get("from"), dict) else {}
        target = envelope.get("to") if isinstance(envelope.get("to"), dict) else {}
        return cls(
            str(envelope.get("message_id", "")),
            str(envelope.get("group_id", "")),
            str(source.get("member", "")),
            str(target.get("member", "")),
            str(envelope.get("type", "")),
            _unwrap_sonic(envelope.get("payload")),
            _unwrap_sonic(envelope.get("causal_context")) if envelope.get("causal_context") is not None else None,
            "",
            event.id,
            "delivered",
            None,
        )


@dataclass
class HealthComponent:
    """Status of a single component within the server health check."""

    status: str                                    #: Component status (``"ok"``, ``"degraded"``, or ``"down"``)
    details: Dict[str, Any] = field(default_factory=dict)  #: Component-specific details


@dataclass
class HealthStatus:
    """Server health check result from ``GET /healthz``."""

    status: str                                    #: Overall health (``"ok"`` or ``"unavailable"``)
    timestamp: str                                 #: ISO 8601 timestamp of the health check
    instance_id: str                               #: Unique server instance identifier
    components: Dict[str, HealthComponent]         #: Per-component health status
    check_duration_ms: int                         #: Duration of the health check in milliseconds

    @classmethod
    def from_response(cls, data: Dict[str, Any]) -> "HealthStatus":
        components = {}
        for name, comp in data.get("components", {}).items():
            components[name] = HealthComponent(
                status=comp.get("status", "unknown"),
                details={k: v for k, v in comp.items() if k != "status"},
            )
        return cls(
            status=data.get("status", "unknown"),
            timestamp=data.get("timestamp", ""),
            instance_id=data.get("instance_id", ""),
            components=components,
            check_duration_ms=data.get("check_duration_ms", 0),
        )


@dataclass
class TransitionResult:
    """Result of an atomic event+state transition (``client.transition()``)."""

    event: "Event"                                 #: The persisted event
    state_version: int                             #: Version of the state snapshot written with the event


@dataclass
class GroupInfo:
    """A durable consumer group bound to a channel."""

    group_id: str                                  #: Unique group identifier
    channel_id: str                                #: Channel the group consumes
    created_at: str                                #: ISO 8601 creation timestamp
    metadata: Optional[Dict[str, Any]] = None      #: Arbitrary group metadata

    @classmethod
    def from_response(cls, data: Dict[str, Any]) -> "GroupInfo":
        return cls(
            group_id=data.get("group_id", ""),
            channel_id=data.get("channel_id", ""),
            created_at=data.get("created_at", ""),
            metadata=_unwrap_sonic(data.get("metadata")),
        )


@dataclass
class GroupOffset:
    """A consumer's durable offset state within a group."""

    group_id: str                                  #: Group identifier
    consumer_id: str                               #: Consumer identifier
    last_cursor: int                               #: Last acknowledged channel_cursor
    claimed_cursor: int                            #: Highest channel_cursor currently claimed
    updated_at: str                                #: ISO 8601 last update timestamp

    @classmethod
    def from_response(cls, data: Dict[str, Any]) -> "GroupOffset":
        return cls(
            group_id=data.get("group_id", ""),
            consumer_id=data.get("consumer_id", ""),
            last_cursor=int(data.get("last_cursor", 0)),
            claimed_cursor=int(data.get("claimed_cursor", 0)),
            updated_at=data.get("updated_at", ""),
        )


@dataclass
class ClaimedWork:
    """A work batch claimed by a consumer."""

    group_id: str                                  #: Group identifier
    consumer_id: str                               #: Consumer identifier
    events: List["Event"]                          #: Claimed events (ordered by channel_cursor)
    lease_until: str                               #: ISO 8601 lease expiry; heartbeat or re-join before this

    @classmethod
    def from_response(cls, data: Dict[str, Any]) -> "ClaimedWork":
        return cls(
            group_id=data.get("group_id", ""),
            consumer_id=data.get("consumer_id", ""),
            events=[Event.from_replay(e) for e in data.get("events", [])],
            lease_until=data.get("lease_until", ""),
        )


@dataclass
class Wakeup:
    """A persisted scheduled wake-up."""

    id: str                                        #: Wake-up UUID
    channel_id: str                                #: Channel that receives the ``scheduler.wakeup`` event
    run_at: str                                    #: ISO 8601 scheduled fire time
    status: str                                    #: ``pending`` | ``done`` | ``failed`` | ``cancelled``
    payload: Optional[Dict[str, Any]] = None       #: Arbitrary wake-up payload
    created_at: str = ""                           #: ISO 8601 creation timestamp
    executed_at: Optional[str] = None              #: ISO 8601 fire time (null until dispatched)
    attempts: int = 0                              #: Dispatch attempt count
    error: Optional[str] = None                    #: Last dispatch error (failed wake-ups only)

    @classmethod
    def from_response(cls, data: Dict[str, Any]) -> "Wakeup":
        return cls(
            id=data.get("id", ""),
            channel_id=data.get("channel_id", ""),
            run_at=data.get("run_at", ""),
            status=data.get("status", "pending"),
            payload=_unwrap_sonic(data.get("payload")),
            created_at=data.get("created_at", ""),
            executed_at=data.get("executed_at"),
            attempts=int(data.get("attempts", 0)),
            error=data.get("error"),
        )


@dataclass
class ExecutionInfo:
    """A single idempotent tool execution ledger record.

    ``status`` is one of ``running | completed | failed | interrupted |
    cancelled | expired``. ``attempts`` increments on every claim/reclaim.
    ``params`` holds the tool input, ``result``/``error`` the persisted
    terminal payloads.
    """

    id: str                                          #: Execution UUID
    channel_id: str                                  #: Channel the execution belongs to
    key_name: str                                    #: Idempotency key (unique per channel)
    tool_name: str                                   #: Logical tool name (e.g. ``"charge_customer"``)
    status: str                                      #: running | completed | failed | interrupted | cancelled | expired
    attempts: int                                    #: Number of start attempts (claims)
    lease_until: Optional[str] = None                #: ISO 8601 lease expiry while running
    started_cursor: Optional[int] = None             #: Channel cursor of the ``tool.started`` event
    completed_cursor: Optional[int] = None           #: Channel cursor of the terminal event
    started_event_id: Optional[str] = None           #: ID of the ``tool.started`` event
    completed_event_id: Optional[str] = None         #: ID of the terminal event
    replay_emitted: bool = False                     #: Whether a ``tool.replayed`` marker was recorded
    params: Optional[Dict[str, Any]] = None          #: Tool input parameters (JSON)
    result: Optional[Any] = None                     #: Persisted result (completed executions)
    error: Optional[Any] = None                      #: Persisted structured error (failed executions)
    created_at: str = ""                             #: ISO 8601 creation timestamp
    updated_at: str = ""                             #: ISO 8601 last update timestamp
    completed_at: Optional[str] = None               #: ISO 8601 terminal timestamp

    @classmethod
    def from_response(cls, data: Dict[str, Any]) -> "ExecutionInfo":
        return cls(
            id=data.get("id", ""),
            channel_id=data.get("channel_id", ""),
            key_name=data.get("key_name", ""),
            tool_name=data.get("tool_name", ""),
            status=data.get("status", ""),
            attempts=int(data.get("attempts", 0)),
            lease_until=data.get("lease_until"),
            started_cursor=data.get("started_cursor"),
            completed_cursor=data.get("completed_cursor"),
            started_event_id=data.get("started_event_id"),
            completed_event_id=data.get("completed_event_id"),
            replay_emitted=bool(data.get("replay_emitted", False)),
            params=_unwrap_sonic(data.get("params")),
            result=_unwrap_sonic(data.get("result")),
            error=_unwrap_sonic(data.get("error")),
            created_at=data.get("created_at", ""),
            updated_at=data.get("updated_at", ""),
            completed_at=data.get("completed_at"),
        )


@dataclass
class ExecutionClaim:
    """Outcome of claiming an idempotent tool execution.

    ``status`` is one of:
    - ``claimed`` — this caller owns the run; call ``complete_execution`` /
      ``fail_execution`` / ``heartbeat_execution`` with ``claim_token``.
    - ``replayed`` — a completed execution with a matching request was found;
      ``result`` carries the persisted response and the tool was not run.
    - ``reclaimed`` — a stale/abandoned run was taken over (prior outcome
      ambiguous).
    - ``in_progress`` — another owner holds a valid lease.
    """

    status: str                                      #: claimed | replayed | reclaimed | in_progress
    execution: "ExecutionInfo"                       #: The execution ledger record
    result: Optional[Any] = None                     #: Persisted result (replayed only)
    claim_token: Optional[str] = None                #: Ownership token (claimed/reclaimed only)

    @classmethod
    def from_response(cls, data: Dict[str, Any]) -> "ExecutionClaim":
        return cls(
            status=data.get("status", ""),
            execution=ExecutionInfo.from_response(data.get("execution", {})),
            result=_unwrap_sonic(data.get("result")),
            claim_token=data.get("claim_token"),
        )
