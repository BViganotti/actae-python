# Actae Python SDK Reference

For new application integrations, start with `Actae.from_env()` rather than
threading `ActaeClient` through every function. `Actae` provides async and
sync run scopes, ambient context, `effect`/`tool` decorators, native framework
providers, OpenAI Agents `RunState` persistence, and orchestrator carriers.
The complete short path is in `docs/SDK_INTEGRATION.md`; every lower-level API
in this reference remains available through `actae.client`.

Actae client for event recording, replay, WebSocket messaging,
and agent workflow instrumentation (fork/resume/compare).

## Package

```text
actae-client>=1.0.0     Python >=3.9    Dep: aiohttp>=3.9
```

## Import

```python
from actae_client import (
    Actae,                     # High-level integration facade
    ActaeCarrier,              # Credential-free activity/workflow lineage
    ActaeClient,               # Main client
    Event,                     # Event dataclass
    HealthStatus,              # Health check result
    ReadinessResult,           # Readiness probe result
    MetricsSnapshot,           # Metrics snapshot
    AuthResult,                # Login/signup result
    UserInfo,                  # User profile
    ChannelMetadata,           # Channel/fork metadata
    ForkInfo,                # Execution tree node
    HealthComponent,           # Health sub-component
    AgentSession,              # Agent instrumentation
    ActaeError,                 # Base exception
    AuthError,                 # Auth failure
    ActaeConnectionError,    # WS connection failure (never shadows builtins.ConnectionError)
    APIError,                  # HTTP error status
    RateLimitError,            # HTTP 429
    SessionError,              # AgentSession lifecycle error
    SessionCompletedError,     # Step on completed session
    SessionLockedError,        # Fenced channel owned by another live owner
)
# Framework adapters:
from actae_client.adapters.base import StateManager
from actae_client.adapters.langgraph import ActaeCheckpointSaver
from actae_client.adapters.crewai import ActaeCrewStateHook, CrewAIResumer
from actae_client.adapters.langchain import ActaeContextSaver, ChainResumer
from actae_client.adapters.claude import ActaeClaudeSessionStore, ActaeClaudeHook
from actae_client.adapters.openai_agents import install_actae_tracing
```

---

## High-level integration facade

`Actae.from_env()` reads `ACTAE_URL` and `ACTAE_API_KEY`, keeps the existing
`ActaeClient` at `actae.client`, and adds ambient run scopes. It is additive:
use lower-level client methods whenever an integration needs them.

```python
from actae_client import Actae

actae = Actae.from_env()

@actae.observe(lambda job: job.id, framework="worker")
async def process(job):
    return await existing_process(job)

@actae.effect("invoice:{invoice_id}:send", pass_cancel_event=True)
async def send_invoice(invoice_id, cancelled):
    # Pass `cancelled` to an HTTP client / poll it during cooperative work.
    return await mailer.send(invoice_id)
```

`run()` / `run_sync()` open an explicit boundary; `current()` returns its
`ActaeScope`; `scope.child()` / `child_sync()` retain parent lineage;
`scope.record()` / `record_sync()` retain the framework run channel. The
effect decorator is for mutations and is ledger-backed. `tool` is for
read-only observation and does not claim an execution lease.

For Temporal and Durable Functions, call only the pure
`actae.orchestrator("temporal").carrier(workflow_id)` in replayed workflow
code. Serialize its `to_dict()` output as normal activity input, then call
`orchestrator.activity(activity_id, parent=carrier, attempt=attempt)` in the
worker. The carrier intentionally contains no client or credentials.

---

## ActaeClient

The primary entry point. **Always use as an async context manager** unless you
need manual lifecycle control.

```python
async with ActaeClient(api_key="sk-...", endpoint="http://localhost:8002") as actae:
    ...
```

### Constructor

```python
ActaeClient(
    *,
    api_key: str,                                    # Required. API key for auth.
    endpoint: str = "",                              # HTTP base URL.
    ws_endpoint: str = "",                           # WebSocket URL. Auto-derived from endpoint if omitted.
    ssl_context: Optional[SSLContext] = None,         # Pre-built SSL context.
    client_cert: Optional[str] = None,               # mTLS client cert path.
    client_key: Optional[str] = None,                # mTLS client key path.
    ca_cert: Optional[str] = None,                   # Custom CA cert path.
    timeout: float = 30.0,                           # HTTP + WS timeout seconds.
    auto_reconnect: bool = True,                     # Auto-reconnect WS on drop.
    echo_self: bool = False,                         # Deliver own publishes to on_message/stream().
) -> ActaeClient
```

- Raises `ValueError` if `api_key` is empty or neither endpoint is provided.
- If only `endpoint` is given, `ws_endpoint` is derived by replacing scheme
  (`https://` → `wss://`, `http://` → `ws://`).

### Lifecycle

```python
# Context manager (preferred):
async with ActaeClient(...) as actae:
    await actae.subscribe("my-channel")

# Manual:
client = ActaeClient(...)
await client.connect()
# ... use it ...
await client.disconnect()
```

`connect()` is safe to call multiple times — subsequent calls are no-ops.
`disconnect()` cancels reader/reconnect tasks, closes WS + HTTP session.

### Callbacks

Register before calling `connect()` or `subscribe()`. `on_message`
callbacks **accumulate** (all registered callbacks receive every
broadcast); the other callback slots are single-slot (later registration
replaces).

```python
actae.on_message(lambda topic, event: print(topic, event.event_type, event.payload))
actae.on_error(lambda msg: print("WS error:", msg))
actae.on_subscribed(lambda topic, cursor: print(f"Subscribed to {topic} at {cursor}"))
actae.on_disconnected(lambda: print("WS disconnected"))
actae.on_reconnect(lambda: print("WS reconnected"))
```

| Method | Callback Signature | When Fires |
|--------|-------------------|------------|
| `on_message` | `(topic: str, event: Event) -> None` | Broadcast received on subscribed topic |
| `on_error` | `(message: str) -> None` | Server error frame or subscription error |
| `on_subscribed` | `(topic: str, cursor: int \| None) -> None` | Subscription confirmed |
| `on_disconnected` | `() -> None` | WebSocket connection lost |
| `on_reconnect` | `() -> None` | Successful auto-reconnect completed |

### Events — HTTP API

#### `record(channel_id, event_type, payload, *, actor, agent_id=None, user_id=None, metadata=None, operation_id=None) -> Event`

`POST /api/v1/events/record`

Record a new event on a channel.

```python
event = await actae.record(
    "my-channel", "agent.step",
    {"input": "hello", "output": "world"},
    actor="my-agent",
    metadata={"model": "gpt-4"},
)
print(event.id, event.cursor)  # UUID v7, monotonic cursor
```

`operation_id` (optional) makes the record idempotent: retrying with the
same `(channel_id, operation_id)` returns the original persisted event
instead of inserting a duplicate — pass a stable UUID to make request
retries safe against lost responses.

The returned `Event` is self-consistent: `metadata`, `agent_id` and
`user_id` are filled from the call arguments (the record response omits
them).

#### `replay(channel_id, *, cursor=None, limit=100, event_type=None) -> List[Event]`

`GET /api/v1/events/replay/{channel_id}`

Replay events from a channel. `cursor` is an exclusive start cursor — events with a per-channel cursor strictly greater than this value are returned (pass `N - 1` for events up to and including N). Cursors are per-channel (each channel numbers its events 1, 2, 3, … independently and gapless). Limit defaults to 100; the server caps at 1000 per call (page with `cursor` for more).

#### `query(*, channel_ids=None, event_type=None, actor=None, cursor_start=None, cursor_end=None, from_time=None, to_time=None, limit=100, offset=None) -> List[Event]`

`POST /api/v1/events/query`

Cross-channel event search with flexible filters. `from_time`/`to_time` are ISO 8601 strings. `cursor_start`/`cursor_end` are per-channel cursors; results carry each event's own per-channel `cursor`, ordered by global insertion time (newest first).

#### `get_cursor(channel_id) -> Optional[int]`

`GET /api/v1/events/cursor/{channel_id}`

Latest cursor on the channel. Returns `None` if channel has no events.

#### `latest_cursor(channel_id) -> Optional[int]`

Alias for `get_cursor`.

### Events — Atomic Transitions

#### `transition(channel_id, event_type, payload, state, *, actor="system", agent_id=None, user_id=None, metadata=None, operation_id=None, expected_version=None, expected_cursor=None) -> TransitionResult`

`POST /api/v1/events/transition` — commits the event and the state snapshot
in one transaction. Returns `TransitionResult(event, state_version)`. Raises
`VersionConflictError` when a guard mismatches.

### Consumer Groups — HTTP API (durable workers)

| Method | Server | Notes |
|--------|--------|-------|
| `create_group(group_id, channel_id, *, metadata=None) -> GroupInfo` | `POST /api/v1/groups` | Group bound to a channel |
| `list_groups(*, channel_id=None) -> List[GroupInfo]` | `GET /api/v1/groups` | Filter by channel |
| `delete_group(group_id) -> None` | `DELETE /api/v1/groups/{id}` | Cascades offsets |
| `join_group(group_id, consumer_id, *, lease_seconds=60) -> GroupOffset` | `POST .../join` | Lease + durable offset |
| `claim_work(group_id, consumer_id, *, limit=100) -> ClaimedWork` | `POST .../work` | At-least-once batch; `409 consumer_not_found` without a lease |
| `ack_work(group_id, consumer_id, cursor) -> None` | `POST .../ack` | Commits a watermark: pass the highest contiguously processed **channel** cursor; acking higher skips events permanently |
| `heartbeat(group_id, consumer_id, *, lease_seconds=60) -> None` | `POST .../heartbeat` | Extends lease |
| `group_offsets(group_id) -> List[GroupOffset]` | `GET .../offsets` | last/claimed cursors |

### Wake-ups — HTTP API (persisted scheduling)

| Method | Server | Notes |
|--------|--------|-------|
| `schedule_wakeup(channel_id, run_at, *, payload=None) -> Wakeup` | `POST /api/v1/scheduler/wakeups` | Fires `scheduler.wakeup` event at run_at; `run_at` is an RFC 3339 string or a timezone-aware `datetime` (naive raises `ValueError`) |
| `list_wakeups(*, channel_id=None, status=None) -> List[Wakeup]` | `GET /api/v1/scheduler/wakeups` | Filter by channel/status |
| `get_wakeup(wakeup_id) -> Optional[Wakeup]` | `GET .../{id}` | None on 404 |
| `cancel_wakeup(wakeup_id) -> bool` | `DELETE .../{id}` | False when not pending |

### Human Approval — HTTP API (durable human-in-the-loop)

Records `approval.requested` / `approval.decided` / `approval.expired` events.
The agent process need not stay alive; a timeout schedules a persisted
wake-up; Actae never resumes anything itself. Decisions are idempotent per
`(request_id, decision)` — a retried/duplicated decision replays; a different
decision is a new event.

| Method | Server | Notes |
|--------|--------|-------|
| `request_approval(channel_id, *, summary, details=None, timeout_seconds=None, requester="agent", request_id=None) -> str` | `POST /events/record` (+ wake-up) | Returns the request id |
| `decide_approval(channel_id, request_id, *, decision, actor, reason=None, operation_id=None) -> Event` | `POST /events/record` | decision ∈ approved/rejected; deterministic operation id |
| `wait_for_approval(channel_id, request_id, *, timeout_seconds=None, poll_interval=1.0, start_cursor=0) -> Optional[Event]` | polls `GET /events/replay` | None on timeout (records `approval.expired`) |

### Idempotent Tool Executions — HTTP API

Durable, idempotent tool execution ledger keyed by `(channel_id, key_name)`.
A completed execution replays its persisted result for retries; a mismatch
raises `IdempotencyKeyMismatchError`. Ownership of a running execution is
proven by `claim_token` (stale tokens raise `ExecutionNotOwnedError`; missing
executions raise `ExecutionNotFoundError`).

| Method | Server | Notes |
|--------|--------|-------|
| `claim_execution(channel_id, key_name, tool_name, params=None, *, dedup_fields=None, lease_seconds=None, emit_replay_event=False, actor=None) -> ExecutionClaim` | `POST /api/v1/executions/claim` | `status` ∈ claimed/replayed/reclaimed/in_progress; claim_token only for claimed/reclaimed |
| `complete_execution(execution_id, claim_token=None, result=None) -> ExecutionInfo` | `POST .../{id}/complete` | Duplicate complete idempotent |
| `fail_execution(execution_id, message, *, claim_token=None, error_type=None, stack=None) -> ExecutionInfo` | `POST .../{id}/fail` | Duplicate fail idempotent |
| `heartbeat_execution(execution_id, claim_token=None, lease_seconds=None) -> ExecutionInfo` | `POST .../{id}/heartbeat` | Extends lease |
| `cancel_execution(execution_id, claim_token=None) -> ExecutionInfo` | `POST .../{id}/cancel` | Marks cancelled |
| `get_execution(execution_id) -> ExecutionInfo` | `GET .../{id}` | 404 → ExecutionNotFoundError |
| `list_executions(channel_id, limit=50) -> List[ExecutionInfo]` | `GET /api/v1/executions` | Newest first, limit ≤ 100 |
| `delete_execution(execution_id) -> bool` | `DELETE .../{id}` | True when removed |

Lifecycle events (`tool.started`/`tool.completed`/`tool.failed`/`tool.replayed`)
broadcast to WS subscribers and arrive at `on_message(topic, event)` with
`event.event_type` set to the real type and `event.metadata` containing
`{"execution_id": ...}`.

### State Snapshots — HTTP API

Immutable, versioned, cursor-aligned. Each save creates a new version. Never
overwrites. State blob is opaque to Actae.

| Method | HTTP | Returns |
|--------|------|---------|
| `save_state(channel_id, cursor, state, *, expected_version=None, expected_cursor=None) -> int` | `POST /api/v1/state/{channel_id}` | Version number; 409 `version_conflict` on stale guards |
| `latest_state(channel_id) -> dict \| None` | `GET /api/v1/state/{channel_id}` | `{"cursor": int, "state": ...}` or None |
| `list_states(channel_id, *, limit=100, offset=0) -> list[dict]` | `GET /api/v1/state/{channel_id}/versions` | `[{"version": 1, "cursor": 42, ...}]` |
| `get_state(channel_id, version) -> dict \| None` | `GET /api/v1/state/{channel_id}/version/{version}` | Full snapshot or None |
| `delete_state(channel_id, version) -> None` | `DELETE /api/v1/state/{channel_id}/version/{version}` | — |

#### `diff_states(left, right) -> dict` — counterfactual debugging

`GET /api/v1/channels/diff?left={left}&right={right}`

Structural diff of two channels' latest saved states (not event lists).
Returns `{left, right, common, left_diverged_at_cursor, right_diverged_at_cursor, entries, truncated, entry_count_total, max_entries}` where each
entry is `{path: [...], kind: "added"|"removed"|"changed", left, right}`.
`added` = right-only, `removed` = left-only, `changed` = both differ.
`common` is the shared ancestor state when both channels share an
`origin_run_id` (sibling forks), else `None`. This is the primitive behind
"failing run → two forks → diff proves which fix worked" (see
`docs/COUNTERFACTUAL.md`).

#### `decision_trail(channel_id) -> dict` — lineage as audit

`GET /api/v1/channels/{channel_id}/trail`

Returns `{channel_id, origin_run_id, ancestry: [...], boundary, executions}`
where `ancestry` is the parent chain back to the root run, `boundary` is the
state the fork forked from, and `executions` is the channel's idempotent
`tool_executions` ledger rows. Every decision in a run traces back to a fork
point with the tool calls that produced each state.

### Channels & Fork — HTTP API

| Method | HTTP | Returns |
|--------|------|---------|
| `list_channels() -> list[str]` | `GET /api/v1/channels` | Channel ID strings |
| `fork(source, new, at_cursor=0, *, display_name=None, reason=None, experiment_metadata=None, operation_id=None, manifest=None, tool_policies=None, expected_version=None, expected_cursor=None) -> ForkReceipt` | `POST /api/v1/channels/fork` | Fork at exact boundary (0 = latest). Returns the **immutable ForkReceipt** (requested/resolved cursors, source state version + SHA-256, restorable, reproducibility, tool_policies). 409 `snapshot_boundary_required` on miss, `idempotency_conflict` on request drift, `channel_conflict` on child-id reuse under a different definition; idempotent via operation_id. `tool_policies` sets the child's side-effect policy (`replay`/`block`/`live`/`auto`, default `auto`) |
| `resolve_step(channel_id, step_number) -> tuple[str, int, str \| None] \| None` | `GET /api/v1/channels/{id}/steps/{n}` | `(owner, cursor, event_id)` via the server step index (walks lineage); None on 404 |
| `latest_step_number(channel_id) -> int \| None` | `GET /api/v1/channels/{id}/steps` | Highest indexed step (0 = none); O(1) crash-recovery probe (None if the route is unavailable) |
| `get_channel_metadata(channel_id) -> ChannelMetadata \| None` | `GET /api/v1/channels/{id}/metadata` | None on 404 |
| `list_forks(channel_id) -> list[ChannelMetadata]` | `GET /api/v1/channels/{id}/forks` | Direct children only |
| `get_fork_tree(root_channel_id) -> ForkInfo` | `GET /api/v1/channels/{root_id}/fork-tree` | Recursive tree |
| `update_metadata(channel_id, *, display_name=None, reason=None, experiment_metadata=None) -> ChannelMetadata` | `PUT /api/v1/channels/{id}/metadata` | Partial update |
| `diff_states(left, right) -> dict` | `GET /api/v1/channels/diff?left=&right=` | Structural diff of two channels' latest states — see below |
| `decision_trail(channel_id) -> dict` | `GET /api/v1/channels/{id}/trail` | Lineage chain + fork boundary state + tool-execution ledger |

### Health — HTTP API

| Method | HTTP | Returns |
|--------|------|---------|
| `health_check() -> HealthStatus` | `GET /healthz` | Component-level health |
| `readiness_check() -> ReadinessResult` | `GET /readyz` | DB, capacity |
| `get_metrics_text() -> str` | `GET /metrics` | Prometheus text format |
| `get_metrics_json() -> MetricsSnapshot` | `GET /metrics.json` | Structured JSON metrics |

### Auth — HTTP API

| Method | HTTP | Notes |
|--------|------|-------|
| `signup(email, password, *, name=None) -> AuthResult` | `POST /api/v1/auth/signup` | Create account |
| `login(email, password) -> AuthResult` | `POST /api/v1/auth/login` | Get JWT token |
| `logout(jwt_token) -> None` | `POST /api/v1/auth/logout` | Invalidate token |
| `get_me(jwt_token) -> UserInfo` | `GET /api/v1/auth/me` | Current user profile |

### WebSocket

```python
async with ActaeClient(api_key="sk-...", endpoint="http://localhost:8002") as actae:
    actae.on_message(lambda t, e: print(f"[{t}] {e.event_type}: {e.payload}"))
    await actae.subscribe("channel-1", cursor=42)   # Replay from cursor 42, then live
    await actae.subscribe("channel-2", wait=True)    # Wait for confirmation

    event = await actae.publish("channel-1", {"msg": "hello"})
    print(f"Published at cursor {event.cursor}")

    await actae.unsubscribe("channel-2")
```

| Method | Notes |
|--------|-------|
| `connect()` | Open WS + auth. No-op if connected. Raises `ActaeConnectionError` or `AuthError`. |
| `disconnect()` | Close WS, cancel tasks, close HTTP session. |
| `subscribe(topic, *, cursor=None, wait=True)` | Subscribe. `cursor` replays events with a per-channel cursor strictly greater than it (exclusive, same semantics as `replay`). `wait` blocks for ack (up to the client's `timeout`, default). |
| `unsubscribe(topic)` | Unsubscribe. Raises `ActaeConnectionError` if not connected. |
| `publish(topic, payload, *, operation_id=None) -> Event` | Publish event. Waits for Ack (up to `timeout` seconds). Returns persisted Event. Publishes are serialized so each call gets its own Ack. `operation_id` (stable UUID) makes retries idempotent — same `(topic, operation_id)` replays the original event. |
| `stream(topic, *, cursor=None) -> async iterator[Event]` | `async for event in client.stream("ch"): ...` — subscribes (wait=True) then yields each broadcast; ends on disconnect. |
| `on_message(callback)` | Register callback `(topic, event)`. Multiple callbacks allowed, called in order. |

---

## Namespaced facades

Every `ActaeClient` exposes read-only facades grouping methods by concern.
They delegate to the client — `client.events.record(...)` is exactly
`client.record(...)`.

| Facade | Methods |
|--------|---------|
| `client.events` | `record`, `replay`, `query`, `transition`, `get_cursor`, `latest_cursor` |
| `client.state` | `save_state`, `latest_state`, `list_states`, `get_state`, `delete_state` |
| `client.channels` | `list_channels`, `fork`, `get_channel_metadata`, `list_forks`, `get_fork_tree`, `update_metadata`, `diff_states`, `decision_trail` |
| `client.executions` | `claim_execution`, `complete_execution`, `fail_execution`, `heartbeat_execution`, `cancel_execution`, `get_execution`, `list_executions`, `delete_execution` |
| `client.groups` | `create_group`, `list_groups`, `delete_group`, `join_group`, `claim_work`, `ack_work`, `heartbeat`, `group_offsets` |
| `client.wakeups` | `schedule_wakeup`, `list_wakeups`, `get_wakeup`, `cancel_wakeup` |
| `client.health` | `health_check`, `readiness_check`, `get_metrics_text`, `get_metrics_json` |
| `client.auth` | `signup`, `login`, `logout`, `get_me` |
| `client.ws` | `connect`, `disconnect`, `subscribe`, `unsubscribe`, `publish`, `stream`, `on_message`, `on_error`, `on_subscribed`, `on_disconnected`, `on_reconnect` |

---

## Synchronous facade

Blocking variants for non-async codebases. Each runs on a per-thread
daemon background loop (`actae_client._sync.run_sync`). Coverage is
partial by design — WebSocket methods, consumer groups, executions,
wake-ups, and `AgentSession` are async-only:

| Method | Mirrors |
|--------|---------|
| `record_sync(channel_id, event_type, payload, *, actor, ...) -> Event` | `record` |
| `replay_sync(channel_id, *, cursor=None, limit=100, event_type=None) -> List[Event]` | `replay` |
| `query_sync(*, ...filters..., limit=100, offset=None) -> List[Event]` | `query` |
| `get_cursor_sync(channel_id) -> Optional[int]` | `get_cursor` |
| `transition_sync(channel_id, event_type, payload, state, *, ...) -> TransitionResult` | `transition` |
| `fork_sync(source_channel_id, new_channel_id, at_cursor=0, *, ...) -> ChannelMetadata` | `fork` |
| `latest_state_sync(channel_id) -> Optional[dict]` | `latest_state` |
| `save_state_sync(channel_id, cursor, state, *, expected_version=None, expected_cursor=None) -> int` | `save_state` |
| `disconnect_sync() -> None` | `disconnect` — call at shutdown to release sessions |

```python
client = ActaeClient(endpoint="http://localhost:8002", api_key="sk-...")
event = client.record_sync("ch", "agent.step", {"ok": 1}, actor="me")
```

---

## Data Types

### Event

```python
@dataclass
class Event:
    id: str                     # Unique event identifier (UUID v7)
    channel_id: str             # Channel this event belongs to
    event_type: str             # User-defined type (e.g. "agent.step")
    payload: Any                # JSON-serializable payload
    actor: str                  # Who/what created the event
    cursor: int                 # Gapless per-channel monotonic cursor
    channel_cursor: Optional[int]  # Deprecated alias of cursor (same value)
    timestamp: str              # ISO 8601
    agent_id: Optional[str]     # Agent identifier
    user_id: Optional[str]      # User identifier
    metadata: Optional[dict]    # Arbitrary metadata
    depends_on: Optional[str]   # ID of the causally-depended-on parent event

    @classmethod from_record(data: dict) -> Event
    @classmethod from_replay(data: dict) -> Event
    @classmethod from_query(data: dict) -> Event
    @classmethod from_broadcast(data: dict) -> Event
```

### HealthStatus

```python
@dataclass
class HealthStatus:
    status: str                    # "ok" | "unavailable"
    timestamp: str                 # ISO 8601
    instance_id: str               # Server instance UUID
    components: dict[str, HealthComponent]  # Per-component status
    check_duration_ms: int         # Duration in ms
```

### HealthComponent

```python
@dataclass
class HealthComponent:
    status: str                    # "ok" | "degraded" | "down"
    details: dict                  # Component-specific details
```

### ReadinessResult

```python
@dataclass
class ReadinessResult:
    status: str                    # "ok" | "degraded" | "unavailable"
    timestamp: str                 # ISO 8601
    instance_id: str               # Server instance UUID
    check_duration_ms: int         # Duration in ms
    database_ready: bool           # PostgreSQL healthy
    capacity_available: bool       # Room for new connections
    uptime_seconds: int            # Server uptime
    connection_utilization: float  # 0.0–1.0
    active_connections: int        # Current WS connections
    max_connections: int           # Max WS connections
```

### MetricsSnapshot

```python
@dataclass
class MetricsSnapshot:
    status: str                    # "ok" | "unavailable"
    timestamp: str                 # ISO 8601
    uptime_seconds: int
    websocket_connections: int
    topic_count: int               # Distinct subscribed topics
    messages_sent: int             # Server→client
    messages_received: int         # Client→server
    total_messages: int            # sent + received
    back_pressure: dict            # Per-topic queue depth
```

### AuthResult

```python
@dataclass
class AuthResult:
    user: UserInfo                 # Authenticated user
    token: str                     # JWT token
```

### UserInfo

```python
@dataclass
class UserInfo:
    id: str                        # UUID
    email: str
    email_verified: bool
    name: Optional[str]
    image: Optional[str]           # Avatar URL
    created_at: Optional[str]      # ISO 8601
```

### ChannelMetadata

```python
@dataclass
class ChannelMetadata:
    channel_id: str
    parent_channel_id: Optional[str]   # Source channel (null for roots)
    origin_run_id: str                 # Root channel in execution tree
    forked_at_cursor: int           # RESOLVED boundary cursor (snapshot actually inherited)
    forked_at: str                     # ISO 8601
    display_name: Optional[str]
    reason: Optional[str]              # Why forked
    experiment_metadata: Optional[dict]
```

### ForkInfo

```python
@dataclass
class ForkInfo:
    channel_id: str
    display_name: Optional[str]
    reason: Optional[str]
    forked_at_cursor: int           # RESOLVED boundary cursor (snapshot actually inherited)
    latest_cursor: Optional[int]
    children: list[ForkInfo]         # Recursive
```

### ExecutionInfo

```python
@dataclass
class ExecutionInfo:
    id: str
    channel_id: str
    key_name: str
    tool_name: str
    status: str                        # running|completed|failed|interrupted|cancelled|expired
    attempts: int
    lease_until: Optional[str] = None
    started_cursor: Optional[int] = None
    completed_cursor: Optional[int] = None
    started_event_id: Optional[str] = None
    completed_event_id: Optional[str] = None
    replay_emitted: bool = False
    params: Optional[dict] = None
    result: Optional[Any] = None
    error: Optional[Any] = None
    created_at: str = ""
    updated_at: str = ""
    completed_at: Optional[str] = None
```

### ExecutionClaim

```python
@dataclass
class ExecutionClaim:
    status: str                        # claimed|replayed|reclaimed|in_progress
    execution: ExecutionInfo
    result: Optional[Any] = None       # persisted result (replayed only)
    claim_token: Optional[str] = None  # ownership token (claimed/reclaimed only)
```

---

## Errors

| Exception | Attributes | When Raised |
|-----------|-----------|-------------|
| `ActaeError` | — | Base class for all SDK errors |
| `AuthError` | — | HTTP 401 (bad/expired API key, wrong password on `login`), WS API key rejection, JWT expiry, auth timeout |
| `ActaeConnectionError` | — | WS handshake failure, subscription timeout, transport error |
| `APIError` | `.status_code: int` | HTTP non-2xx response (except 401/429/409/404 mapped above) |
| `RateLimitError` | `.retry_after_seconds: int` | HTTP 429 |
| `SnapshotBoundaryError` | — | HTTP 409 `snapshot_boundary_required` — fork `at_cursor` has no saved state boundary |
| `NoRestorableCheckpointError` | — | `AgentSession` strict (`boundary_mode="exact"`) fork/resume raised instead of silently falling forward to later state |
| `IdempotencyConflictError` | — | HTTP 409 `idempotency_conflict` — the same `operation_id` was reused with a different request |
| `ChannelConflictError` | — | HTTP 409 `channel_conflict` — `new_channel_id` already exists under a different source/boundary |
| `VersionConflictError` | — | HTTP 409 `version_conflict` — optimistic-concurrency guard mismatch |
| `ConsumerError` | — | HTTP 409 `consumer_not_found` — no lease / unknown group |
| `IdempotencyKeyMismatchError` | — | HTTP 409 `idempotency_key_mismatch` on claim with different request hash |
| `ExecutionNotOwnedError` | — | HTTP 409 `execution_not_owned` — stale claim token |
| `ExecutionNotFoundError` | — | HTTP 404 `execution_not_found` |
| `ForkToolBlockedError` | `.boundary: str \| None` | HTTP 409 `fork_tool_blocked` — a forked channel's tool policy refused a call |
| `SessionError` | — | AgentSession lifecycle violation |
| `SessionCompletedError` | — | `step()` on completed session |
| `SessionLockedError` | — | a fenced session's channel is owned by another live owner (or the fence was reclaimed) |

`ActaeConnectionError` is named so it never shadows Python's built-in
`ConnectionError` (which the SDK does not export).

---

## AgentSession

High-level instrumentation for agent execution pipelines. Wraps an Actae
channel and records each step as an event. Supports fork at any step,
crash recovery, and state snapshots.

### Lifecycle

```
created → started → stepping → completed
                              ↘ crashed
```

Not thread-safe. Use a single async task per session.

### Constructor

```python
AgentSession(
    actae: ActaeClient,
    name: str,                                          # Session name (used as channel ID)
    *,
    display_name: Optional[str] = None,                 # Human-readable name for the dashboard
    state_fn: Optional[Callable[[], Optional[dict]]] = None,  # Returns current agent state (sync or async fn)
    snapshot_interval: int = 1,                         # Save snapshot every N steps
    params: Optional[dict] = None,                      # Arbitrary session params
    fence_owner: Optional[str] = None,                  # Opt-in channel ownership fence
    fence_lease_seconds: int = 60,                      # Fence lease (renewed per step)
)
```

### Properties

```python
session.channel_id   # str | None — channel ID (None before __aenter__)
session.name         # str — session name
session.step_count   # int — steps recorded
session.status       # str — "created" | "started" | "stepping" | "completed" | "crashed"
session.cursors      # list[int] — cursor per step (index == step - 1)
session.params       # dict — session params (read-only copy)
session.intervention # dict — opaque fork intervention (application-applied)
session.tool_policies # dict | None — child side-effect policy (None = server default "auto")
```

### Methods

```python
# Record a step:
event = await session.step(
    "inference",                          # Event type
    input={"prompt": "..."},              # Step input
    output={"response": "..."},           # Step output
    context={                             # Optional delta context for this step
        "messages": [{"role": "assistant", "content": "..."}],
        "thinking": "Chain of thought...",
        "tool_results": [...],
    },
    metadata={"model": "gpt-4"},          # Optional metadata
)
# Returns the persisted Event (with cursor, id, timestamp)
# context is stored in the event payload and the dashboard reconstructs
# the cumulative full context at any node by merging deltas across the
# cursor-linked event chain.
# Steps retry transient failures safely: every retry of a step carries the
# same per-step operation_id, so the server replays the original event
# instead of recording a duplicate if the first attempt actually landed.
# The per-step operation_id is DETERMINISTIC (UUIDv5 over NUL-joined
# scope/type/channel/step_number/canonical payload+metadata, 256-byte cap,
# byte-identical to the Go SDK's DeterministicOperationKey and the TS SDK's
# deterministicOperationKey): a crash-recovery re-drive of the same step
# (same step_number, same content) derives the SAME id and replays; changed
# content records anew. The canonical serialization is Go-exact (payload
# first, recursively sorted keys, Go float formatting — 1.0 → 1, 1e-7,
# 1e20 fixed notation; NaN/Infinity raise ValueError, never a silently
# divergent id).

# Fork at a step:
fork_session = await session.fork(
    at_step=5,
    name="exp-gpt5",
    display_name="GPT-5 Experiment",      # Optional human-readable name
    params={"model": "gpt-5"},
    state_fn=my_state_fn,                 # Optional custom state fn for fork
    reason="Testing GPT-5",
    # Side-effect-safe forking:
    intervention={"model": "gpt-5"},      # Opaque descriptor the APP applies
    tool_policies={"email.send": "block", "*": "auto"},  # replay|block|live|auto
)
async with fork_session:
    # The application reads fork_session.intervention and applies it; Actae
    # never interprets it. A refused tool call raises ForkToolBlockedError.
    ...

# Durable human approval (the agent need not stay alive):
decision = await session.require_approval("Ship this release?", timeout_seconds=3600)
if decision["approved"]:
    ...

# Resume (class method):
session = await AgentSession.resume(
    actae,
    channel_id="exp-v1",                  # Channel to resume/fork from
    fork_at_step=5,                        # None = crash recovery, int = fork
    name="exp-v2",                         # Required when fork_at_step is set
    params={"model": "gpt-5"},
    state_fn=my_state_fn,
    snapshot_interval=1,
)
```

### Usage — First Run

```python
async with AgentSession(actae, name="exp-v1", state_fn=lambda: {"memory": mem}) as session:
    for i, item in enumerate(dataset):
        result = llm.call(item)
        await session.step("inference", input=item, output=result)
```

### Usage — Crash Recovery

```python
session = await AgentSession.resume(actae, "exp-v1")
async with session:
    for item in dataset[session.step_count:]:
        result = llm.call(item)
        await session.step("inference", input=item, output=result)
```

Raises `SessionCompletedError` if the session already completed. Fork first
to continue from a completed session.

### Usage — Fork from Existing

```python
session = await AgentSession.resume(
    actae, "exp-v1",
    fork_at_step=5, name="exp-gpt5",
    params={"model": "gpt-5"},
)
async with session:
    for item in dataset[5:]:
        result = llm.call(item, model="gpt-5")
        await session.step("inference", input=item, output=result)
```

### Step → Cursor Mapping

Each step records an event with a monotonic cursor. The mapping is stored
in the channel's `experiment_metadata.cursors` list (index == step - 1).
Capped at 1000 entries — beyond that, only the most recent cursors are stored
and older steps fall back to replay on fork.

Retry behavior: transient failures (ActaeConnectionError, 5xx, 429, timeout) are
retried up to 3 times with exponential backoff (0.5s → 1s → 2s → max 5s).

---

## Framework Adapters

### StateManager (generic)

```python
from actae_client.adapters.base import StateManager

mgr = StateManager(actae_client, channel="my-agent")

await mgr.save(state_dict)           # → version int
state = await mgr.load()             # → dict | None
state = await mgr.resume(default)    # → dict (load or default)
forked = await mgr.fork("my-agent-fork", reason="refine from here")  # → StateManager
versions = await mgr.list_versions() # → list[dict]
snap = await mgr.get_version(ver)    # → dict | None
await mgr.delete_version(ver)        # → None
```

`fork` copies the opaque state dict to a new channel — the framework-agnostic
primitive every adapter is built on.

### LangGraph — ActaeCheckpointSaver

```python
from actae_client.adapters.langgraph import ActaeCheckpointSaver

saver = ActaeCheckpointSaver(actae_client)          # channel prefix defaults to "langgraph"
graph = builder.compile(checkpointer=saver)

# Async (recommended):
result = await graph.ainvoke(input, {"configurable": {"thread_id": "1"}})

# Sync (fresh event loop; ActaeLangGraphSyncError inside a running loop):
result = graph.invoke(input, {"configurable": {"thread_id": "1"}})

# Resume (new process, same Actae):
config = {"configurable": {"thread_id": "1"}}
previous = graph.get_state(config)  # loads from Actae
graph.invoke(None, config)          # resumes from last checkpoint

# Fork & resume (refine a later step without re-running earlier ones):
fork_cfg = await saver.fork_thread(
    config, new_thread_id="1-fix", reason="refine node 5",
)
result = await graph.ainvoke(None, fork_cfg)   # nodes after 5 re-run, context intact
# graph.invoke users: use saver.fork_thread_sync(...)
```

`fork_thread` copies the thread's checkpoint (full graph state incl. the LLM
message context) into a new thread's channel; pass an explicit
`configurable.checkpoint_id` to fork at an intermediate step.

Protocol-complete LangGraph 1.x `BaseCheckpointSaver[str]`:
`aput`/`put`, `aput_writes`/`put_writes`, `aget_tuple`/`get_tuple`,
`alist`/`list`, `adelete_thread`/`delete_thread`; `config_specs` and
`get_next_version` (string versions `"{step:032}.{random:016}"`) are
implemented. Deprecated `save_checkpoint`/`get_checkpoint` shims remain
(emit `DeprecationWarning`).

Behavior / storage:
- Channel: `f"{prefix}:{sha256(thread_id \\x00 checkpoint_ns)[:16]}"` — one
  channel per (thread, namespace); subgraph state is isolated. `thread_id`
  is required; a `channel_resolver: (config) -> str` can override resolution.
- Snapshots: envelope v1 stored via `save_state`, aligned to the cursor of a
  recorded `langgraph.checkpoint` / `langgraph.pending_writes` event.
  Checkpoint + metadata are serialized losslessly with LangGraph's own serde
  (`dumps_typed` -> `{"t": type_tag, "b": base64}`); typed objects such as
  `AIMessage` with `tool_calls` round-trip exactly.
- `aput_writes`: merges per `(task_id, idx)` last-write-wins and persists a
  new snapshot version of the same checkpoint id (crash-recoverable
  interrupts). If the target checkpoint is not persisted yet (the runtime
  saves on a background executor and can race), writes are buffered in memory
  and flushed by the following `aput`.
- Sync bridge: `asyncio.run()` in a fresh loop, serialized by a threading
  lock; the client's per-loop aiohttp session is closed when the loop exits.
- Without `langgraph` installed the module still imports;
  `ActaeCheckpointSaver(...)` then raises `ImportError` pointing at
  `pip install 'actae-client[langgraph]'`.
- Errors: `ActaeLangGraphError`, `ActaeLangGraphSyncError` (both subclass
  `ActaeError`).

Migration: data written by the pre-1.1.0 fixed-channel adapter is retained
in Actae for audit but not read or migrated.

### CrewAI — ActaeCrewStateHook + CrewAIResumer

```python
from actae_client.adapters.crewai import ActaeCrewStateHook, CrewAIResumer

hook = ActaeCrewStateHook(actae_client, channel="my-crew")
crew = Crew(..., step_callback=hook.after_step)
result = crew.kickoff()

# Resume:
resumer = CrewAIResumer(actae_client, channel="my-crew")
remaining = await resumer.get_remaining_tasks(all_tasks)
if remaining:
    crew.kickoff(tasks=remaining)

# Or one-shot:
await resumer.resume(crew, all_tasks)  # returns None if all done
```

### LangChain — ActaeContextSaver + ChainResumer

```python
from actae_client.adapters.langchain import ActaeContextSaver, ChainResumer

saver = ActaeContextSaver(actae_client, channel="my-chain", save_every_n=3)
chain.invoke(input, config={"callbacks": [saver]})

# Resume:
resumer = ChainResumer(actae_client, channel="my-chain")
response = await resumer.resume(chain, default_input)
```

### Claude Agent SDK — ActaeClaudeSessionStore + ActaeClaudeHook

```python
from actae_client.adapters.claude import (
    ActaeClaudeSessionStore,
    ActaeClaudeHook,
    channel_for_session,
)
from claude_agent_sdk import ClaudeAgentOptions, query

store = ActaeClaudeSessionStore(actae_client)

# Durable, resumable transcripts (full SessionStore protocol):
async for message in query(
    prompt="Refactor this module",
    options=ClaudeAgentOptions(session_store=store),
):
    ...

# Continue the same conversation on restart:
async for message in query(
    prompt="Now run the tests",
    options=ClaudeAgentOptions(session_store=store, resume=True),
):
    ...

# Fork & resume (refine the conversation from a point, context intact):
new_key = await store.fork_session(
    project_key="my-project", session_id="run-1",
    new_session_id="run-1-fix", reason="refine the conclusion",
)
async for message in query(
    prompt="Actually, change the conclusion",
    options=ClaudeAgentOptions(
        session_store=store, resume=True, session_id=new_key["session_id"]),
):
    ...
```

- Implements `append` / `load` / `delete` / `list_sessions` /
  `list_subkeys` / `list_session_summaries` / `fork_session`. Every append
  writes a cursor-aligned versioned snapshot (channel `claude:<sha256(project
  \x00
  session)[:16]>`, subkeys on their own channel, project index on
  `claude:idx:<...>`) and streams a `claude.session.append` event.
  `list_session_summaries` stores the SDK-owned `fold_session_summary`
  sidecar verbatim (never interpreted).
- `ActaeClaudeHook(actae)` — `.hooks` property builds `PreToolUse` /
  `PostToolUse` / `PostToolUseFailure` / `UserPromptSubmit` / `Stop`
  matchers forwarding events as `claude.hook.<event>` on
  `claude-hooks:<sha256(session_id)[:16]>`. Hook failures are logged, never
  raised.
- Requires `pip install 'actae-client[claude]'` (`claude-agent-sdk>=0.2`).
  Module imports without the SDK; store construction raises `ImportError`.

### OpenAI Agents SDK — ActaeTracingProcessor + install_actae_tracing

```python
from actae_client.adapters.openai_agents import (
    install_actae_tracing,
    uninstall_actae_tracing,
)

proc = install_actae_tracing(actae_client)   # once, before Runner.run
result = await Runner.run(agent, "Hello")  # auto-recorded

snapshot = await actae_client.latest_state("openai_agents")
for span in snapshot["state"]["spans"]:
    print(span["type"], span["name"])

await proc.flush()                         # await pending writes at shutdown
```

- Implements the full `agents.tracing.TracingProcessor` protocol
  (`on_trace_start` / `on_trace_end` / `on_span_start` / `on_span_end` /
  `force_flush` / `shutdown`). Events: `openai.trace.start`,
  `openai.span.end`, and `openai.trace.end` via `transition()` (atomic
  event + cursor-aligned snapshot with the full span tree, versioned per
  run).
- Sync callbacks post writes to a dedicated background event loop
  (daemon thread), so both sync and async `Runner.run` work; `force_flush`
  / `shutdown` wait up to `flush_timeout` (default 5 s).
- `include_inputs_outputs=False` strips span input/output payloads.
- Callback exceptions are caught and logged — the processor never breaks a
  run. `uninstall_actae_tracing(proc)` is best-effort (private SDK
  internals; returns `False` if it can't remove).
- Requires `pip install 'actae-client[openai-agents]'`
  (`openai-agents>=0.7`); module imports without the SDK.

### Codex CLI — CodexOTLPReceiver (observability mirror)

Codex has native local fork/resume; Actae mirrors its OTEL log events into
per-conversation channels so the dashboard, state-diff and decision-trail work
on Codex runs.

```python
from actae_client.adapters.codex import CodexOTLPReceiver

receiver = CodexOTLPReceiver(actae_client, host="127.0.0.1", port=4319)
await receiver.start()   # serves POST /v1/logs
# ... run `codex` with [otel] exporter → http://127.0.0.1:4319/v1/logs ...
await receiver.stop()
```

- Enable Codex export in `~/.codex/config.toml` (`[otel]` with an
  `otlp-http` exporter pointed at the receiver's `/v1/logs`).
- Mirrors `codex.conversation_starts`, `codex.api_request`,
  `codex.sse_event` (token counts), `codex.user_prompt`,
  `codex.tool_decision`, `codex.tool_result` to channel `codex:<conversation.id>`.
- Persists an accumulating state snapshot (tokens + tool ledger) so
  `diff` / `trail` work across Codex runs.

---

## Common Patterns & Pitfalls

### Correct: context manager (auto lifecycle)
```python
async with ActaeClient(api_key, endpoint=url) as actae:
    await actae.subscribe("ch")
    ...
```

### Correct: manual lifecycle
```python
actae = ActaeClient(api_key, endpoint=url)
await actae.connect()
try:
    await actae.subscribe("ch")
finally:
    await actae.disconnect()
```

### WRONG: missing connect
```python
actae = ActaeClient(api_key, endpoint=url)
await actae.subscribe("ch")   #  ActaeConnectionError: Not connected
```

### WRONG: shared ActaeClient across AgentSession
Each agent session needs its own `ActaeClient` instance if running
concurrently (the client has a single WebSocket).

### WRONG: conflicting ActaeClient name
The SDK exports `ActaeConnectionError`, never `ConnectionError`, so user
code can use `except ConnectionError` for builtin socket errors safely.

### Sonic-rs number parsing
The server may serialize large JSON numbers as
`{"$sonic_rs::private::JsonNumber": "12345"}`. The SDK automatically
unwraps these in `Event.payload` and `state` data.

### Max replay limit: 1000 events per call
The server caps replay/query at 1000 events per call. Page with the
`cursor` argument for larger datasets (or use `AgentSession.resume`, which
pages automatically).

### Session cursors cap: 1000 stored
Beyond 1000 steps, fork resolution falls back to replay. This is a
metadata storage optimization — no data loss.

---

## HTTP Endpoints Summary

| Method | Path | Purpose |
|--------|------|---------|
| POST | `/api/v1/events/record` | Record event |
| GET | `/api/v1/events/replay/{channel_id}` | Replay events |
| POST | `/api/v1/events/query` | Query events |
| GET | `/api/v1/events/cursor/{channel_id}` | Get latest cursor |
| POST | `/api/v1/state/{channel_id}` | Save state snapshot |
| GET | `/api/v1/state/{channel_id}` | Get latest state |
| GET | `/api/v1/state/{channel_id}/versions` | List state versions |
| GET | `/api/v1/state/{channel_id}/version/{version}` | Get specific version |
| DELETE | `/api/v1/state/{channel_id}/version/{version}` | Delete version |
| GET | `/api/v1/channels` | List channels |
| POST | `/api/v1/channels/fork` | Fork channel |
| GET | `/api/v1/channels/{id}/metadata` | Get channel metadata |
| PUT | `/api/v1/channels/{id}/metadata` | Update metadata |
| GET | `/api/v1/channels/{id}/forks` | List forks |
| GET | `/api/v1/channels/{id}/fork-tree` | Get execution tree |
| POST | `/api/v1/groups` | Create consumer group |
| GET | `/api/v1/groups` | List consumer groups |
| DELETE | `/api/v1/groups/{group_id}` | Delete group (cascades offsets) |
| POST | `/api/v1/groups/{group_id}/join` | Join group (lease + durable offset) |
| POST | `/api/v1/groups/{group_id}/work` | Claim work batch |
| POST | `/api/v1/groups/{group_id}/ack` | Ack processed work (watermark) |
| POST | `/api/v1/groups/{group_id}/heartbeat` | Extend consumer lease |
| GET | `/api/v1/groups/{group_id}/offsets` | Per-consumer offsets |
| POST | `/api/v1/scheduler/wakeups` | Schedule a wake-up |
| GET | `/api/v1/scheduler/wakeups` | List wake-ups |
| GET | `/api/v1/scheduler/wakeups/{id}` | Get a wake-up |
| DELETE | `/api/v1/scheduler/wakeups/{id}` | Cancel a wake-up |
| POST | `/api/v1/executions/claim` | Claim idempotent tool execution |
| POST | `/api/v1/executions/{id}/complete` | Complete an execution |
| POST | `/api/v1/executions/{id}/fail` | Fail an execution |
| POST | `/api/v1/executions/{id}/heartbeat` | Extend execution lease |
| POST | `/api/v1/executions/{id}/cancel` | Cancel an execution |
| GET | `/api/v1/executions/{id}` | Get an execution |
| GET | `/api/v1/executions` | List executions for a channel |
| DELETE | `/api/v1/executions/{id}` | Delete an execution |
| GET | `/healthz` | Health check |
| GET | `/readyz` | Readiness probe |
| GET | `/metrics` | Prometheus metrics |
| GET | `/metrics.json` | JSON metrics |
| POST | `/api/v1/auth/signup` | Create account |
| POST | `/api/v1/auth/login` | Login |
| POST | `/api/v1/auth/logout` | Logout |
| GET | `/api/v1/auth/me` | Current user |
