# Actae Python SDK

> Distributed agent coordination and Group Fork are documented in [`../../docs/EXECUTION_GROUPS.md`](../../docs/EXECUTION_GROUPS.md). Use `client.execution_group(id)` for durable messaging, member leases, `wait_for`/`wait_any`/`wait_all`, acknowledgement, promotion, and counterfactual receipts.

Full-featured Python client for Actae — record, replay, query, subscribe, state
snapshots, fork/experiment forks, consumer groups, wake-ups, and auth.

```bash
pip install actae-client
```

Requires Python 3.9+ and `aiohttp`. Framework adapters (LangGraph, Claude,
OpenAI Agents, …) are optional extras — see [Framework Adapters](#framework-adapters).

## Start with self-hosted Actae (free)

Self-hosting is the default way to run Actae: one free, self-contained binary
with the API, dashboard, and PostgreSQL embedded. No account, card, or control
plane.

```bash
# Download for your platform: https://actae.dev/download
tar -xzf actae-<version>-<platform>.tar.gz && chmod +x ./actae
./actae
# The banner ends with your endpoint and a one-time API key:
#   Dashboard:   http://127.0.0.1:8002
#   API key:     sk-...   (shown once — store it now)
```

**Self-hosted still needs an API key** — but it is a *local* key the binary
generates and prints once on first boot. It is not an account or a portal
credential, and there is no signup. (Anonymous access is off by default; leave
it off.) Mint more with `actae keys create <name>`.

Point the SDK at that endpoint and key:

```python
import os
from actae_client import ActaeClient

async with ActaeClient(
    endpoint=os.environ["ACTAE_URL"],      # http://127.0.0.1:8002
    api_key=os.environ["ACTAE_API_KEY"],   # the sk-... from the banner
) as client:
    await client.record("my-channel", "tool.completed", payload={"ok": True})
```

Prefer not to operate it? **Actae Cloud** is the managed runtime — create an
API key in the portal and use your instance URL. Everything in this README
works identically against either deployment.

> **For LLM consumption**: a single-file consolidated reference is at
> [`SDK_REFERENCE.md`](./SDK_REFERENCE.md).

---

## Quick Start

The shortest production integration uses the high-level facade:

```python
from actae_client import Actae

actae = Actae.from_env()

@actae.effect("order:{order_id}:charge")
async def charge(order_id: str, amount: int):
    return await payments.charge(order_id, amount)

async with actae.run("ticket-42", framework="support-agent"):
    await charge("order-7", 2500)
```

`Actae` also exposes `langgraph`, `langchain`, `claude`, `crewai`, `openai`
and `codex` providers, synchronous scopes/decorators, and a credential-free
orchestrator carrier. See [SDK Integration](../../docs/SDK_INTEGRATION.md).

For a cooperative cancellation signal on long effects, set
`pass_cancel_event=True` and accept one final `asyncio.Event` argument. Sync
scopes expose `record_sync`, `effect_sync`, and `child_sync`; no coroutine
plumbing is required in synchronous frameworks.

```python
import asyncio, os
from actae_client import ActaeClient

async def main():
    async with ActaeClient(
        endpoint=os.environ["ACTAE_URL"],       # http://127.0.0.1:8002
        api_key=os.environ["ACTAE_API_KEY"],    # from the banner
    ) as client:
        event = await client.record(
            "my-channel", "tool.completed",
            payload={"result": "success"},
            actor="my-agent",
        )
        print(f"Recorded: cursor={event.cursor}")

        events = await client.replay("my-channel", cursor=0)
        for e in events:
            print(f"  [{e.cursor}] {e.event_type}")

asyncio.run(main())
```

`api_key` is required; `endpoint` alone is enough to derive the WebSocket URL
automatically (`http://` → `ws://…/ws`, `https://` → `wss://…/ws`). The
server runs everything (HTTP API, WebSocket, health, metrics) on one port:
**8002** (dev mode: **8002**, auto-bumping to the next free port when busy).

`replay(channel, cursor=...)` uses **exclusive** cursor semantics: events with
a per-channel cursor strictly greater than the given value are returned (pass
`N - 1` for events up to and including cursor `N`). The same exclusive
semantics apply to the `cursor` argument of `subscribe()` and `stream()`.
Cursors are **per-channel**: each channel numbers its events 1, 2, 3, …
independently (gapless), so `cursor` is always relative to a single channel.

### Connecting to your Actae instance

You need two things: the **endpoint** (where Actae runs) and an **API key**
(who you are). Where you get them depends on how you run Actae:

| Setup | Endpoint | API key |
|-------|----------|---------|
| **Self-hosted community build** (recommended) | `http://127.0.0.1:8002` from the startup banner | the `sk-...` the binary prints once (local; no account) |
| **Self-hosted, docker compose** | `https://your-server:8002` (TLS) | the `ACTAE_API_KEY` you provisioned (see `docs/PRODUCTION.md`) |
| **Source checkout (`cargo run`)** | `http://localhost:8002` | `sk-dev-0000000000000000000000` (dev-only fixed key) |
| **Actae Cloud** | your instance URL shown in the portal | `sk-...` created in the portal (Settings → API keys) |

Three kinds of credentials exist — here's when each is used:

1. **API key** (`sk-...`) — the primary credential. Pass it to `ActaeClient`
   as `api_key`; every HTTP call and the WebSocket auth use it. This is the
   only credential most users ever touch.
2. **JWT** (`client.auth.signup/login`) — user-account tokens for the
   dashboard-facing auth endpoints (`get_me`, `logout`). The SDK does not
   hold or reuse them automatically; pass them per call.
3. **mTLS client certificate** — optional transport-level auth on top of the
   API key, required only if your instance enforces it. See below.

### Real-time: subscribe and stream

```python
async with ActaeClient(endpoint="http://localhost:8002", api_key="sk-...", echo_self=True) as client:
    # Option A — async iteration (recommended). Blocks until an event
    # arrives; this demo publishes its own (echo_self) so it terminates —
    # omit `break` to keep streaming live events.
    async def show():
        async for event in client.stream("my-channel"):
            print(f"[{event.cursor}] {event.event_type}: {event.payload}")
            break
    task = asyncio.create_task(show())

    # Option B — callback style:
    client.on_message(lambda topic, event: print(topic, event.event_type))
    await client.subscribe("my-channel")            # wait=True by default

    await client.publish("my-channel", {"hello": "world"})
    await task
```

Both forms **block until events arrive** — a live stream never "finishes".

> **Note:** the server does not echo a publish back to the connection that
> published it. To see your own events live, either subscribe with one
> client and publish with another, or construct the client with
> `echo_self=True` — then your own publishes are delivered to this client's
> `on_message` callbacks and `stream()` iterators too (mirrors the Go SDK's
> `EchoSelf`).

### Sync code? No problem.

The most common calls also exist in a blocking form for non-async codebases:

```python
client = ActaeClient(endpoint="http://localhost:8002", api_key="sk-...")
event = client.record_sync("my-channel", "tool.completed", {...}, actor="me")
events = client.replay_sync("my-channel")
state = client.latest_state_sync("my-channel")
```

Each blocking call runs on its own per-thread background event loop, so you
can mix sync and async use of the same client freely.

| Sync variant | Async counterpart |
|--------------|-------------------|
| `record_sync(channel, type, payload, *, actor, ...)` | `record` |
| `replay_sync(channel, *, cursor?, limit?)` | `replay` |
| `query_sync(*, event_type?, ...)` | `query` |
| `get_cursor_sync(channel)` | `get_cursor` |
| `transition_sync(channel, type, payload, state, ...)` | `transition` |
| `fork_sync(source, new, at_cursor=0, ...)` | `fork` |
| `latest_state_sync(channel)` | `latest_state` |
| `save_state_sync(channel, cursor, state, ...)` | `save_state` |
| `disconnect_sync()` | `disconnect` — call at shutdown to release the WS + HTTP sessions |

WebSocket methods (`subscribe`, `stream`, `publish`, …), consumer groups,
executions, wake-ups, and `AgentSession` are async-only — the table shows
which async call each `_sync` method maps to.

---

## Which API should I use?

Actae is an event store with a lot of surface. This table tells you where to
go for each use case:

| Use case | Start here |
|----------|-----------|
| Log what an agent did | `client.events.record(channel, type, payload, actor=...)` |
| Store agent context so a run can be resumed | `client.state.save_state(channel, cursor, state)` |
| Record an event **and** its resulting state atomically | `client.events.transition(...)` |
| Replay / query what happened | `client.events.replay(...)`, `client.events.query(...)` |
| Watch events live | `client.ws.stream(channel)` or `client.ws.subscribe(...)` |
| High-level "session" wrapper (steps, forks, resume) | `AgentSession` — see below |
| Framework-agnostic state save/load/resume | `StateManager` adapter |
| A/B test by forking a channel into forks | `client.channels.fork(...)` |
| Prove which fork fixed a failure (state diff) | `client.channels.diff_states(left, right)` |
| Audit a fork (lineage + tool executions) | `client.channels.decision_trail(channel)` |
| Durable worker consuming a channel | `client.groups.*` |
| Schedule a one-off event in the future | `client.wakeups.schedule_wakeup(...)` |
| Coordinate idempotent tool execution across retries | `client.executions.claim_execution(...)` |

Every method lives on the client (`client.record(...)`) **and** on a
namespaced facade (`client.events.record(...)`). The facades just group
methods by concern — they are the same calls, so pick whichever reads
better in your code.

#### "How do I persist agent state?" — the four ways, and when

State persistence is the one place Actae offers overlapping primitives.
Pick by how much structure you want:

| You want to… | Use | Notes |
|---|---|---|
| Persist raw context manually | `save_state(channel, cursor, state)` | You fetch the cursor yourself (`latest_cursor()`) |
| Record an event + state **atomically** | `transition(...)` | Both commit in one server-side transaction |
| Steps + auto-snapshots + fork/resume | `AgentSession(state_fn=...)` | Saves state for you every N steps; `state_fn` may be sync or async |
| Framework-agnostic save/load/resume helpers | `StateManager(actae, channel=...)` | Thin wrapper that hides cursor handling |
| LangGraph checkpoints | `ActaeCheckpointSaver` | Framework-specific, see adapters below |

Rule of thumb: **transition** when the state belongs to a specific event,
**AgentSession** when you're instrumenting a multi-step agent run, and
**StateManager** (or raw `save_state`) when you want full control.

---

## Client

```python
ActaeClient(
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
)
```

| Param | Default | Notes |
|-------|---------|-------|
| `api_key` | required | API key for authentication |
| `endpoint` | `""` | HTTP API base URL (e.g. `http://localhost:8002`) |
| `ws_endpoint` | `""` | WebSocket URL — auto-derived from `endpoint` if omitted |
| `client_cert` / `client_key` / `ca_cert` | `None` | mTLS cert files |
| `timeout` | `30.0` | Request/connection timeout (seconds) |
| `auto_reconnect` | `True` | Auto-reconnect WebSocket on disconnect (resubscribes topics) |
| `echo_self` | `False` | Deliver this client's own publishes to its `on_message`/`stream()` (Go SDK `EchoSelf` parity) |

Use it as an async context manager (`async with`) or call
`await client.connect()` / `await client.disconnect()` manually.

### Connecting to an mTLS-protected instance

A production Actae instance requires every client to present a certificate
signed by the instance's client CA (this is **transport-level** auth — the
API key is checked on top of it). Two ways to supply the certs:

**Zero-config (recommended).** Drop the identity into the config dir once —
`~/.actae/` (or `ACTAE_CONFIG_DIR`) with `ca.crt`, `client.crt`,
`client.key` — and the SDK finds it automatically for any `https://` /
`wss://` endpoint:

```bash
# on the server: issue and install an identity
./scripts/issue-client-cert.sh deploy-agent --install
# (for a remote client, use --bundle and extract into ~/.actae)
```

```python
ActaeClient(endpoint="https://actae.example.com", api_key="sk-...")  # certs auto-loaded
```

**Explicit.** Pass the paths yourself — overrides the config dir:

```python
ActaeClient(
    endpoint="https://actae.example.com",
    api_key="sk-...",
    ca_cert="certs/ca.crt",                                   # trust the server
    client_cert="certs/client/deploy-agent.crt",              # identify yourself
    client_key="certs/client/deploy-agent.key",
)
```

`ws_endpoint` derives automatically (`https://` → `wss://…/ws`), and the
same certs are used for both HTTP and WebSocket. If the connection fails
and no certs are configured, the error message tells you exactly how to
fix it.

---

## Namespaced API surface

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

Plus blocking variants for sync code: `record_sync`, `replay_sync`,
`query_sync`, `get_cursor_sync`, `transition_sync`, `fork_sync`,
`latest_state_sync`, `save_state_sync`.

---

## Concepts (in one paragraph)

- **Channel** — a named event log (e.g. `"my-agent"` or `"exp-v1"`). All
  writes and replays target a channel. Channels can be **forked** into
  experiment forks.
- **Event** — one immutable entry on a channel: `event_type` + `payload` +
  `actor`, stamped with a gapless per-channel `cursor` (each channel numbers
  its events 1, 2, 3, … independently).
- **State snapshot** — a versioned, cursor-aligned JSON blob attached to a
  channel (`save_state` → `latest_state`). It is how you persist "where my
  agent got to" so a run can resume after a crash.
- **Topic** — what WebSocket methods call a channel. `subscribe("ch")` and
  `replay("ch")` talk about the same thing.

---

## Methods

### Events (HTTP)

> **Cursor semantics** (applies to every `cursor`-taking event method):
> cursors are **per-channel** (each channel numbers its events 1, 2, 3, …
> independently and gapless), and boundaries are **exclusive** — pass
> `N - 1` to include cursor `N`. `Event.cursor` is the per-channel cursor
> (used by consumer groups via `ack_work` and state alignment);
> `Event.channel_cursor` is a deprecated alias carrying the same value.

| Method | Returns | Server |
|--------|---------|--------|
| `record(channel_id, event_type, payload, *, actor, agent_id?, user_id?, metadata?, operation_id?)` | `Event` | `POST /api/v1/events/record` |
| `transition(channel_id, event_type, payload, state, *, actor="system", agent_id?, user_id?, metadata?, operation_id?, expected_version?, expected_cursor?)` | `TransitionResult` (event + state_version) | `POST /api/v1/events/transition` |
| `replay(channel_id, *, cursor?, limit?, event_type?)` | `List[Event]` | `GET /api/v1/events/replay/{channel_id}` |
| `query(*, channel_ids?, event_type?, actor?, cursor_start?, cursor_end?, from_time?, to_time?, limit?, offset?)` | `List[Event]` | `POST /api/v1/events/query` |
| `get_cursor(channel_id)` | `Optional[int]` | `GET /api/v1/events/cursor/{channel_id}` |

### State (HTTP — versioned snapshots)

The state family returns different shapes — here they are at a glance:

| Method | Returns | Server |
|--------|---------|--------|
| `save_state(channel_id, cursor, state, *, expected_version?, expected_cursor?)` | `int` (new version) | `POST /api/v1/state/{channel_id}` |
| `latest_state(channel_id)` | `Optional[{"cursor": int, "state": ...}]` | `GET /api/v1/state/{channel_id}` |
| `list_states(channel_id, *, limit?, offset?)` | `List[{"version": int, "cursor": int, "timestamp": str}]` | `GET /api/v1/state/{channel_id}/versions` |
| `get_state(channel_id, version)` | `Optional[{"cursor": int, "version": int, "state": ...}]` | `GET /api/v1/state/{channel_id}/version/{version}` |
| `delete_state(channel_id, version)` | `None` | `DELETE /api/v1/state/{channel_id}/version/{version}` |

`save_state` takes the **global** cursor of the event the snapshot aligns
with — fetch it from the event you just recorded (`event.cursor`) or
`get_cursor(channel_id)`. If this cursor bookkeeping gets in the way, use
`StateManager` or `AgentSession`, which handle it for you.

### Channels & Fork (HTTP)

| Method | Returns | Server |
|--------|---------|--------|
| `list_channels()` | `List[str]` | `GET /api/v1/channels` — ordered by most recent activity |
| `fork(source, new, at_cursor=0, *, display_name?, reason?, experiment_metadata?, operation_id?, manifest?, tool_policies?, expected_version?, expected_cursor?)` | `ForkReceipt` (immutable) | `POST /api/v1/channels/fork` |
| `get_fork_receipt(channel_id)` | `ForkReceipt` | `GET /api/v1/channels/{channel_id}/receipt` |
| `resolve_step(channel_id, step_number)` | `(owner, cursor, event_id)?` | `GET /api/v1/channels/{channel_id}/steps/{step_number}` |
| `latest_step_number(channel_id)` | `Optional[int]` (highest step, 0 = none) | `GET /api/v1/channels/{channel_id}/steps` — O(1) crash-recovery probe |
| `get_channel_metadata(channel_id)` | `Optional[ChannelMetadata]` | `GET /api/v1/channels/{channel_id}/metadata` |
| `list_forks(channel_id)` | `List[ChannelMetadata]` | `GET /api/v1/channels/{channel_id}/forks` |
| `get_fork_tree(root_channel_id)` | `ForkInfo` | `GET /api/v1/channels/{root_channel_id}/fork-tree` |
| `update_metadata(channel_id, *, display_name?, reason?, experiment_metadata?)` | `ChannelMetadata` | `PUT /api/v1/channels/{channel_id}/metadata` |
| `diff_states(left, right)` | `dict` (per-side divergence + honest truncation) | `GET /api/v1/channels/diff` |
| `compare_channels(left, right)` | `dict` (state + manifest + tools + output + metrics) | `GET /api/v1/channels/compare` |
| `set_outcome(channel_id, outcome, *, score?)` | `dict` | `PATCH /api/v1/channels/{channel_id}/outcome` |
| `promote_channel(channel_id)` | `dict` | `POST /api/v1/channels/{channel_id}/promote` |
| `delete_channel(channel_id, recursive=False)` | `dict` | `DELETE /api/v1/channels/{channel_id}` |
| `create_experiment / list_experiments / get_experiment / add_experiment_member / rank_experiment` | `dict` | `POST/GET /api/v1/experiments...` |

**Boundary policy** (`AgentSession.fork` / `resume(fork_at_step=)`):
`boundary_mode="exact"` (default) raises `NoRestorableCheckpointError` when no
snapshot exists at the requested step — it never silently inherits later
state. `"approximate"` falls back to the latest state and reports the drift
via `session.resolved_boundary_cursor`; `"lineage_only"` creates the fork
with no state copy. Fork errors: `SnapshotBoundaryError`,
`IdempotencyConflictError`, `ChannelConflictError`.

**Side-effect-aware forking:** `AgentSession.fork` accepts
`tool_policies={"tool": "replay"|"block"|"live"|"auto", "*": default}` (default
`auto`: an exact inherited result replays, new/changed work runs live) and an
opaque `intervention={...}` descriptor stored on the fork metadata. The
application reads `session.intervention` and applies the change; Actae never
interprets it. A refused call raises `ForkToolBlockedError` and persists a
`tool.blocked` event. `resume(fork_at_step=..., intervention=..., tool_policies=...)`
forwards both; `fork_receipt.tool_policies` echoes the policy.

**Hard-kill crash recovery:** `resume()` reconstructs true step progress from
the server step index (`latest_step_number`, O(1)) and the durable event log,
so a process killed with `SIGKILL`/`os._exit` resumes at the real step instead
of re-running the prefix from the last metadata write.

### Consumer groups (HTTP — durable workers)

At-least-once channel consumption with leases: consumers join a group,
`claim_work` batches, process, `ack_work` to commit the offset. Crashed
workers' unacked work is redelivered after lease expiry.

| Method | Returns | Server |
|--------|---------|--------|
| `create_group(group_id, channel_id, *, metadata?)` | `GroupInfo` | `POST /api/v1/groups` |
| `list_groups(*, channel_id?)` | `List[GroupInfo]` | `GET /api/v1/groups` |
| `delete_group(group_id)` | `None` | `DELETE /api/v1/groups/{group_id}` |
| `join_group(group_id, consumer_id, *, lease_seconds=60)` | `GroupOffset` | `POST /api/v1/groups/{group_id}/join` |
| `claim_work(group_id, consumer_id, *, limit=100)` | `ClaimedWork` (events + lease_until) | `POST /api/v1/groups/{group_id}/work` |
| `ack_work(group_id, consumer_id, cursor)` | `None` | `POST /api/v1/groups/{group_id}/ack` |
| `heartbeat(group_id, consumer_id, *, lease_seconds=60)` | `None` | `POST /api/v1/groups/{group_id}/heartbeat` |
| `group_offsets(group_id)` | `List[GroupOffset]` | `GET /api/v1/groups/{group_id}/offsets` |

A minimal durable worker (at-least-once: claim → process → ack; crashed
workers' batches are redelivered after lease expiry):

```python
async def worker(client, group_id, consumer_id):
    await client.groups.join_group(group_id, consumer_id, lease_seconds=60)
    while True:
        work = await client.groups.claim_work(group_id, consumer_id, limit=50)
        if not work.events:
            await asyncio.sleep(1); continue
        for event in work.events:                 # process in cursor order
            await handle(event)
            await client.groups.ack_work(group_id, consumer_id, event.channel_cursor)
        await client.groups.heartbeat(group_id, consumer_id)  # keep the lease
```

Two rules to keep straight: **ack is a watermark** — always ack the highest
contiguously processed `event.cursor` (never a higher one, or events
are skipped permanently); and **heartbeat while you work** if a batch takes
longer than the lease.

### Idempotent tool executions (HTTP)

Coordinated tool retries: `claim_execution` returns `claimed` (you run the
tool) or `replayed` (a previous run's result is returned instead). Finish
with `complete_execution` / `fail_execution`, keep the lease alive with
`heartbeat_execution`, or `cancel_execution`. Actae coordinates the retry and
replays recorded results; exactly-once external effects additionally require an
idempotent or queryable downstream operation (see the failure boundary below).

| Method | Returns | Server |
|--------|---------|--------|
| `claim_execution(channel_id, key_name, tool_name, params=None, *, dedup_fields?, lease_seconds?, emit_replay_event?, actor?)` | `ExecutionClaim` | `POST /api/v1/executions/claim` |
| `complete_execution(execution_id, claim_token=None, result=None)` | `ExecutionInfo` | `POST /api/v1/executions/{id}/complete` |
| `fail_execution(execution_id, message, *, claim_token?, error_type?, stack?)` | `ExecutionInfo` | `POST /api/v1/executions/{id}/fail` |
| `heartbeat_execution(execution_id, claim_token=None, lease_seconds=None)` | `ExecutionInfo` | `POST /api/v1/executions/{id}/heartbeat` |
| `cancel_execution(execution_id, claim_token=None)` | `ExecutionInfo` | `POST /api/v1/executions/{id}/cancel` |
| `get_execution(execution_id)` | `ExecutionInfo` | `GET /api/v1/executions/{id}` |
| `list_executions(channel_id, limit=50)` | `List[ExecutionInfo]` | `GET /api/v1/executions` |
| `delete_execution(execution_id)` | `bool` | `DELETE /api/v1/executions/{id}` |

Minimal coordinated-retry pattern — the same key retried by any caller
returns the recorded result instead of running the tool again:

```python
claim = await client.executions.claim_execution(
    "orders", "charge-42", "charge_customer", params={"amount": 9.99},
)
if claim.status == "replayed":
    return claim.result                       # already ran — return it
if claim.status == "in_progress":
    raise RetryLater()                        # someone else owns it
try:
    result = await run_tool(claim.execution)
    await client.executions.complete_execution(
        claim.execution.id, claim_token=claim.claim_token, result=result)
except Exception as e:
    await client.executions.fail_execution(
        claim.execution.id, str(e), claim_token=claim.claim_token)
```

**The failure boundary.** Actae coordinates the retry: it deduplicates
completed executions, fences stale owners with claim tokens, and replays
recorded results. It does **not** make the external operation itself atomic:

```text
claim
  ↓
run external operation
  ↓
persist completion
```

If the external operation succeeds but the worker dies before Actae records
completion, the lease expires and a later worker may run the operation again.
Exactly-once external effects additionally require the downstream operation to
be idempotent or queryable — for example by passing the same downstream
idempotency key, or by checking the upstream state before acting. Actae
guarantees coordination and recorded-result replay; it cannot close the gap
between an external call and the commit that records it.

### Wake-ups (HTTP — persisted scheduling)

`run_at` accepts either an RFC 3339 string (`"2026-08-05T12:00:00Z"`) or a
timezone-aware `datetime` (naive datetimes raise `ValueError`).

| Method | Returns | Server |
|--------|---------|--------|
| `schedule_wakeup(channel_id, run_at, *, payload?)` | `Wakeup` | `POST /api/v1/scheduler/wakeups` |
| `list_wakeups(*, channel_id?, status?)` | `List[Wakeup]` | `GET /api/v1/scheduler/wakeups` |
| `get_wakeup(wakeup_id)` | `Optional[Wakeup]` | `GET /api/v1/scheduler/wakeups/{id}` |
| `cancel_wakeup(wakeup_id)` | `bool` | `DELETE /api/v1/scheduler/wakeups/{id}` |

### Durable human approval (HTTP — human-in-the-loop)

Records `approval.requested` / `approval.decided` / `approval.expired` events;
the agent process need not stay alive, and Actae never resumes anything itself.
See `docs/HUMAN_APPROVAL.md`.

| Method | Returns | Server |
|--------|---------|--------|
| `request_approval(channel_id, *, summary, details?, timeout_seconds?, requester?, request_id?)` | `str` (request id) | `POST /events/record` (+ a wake-up when `timeout_seconds` is set) |
| `decide_approval(channel_id, request_id, *, decision, actor, reason?, operation_id?)` | `Event` | `POST /events/record` |
| `wait_for_approval(channel_id, request_id, *, timeout_seconds?, poll_interval?, start_cursor?)` | `Optional[Event]` | polls `GET /events/replay` |
| `AgentSession.require_approval(summary, *, details?, timeout_seconds?, ...)` | `dict` | request + wait |

### Health & readiness (HTTP)

| Method | Returns | Server |
|--------|---------|--------|
| `health_check()` | `HealthStatus` | `GET /healthz` |
| `readiness_check()` | `ReadinessResult` | `GET /readyz` |
| `get_metrics_text()` | `str` (Prometheus format) | `GET /metrics` |
| `get_metrics_json()` | `MetricsSnapshot` | `GET /metrics.json` |

### Auth (HTTP — user management)

| Method | Returns | Server |
|--------|---------|--------|
| `signup(email, password, name?)` | `AuthResult` (user + JWT) | `POST /api/v1/auth/signup` |
| `login(email, password)` | `AuthResult` (user + JWT) | `POST /api/v1/auth/login` |
| `logout(jwt_token)` | `None` | `POST /api/v1/auth/logout` |
| `get_me(jwt_token)` | `UserInfo` | `GET /api/v1/auth/me` |

### WebSocket

| Method | Returns | Notes |
|--------|---------|-------|
| `connect()` | `None` | Open WebSocket connection (auto on `async with`) |
| `disconnect()` | `None` | Close WebSocket connection |
| `subscribe(topic, *, cursor?, wait=True)` | `None` | Subscribe; `cursor` replays events with a cursor strictly greater than it (exclusive, like `replay`); `wait=True` (default) confirms before returning |
| `unsubscribe(topic)` | `None` | Unsubscribe from channel |
| `publish(topic, payload, *, operation_id?)` | `Event` | Broadcast event over WS, returns persisted event via Ack; `operation_id` makes retries idempotent |
| `stream(topic, *, cursor?)` | `async iterator[Event]` | `async for event in client.stream("ch"): ...` |
| `on_message(callback)` | `None` | `callback(topic, event)` — multiple callbacks allowed |
| `on_error(callback)` | `None` | `callback(message)` |
| `on_subscribed(callback)` | `None` | `callback(topic, cursor)` |
| `on_disconnected(callback)` | `None` | `callback()` — fires before auto-reconnect |
| `on_reconnect(callback)` | `None` | `callback()` — fires after resubscribe |

On reconnect the client resumes each topic at the last event it received, so
**no events are re-delivered** after a drop. If you ever need belt-and-braces
single-processing on top, deduplicate by `event.id` (stable UUID per event).

---

## Agents & Sessions

`AgentSession` wraps a channel with a step → cursor mapping so you can fork
at a *step number* instead of a raw cursor, and auto-saves state snapshots
every `snapshot_interval` steps via `state_fn` (`state_fn` may be a sync or
an async function):

```python
from actae_client import ActaeClient
from actae_client.session import AgentSession

actae = ActaeClient(endpoint="http://localhost:8002", api_key="sk-...")

async def main():
    async with AgentSession(
        actae,
        name="exp-v1",                      # used as the channel ID
        display_name="Temperature Search",  # human-readable name in dashboard
        state_fn=lambda: agent.state,
    ) as session:
        result = await session.step("inference", input="prompt", output="response")

asyncio.run(main())
```

Lifecycle is `created → started → stepping → completed` (or `crashed`).
Three entry points:

| You want to… | Use |
|---|---|
| Start recording a fresh run | `AgentSession(actae, name, ...)` + `async with` |
| Resume a crashed run in place | `await AgentSession.resume(actae, "exp-v1")` |
| Fork an existing run at step N into a new experiment | `await AgentSession.resume(actae, "exp-v1", fork_at_step=5, name="exp-v2")` |

See `docs/AGENT_SESSION.md` for full lifecycle and fork semantics.

---

## Framework Adapters

Every adapter shares `ActaeRunContext`, `CheckpointEnvelope`, and
`ActaeToolExecutor`. The run context exposes replay, checkpointing, forks,
experiments, execution groups, wake-ups, and idempotent tool effects without
taking execution away from the framework. Native resume fidelity is published
separately through `get_adapter_capabilities()`; see
[`docs/FRAMEWORK_ADAPTER_CONTRACT.md`](../../docs/FRAMEWORK_ADAPTER_CONTRACT.md).

| Module | Class | Purpose |
|--------|-------|---------|
| `actae_client.adapters.base` | `StateManager` | Generic state save/load/resume |
| `actae_client.adapters.langgraph` | `ActaeCheckpointSaver` | LangGraph 1.x checkpointer (full `BaseCheckpointSaver` protocol) |
| `actae_client.adapters.crewai` | `ActaeCrewStateHook` / `CrewAIResumer` | CrewAI task state |
| `actae_client.adapters.langchain` | `ActaeContextSaver` / `ChainResumer` | LangChain context |
| `actae_client.adapters.claude` | `ActaeClaudeSessionStore` / `ActaeClaudeHook` | Claude Agent SDK durable transcripts + live hook events |
| `actae_client.adapters.openai_agents` | `ActaeTracingProcessor` / `install_actae_tracing` | OpenAI Agents SDK trace/spans → events + run snapshots |

Framework dependencies remain optional: installing `actae-client` alone never requires
LangGraph, Claude, etc. Install what you need:

```bash
pip install 'actae-client[langgraph]'        # LangGraph checkpointer
pip install 'actae-client[langchain]'        # LangChain context callbacks
pip install 'actae-client[crewai]'           # CrewAI state hooks
pip install 'actae-client[claude]'           # Claude Agent SDK session store + hooks
pip install 'actae-client[openai-agents]'    # tracing + native RunState run/resume/fork
```

### LangGraph checkpointer

```python
from actae_client.adapters.langgraph import ActaeCheckpointSaver

saver = ActaeCheckpointSaver(actae_client)
graph = builder.compile(checkpointer=saver)
result = await graph.ainvoke(input, {"configurable": {"thread_id": "1"}})

# new process, same Actae — resume from the last checkpoint:
state = graph.get_state({"configurable": {"thread_id": "1"}})
result = graph.invoke(None, {"configurable": {"thread_id": "1"}})
```

Sync `invoke()` runs in a fresh event loop; calling sync protocol methods
inside a running loop raises `ActaeLangGraphSyncError`. See
`docs/ADAPTERS.md` for the storage format, channel resolution, and race
semantics.

### Generic StateManager

```python
from actae_client.adapters.base import StateManager

mgr = StateManager(actae_client, channel="my-agent")
state = await mgr.resume()          # dict or {} when nothing saved yet
...
await mgr.save(state)               # new version, returns version int
```

---

## Dataclasses

### Event

```python
@dataclass
class Event:
    id: str
    channel_id: str
    event_type: str
    payload: Any
    actor: str
    cursor: int                    # gapless per-channel monotonic cursor
    channel_cursor: Optional[int]  # deprecated alias of cursor (same value)
    timestamp: str
    agent_id: Optional[str] = None
    user_id: Optional[str] = None
    metadata: Optional[Dict] = None
    depends_on: Optional[str] = None  # ID of the causally-depended-on parent event
```

### HealthStatus

```python
@dataclass
class HealthStatus:
    status: str           # "ok" | "degraded" | "unhealthy"
    timestamp: str
    instance_id: str
    components: Dict[str, HealthComponent]  # database, connections, system
    check_duration_ms: int
```

### ReadinessResult

```python
@dataclass
class ReadinessResult:
    status: str                # "ready" | "not_ready"
    timestamp: str
    instance_id: str
    check_duration_ms: int
    database_ready: bool
    capacity_available: bool
    uptime_seconds: int
    connection_utilization: float
    active_connections: int
    max_connections: int
```

### MetricsSnapshot

```python
@dataclass
class MetricsSnapshot:
    status: str
    timestamp: str
    uptime_seconds: int
    websocket_connections: int
    topic_count: int
    messages_sent: int
    messages_received: int
    total_messages: int
    back_pressure: Dict[str, Any]
```

### AuthResult / UserInfo / ChannelMetadata / ForkInfo / TransitionResult

```python
@dataclass
class AuthResult:
    user: UserInfo
    token: str              # JWT for subsequent auth calls

@dataclass
class UserInfo:
    id: str
    email: str
    email_verified: bool
    name: Optional[str]
    image: Optional[str]
    created_at: Optional[str]

@dataclass
class ChannelMetadata:
    channel_id: str
    parent_channel_id: Optional[str]
    origin_run_id: str
    forked_at_cursor: int
    forked_at: str
    display_name: Optional[str]
    reason: Optional[str]
    experiment_metadata: Optional[Dict[str, Any]]

@dataclass
class ForkInfo:
    channel_id: str
    display_name: Optional[str]
    reason: Optional[str]
    forked_at_cursor: int
    event_count: int
    latest_cursor: Optional[int]
    children: List[ForkInfo]   # recursive tree

@dataclass
class TransitionResult:
    event: Event
    state_version: int
```

Group, wake-up, and execution dataclasses: `GroupInfo`, `GroupOffset`,
`ClaimedWork`, `Wakeup`, `ExecutionInfo`, `ExecutionClaim` — see
`SDK_REFERENCE.md` for their fields.

---

## Errors

All exceptions derive from `ActaeError` (which derives from `Exception`).
Notably, the WebSocket error is named `ActaeConnectionError` so it never
shadows Python's built-in `ConnectionError`.

| Exception | Raised when |
|-----------|-------------|
| `APIError(status_code, message)` | HTTP 4xx/5xx (all except the specific ones below) |
| `RateLimitError(retry_after_seconds, message)` | 429 rate limited |
| `AuthError(message)` | HTTP 401 (bad/expired API key, wrong password on `login`) or WebSocket auth failure |
| `ActaeConnectionError(message)` | WebSocket failure / not connected, **or HTTP transport failure** (server down, timeout) |
| `SnapshotBoundaryError(message)` | Fork `at_cursor` has no saved state boundary |
| `VersionConflictError(message)` | Optimistic-concurrency guard mismatch |
| `ConsumerError(message)` | Consumer-group error (no lease, unknown group) |
| `IdempotencyKeyMismatchError(message)` | Execution key reused with different params |
| `ExecutionNotOwnedError(message)` | Stale claim token on an execution |
| `ExecutionNotFoundError(message)` | Execution id does not exist |
| `SessionError(message)` | AgentSession lifecycle violation |
| `SessionCompletedError(message)` | Step on a completed session |
| `SessionLockedError(message)` | A fenced `AgentSession(fence_owner=...)` channel is owned by another live owner (or the fence was reclaimed) |
| `ActaeLangGraphError(message)` | LangGraph checkpointer config/corruption |
| `ActaeLangGraphSyncError(message)` | Sync checkpointer call inside a running loop |

---

## Dependencies

Only `aiohttp` is required. Framework adapters are optional and imported
lazily.
