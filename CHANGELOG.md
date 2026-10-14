# Changelog

## 1.3.0 (unreleased) — fork provenance & boundary policy

### Breaking changes

- **`ActaeClient.fork()` returns `ForkReceipt`** (immutable) instead of
  `ChannelMetadata`. Use `get_channel_metadata()` for the metadata view.
  `fork_sync()` follows.
- **`AgentSession.fork` / `resume(fork_at_step=)` default to
  `boundary_mode="exact"`** and raise `NoRestorableCheckpointError` when no
  snapshot exists at the requested step — they no longer silently fall
  forward to the latest state. Opt into `"approximate"` (fall back + report
  the drift via `resolved_boundary_cursor`) or `"lineage_only"` (no state
  copy) explicitly.
- **`ChannelMetadata.forked_at_cursor` is now the RESOLVED boundary** (the
  snapshot cursor actually inherited), not the requested one. The requested
  value is available as `requested_at_cursor`.

### New features

- **Immutable fork receipts**: `fork()` returns and
  `get_fork_receipt(channel_id)` fetches `requested_cursor`,
  `resolved_cursor`, `resolved_event_id`, `source_state_version`,
  `source_state_sha256`, `restorable`, `manifest`, `reproducibility`.
- **Strict idempotency**: `IdempotencyConflictError` (409) on request drift,
  `ChannelConflictError` (409) on child-id reuse under a different
  definition.
- **Server-owned step index**: `AgentSession.step` records step numbers;
  `resolve_step()` / server-side `fork_at_step` resolution with a legacy
  fallback.
- **Fork manifests + reproducibility grading** (`state_exact` |
  `context_exact` | `execution_replayable` | `best_effort`).
- **Optimistic fork guards**: `expected_version` / `expected_cursor`.
- **Experiment platform**: `create_experiment`, `list_experiments`,
  `get_experiment`, `add_experiment_member`, `rank_experiment`,
  `set_outcome`, `promote_channel`, `delete_channel`, `compare_channels`.
  `AgentSession` helpers: `set_outcome`, `promote`, `create_experiment`,
  `add_to_experiment`; a crashed session auto-records `outcome="crashed"`.
- **`diff_states`** now returns per-side divergence
  (`left_diverged_at_cursor` / `right_diverged_at_cursor`) and honest
  truncation (`truncated`, `entry_count_total`, `max_entries`).

## 1.2.0 (unreleased) — onboarding overhaul

### Breaking changes (see MIGRATION below)

- **Removed the label-based snapshot API**: `ActaeClient.save_snapshot()` and
  `ActaeClient.list_snapshots()` are gone, along with the server
  `/api/v1/snapshots` endpoints. Use the versioned `save_state()` /
  `latest_state()` / `list_states()` family instead.
- **Renamed `ConnectionError` → `ActaeConnectionError`**. The old name is no
  longer exported, so `except ConnectionError` in user code always refers to
  Python's builtin. Catch `ActaeConnectionError` for Actae WebSocket errors.
- **`ReadinessResult` simplified** — dropped the external-backplane
  readiness flag (the server is standalone single-node).
- **`subscribe(..., wait=True)` is now the default** — you no longer miss
  events while the subscription is being confirmed. Pass `wait=False` for
  fire-and-forget.

### New features

- **`CodexOTLPReceiver`** (`actae_client.adapters.codex`) — an OTLP/HTTP log
  receiver that mirrors Codex CLI's `codex.*` telemetry events into
  per-conversation Actae channels, persisting an accumulating token + tool
  snapshot so the dashboard, structural state-diff and decision-trail work on
  Codex runs. Codex's native local fork/resume is untouched (this is a
  read-only observability mirror).
- **`ActaeCheckpointSaver.fork_thread` / `fork_thread_sync`** — fork a
  LangGraph thread's checkpoint (full graph state incl. the LLM message
  context) into a new thread and resume with the inherited context, re-running
  only the nodes after the fork point. Honors an explicit `checkpoint_id` for
  intermediate-step forks.
- **`ActaeClaudeSessionStore.fork_session`** — fork a Claude session's full
  transcript into a new session (registered in the project index), so a
  resumed session has the complete conversation context.
- **`StateManager.fork`** — the framework-agnostic primitive: copy an opaque
  state dict to a new channel, returning a `StateManager` for the fork.
- **`diff_states(left, right)` / `diff_states_sync`** — structural diff of
  two channels' latest saved states (`GET /api/v1/channels/diff`). Walks
  both JSON states and reports exactly what each fork knew that the other
  did not (added/removed/changed), with the shared ancestor and divergence
  cursor. The counterfactual-debugging primitive (see
  `docs/COUNTERFACTUAL.md`).
- **`decision_trail(channel)` / `decision_trail_sync`** — lineage-as-audit
  (`GET /api/v1/channels/{id}/trail`): the ancestry chain back to the
  `origin_run_id` root, the fork boundary state, and the channel's
  idempotent tool-execution ledger rows.
- **`record(..., operation_id=None)` — idempotent records**: pass a stable
  UUID and the server returns the original persisted event when the same
  `(channel_id, operation_id)` is retried, instead of inserting a duplicate.
  `AgentSession.step` now sends a per-step `operation_id` automatically, so
  a lost response during the retry loop can never double-record a step.
- **`record()` returns a self-consistent `Event`** — `metadata`,
  `agent_id` and `user_id` are filled from the call arguments (the record
  response omits them), matching the Go SDK.
- **`AgentSession.resume(fork_at_step=...)` on event-only channels** now
  falls back to the latest state when no saved state boundary exists at the
  step's cursor (Go SDK parity), pages the replay fallback past the 1000
  events-per-call server cap, and excludes the server's `fork.started`
  lineage marker from position-based step resolution.
- **Namespaced facades**: `client.events`, `client.state`, `client.channels`,
  `client.executions`, `client.groups`, `client.wakeups`, `client.health`,
  `client.auth`, `client.ws` — every method still exists on the client; the
  facades just group them by concern.
- **`client.stream(topic, cursor=None)`** — `async for event in
  client.stream("ch"): ...` async iterator over live events (ends on
  disconnect).
- **`echo_self=True` client option** — the server never echoes a publish
  back to the publishing connection; with `echo_self` your own publishes
  are delivered to this client's `on_message` callbacks and `stream()`
  iterators (Go SDK `EchoSelf` parity).
- **Sync facade**: `record_sync`, `replay_sync`, `query_sync`,
  `get_cursor_sync`, `latest_state_sync`, `save_state_sync` — blocking
  variants for non-async codebases.
- **`transition_sync` / `fork_sync`** — blocking variants of `transition`
  and `fork`, completing the sync facade for the core write paths.
- **`AgentSession.state_fn` accepts async callables** — an `async def`
  state function is awaited before saving (previously passed an async
  function silently never saved state).
- **`schedule_wakeup(run_at=...)` accepts a timezone-aware `datetime`**
  (serialized as UTC RFC 3339); naive datetimes raise `ValueError`.
- **HTTP 401 now raises `AuthError`** (was `APIError`) — consistent with
  WebSocket auth failures, so `except AuthError` catches wrong API keys and
  failed `login()`.
- **Reconnect no longer re-delivers duplicates**: the client tracks the
  last-seen cursor per topic on every broadcast and resumes at that
  watermark, instead of resubscribing at the stale subscription-time cursor.
- **Catch-up replay duplicates dropped**: the server registers a connection
  for live delivery before running a subscribe-with-cursor replay, so an
  event persisted in that window is delivered both in the replay and live.
  The client now drops broadcasts at or below its delivered watermark per
  subscription window (`subscribe()` resets the window, so explicit
  re-subscribes still re-deliver).
- **`disconnect()` no longer crashes when sync-facade sessions exist** —
  sessions created on the background sync loops are now closed on their own
  loop (previously awaiting their close from the main loop raised a
  cross-loop `RuntimeError`, breaking mixed sync + async usage).
- **`disconnect_sync()` added** — sync-only users can now release the
  WebSocket and HTTP sessions at shutdown instead of leaking aiohttp
  connectors.
- **Fixed 30-second reconnect churn on quiet topics**: the WS heartbeat
  (30s) raced the per-receive timeout (30s), so a topic with no events for
  30s dropped the connection right before the next pong arrived. The
  heartbeat is now `timeout / 2` (clamped to [5s, 30s]), keeping pongs
  inside the receive window.
- **`ActaeLangGraphError` / `ActaeLangGraphSyncError` are now importable
  from `actae_client`** (previously only from the langgraph adapter, which
  was undocumented).
- **Concurrent `subscribe(topic, wait=True)` calls on the same topic now
  both complete** — e.g. `stream()` plus an explicit `subscribe()` (the
  second previously overwrote the first's waiter, which then timed out).
- **HTTP transport failures raise `ActaeConnectionError`** — connect
  refused, DNS/TLS failures, request timeouts and premature closes on HTTP
  calls are wrapped instead of leaking raw aiohttp exceptions, so
  `except ActaeError` catches every SDK failure.
- **`subscribe(wait=True)` ack timeout now follows the client's `timeout`**
  (was a hardcoded 10s).
- **`Event.from_query` now parses `depends_on`** — fork-lineage events
  fetched via `query()` keep their parent linkage (was silently dropped).
- **CI runs the Claude + OpenAI Agents adapter suites** (extras installed
  in the python-sdk job; they were previously skipped everywhere).
- **Removed the dead `Event.delivery_id` field** — the server never sent it
  (deduplicate by `event.id` instead).
- **`test_step_retry_on_transient_error` fixed** — its operation-id tracking
  no longer counts the `session.completed` event, which intentionally has no
  operation id.
- **Multiple `on_message` callbacks** (invoked in registration order).
- **Serialized `publish()`** — concurrent publishes each receive their own
  Ack.

### Fixed

- `Event.cursor` is now the **per-channel** gapless cursor (each channel
  numbers its events 1, 2, 3, … independently); `Event.channel_cursor` is a
  deprecated alias carrying the same value, kept for older clients. The
  server also accepts `channel_cursor` / `channel_cursor_start` /
  `channel_cursor_end` as aliases in replay/query, so existing SDK calls are
  unchanged.
- `disconnect()` no longer leaks sync-facade sessions when the background
  loop has already exited: orphaned connectors are force-closed and the
  "never awaited" `RuntimeWarning` is suppressed.
- `AgentSession.resume()` no longer confuses `display_name` with the channel
  name; `session.name` is now the channel id.
- `AgentSession.resume()` snapshot_interval is applied on resumed sessions.
- `test_langgraph_live.py` no longer passes the nonexistent `verify_ssl`
  constructor kwarg.
- `subscribe()`/`stream()` docstrings now state the exclusive cursor
  semantics explicitly (they already matched `replay()` server-side).
- `ack_work` docstring warns that the ack cursor is a commit watermark over
  **channel** cursors — acking higher than contiguously processed skips
  events permanently.
- Module docstring example in `session.py` fixed to the async API (it showed
  sync `with`/`step()` on the async class).
- Removed the dead `[fast]` extras entry (`orjson` was never imported).

### Other

- Canonical port is now **8002** everywhere (dev, docker, docs, tests).
- Removed the obsolete `tests/test_state_snapshots.py` label-snapshot test
  surface (versioned state tests remain).
- Go SDK: readiness result simplified (no external-backplane flag); Go
  toolchain floor raised to `go 1.25.12` (patches stdlib advisories).

## 1.1.0

- LangGraph 1.x checkpointer protocol suite, Claude Agent SDK session store,
  OpenAI Agents tracing, Go SDK, tool-executions ledger, control-plane
  enrollment and key sync.
