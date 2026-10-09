# Migrating to actae-client 1.2.0

This guide covers the breaking changes in 1.2.0 and how to update your code.

## 1. `save_snapshot` / `list_snapshots` → versioned state

The label-based snapshot API (`POST /api/v1/snapshots`) was removed
server-side. The versioned state API is a strict superset (every save is an
immutable, cursor-aligned version).

**Before:**

```python
await client.save_snapshot("ch", cursor, state, label="checkpoint-1")
snapshots = await client.list_snapshots("ch")
```

**After:**

```python
version = await client.save_state("ch", cursor, state)
versions = await client.list_states("ch")          # [{"version": N, "cursor": ..., ...}]
state = await client.latest_state("ch")            # {"cursor": ..., "state": ...}
```

If you used the label for human identification, put it in the state blob or
in channel metadata (`update_metadata(experiment_metadata=...)`).

## 2. `ConnectionError` → `ActaeConnectionError`

`actae_client` no longer exports `ConnectionError`, so the builtin is never
shadowed.

**Before:**

```python
from actae_client import ConnectionError
try:
    await client.connect()
except ConnectionError:
    ...
```

**After:**

```python
from actae_client import ActaeConnectionError
try:
    await client.connect()
except ActaeConnectionError:
    ...
```

(If you previously wrote `except ConnectionError` expecting the builtin, it
now works correctly without any change.)

## 3. `ReadinessResult` simplified

The external-backplane readiness field no longer exists — the server is
standalone single-node. Drop any code reading it.

## 4. `subscribe()` now waits by default

`subscribe(topic)` returns only after the server confirms the subscription
(previously it returned immediately and events could be missed).

- If you relied on the fire-and-forget behavior, pass `wait=False`.
- If you previously passed `wait=True`, nothing changes.
- `AgentSession`, the reconnect loop, and `stream()` are unaffected.

## 5. No other API changes

Everything else (all 50+ methods, dataclasses, adapters, `AgentSession`,
errors) keeps the same names and signatures.

## 6. HTTP 401 now raises `AuthError` (was `APIError`)

A bad/expired API key or a wrong password on `login()` raises `AuthError`
instead of `APIError`, matching WebSocket auth failures. If you previously
caught `APIError` to handle 401s, catch `AuthError` (or the base
`ActaeError`) instead. This only affects HTTP calls returning 401.

## 7. HTTP transport failures raise `ActaeConnectionError`

Raw aiohttp exceptions (`ClientConnectorError`, `ServerDisconnectedError`,
timeouts) on HTTP calls are now wrapped in `ActaeConnectionError`, so
`except ActaeError` catches every SDK failure. Code that imported aiohttp
to catch those exceptions should catch `ActaeConnectionError` instead.
HTTP *status* errors still raise `APIError` and friends.

## 8. `actae-client[fast]` extra removed

The `orjson`-based `[fast]` extra was dead code (never imported). Pip
installs using it fail; drop the extra from your install line.

## 9. New in 1.2.0 (additive)

- `transition_sync()` / `fork_sync()` — blocking variants of `transition`
  and `fork`, completing the sync facade.
- `AgentSession.state_fn` accepts `async def` functions (awaited before
  saving). Previously an async function silently never saved state.
- `schedule_wakeup(run_at=...)` accepts a timezone-aware `datetime`
  (serialized as UTC); naive datetimes raise `ValueError`.
- Auto-reconnect resumes topics at the last received event — no more
  duplicate redelivery after a drop. Deduplicate by `event.id` if you ever
  need belt-and-braces single-processing on top (delivery is already
  deduplicated; the event ID is the stable key).
