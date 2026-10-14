"""
Codex CLI observability adapter for Actae.

Codex (the open-source OpenAI coding agent) has native local fork/resume via
its rollout JSONL + SQLite index — Actae does NOT re-implement that. Actae's
value for Codex is shared observability and cross-session comparison: Codex
can export OpenTelemetry **log events** describing each run (API requests,
token usage, user prompts, tool decisions/results) to an OTLP endpoint. This
adapter hosts that endpoint and forwards the ``codex.*`` events into Actae
channels keyed by ``conversation.id``, persisting a per-conversation state
snapshot so the dashboard, structural state-diff and decision-trail work on
Codex sessions exactly like on any other channel.

Enable Codex export in ``~/.codex/config.toml``::

    [otel]
    environment = "dev"
    log_user_prompt = false          # redact prompts unless you want them
    exporter = { otlp-http = {
      endpoint = "http://127.0.0.1:4319/v1/logs",
      protocol = "json"
    }}

Usage:

    from actae_client.adapters.codex import CodexOTLPReceiver

    receiver = CodexOTLPReceiver(actae_client, host="127.0.0.1", port=4319)
    await receiver.start()
    # ... run `codex` with the [otel] config above ...
    await receiver.stop()

The receiver listens on ``POST /v1/logs`` (OTLP/HTTP JSON log export) and
records each ``codex.*`` event to the Actae channel derived from the event's
``conversation.id``:

    codex.conversation_starts -> codex:api.<conversation_id>  (event + snapshot)
    codex.api_request         -> event + token snapshot
    codex.sse_event           -> event + token snapshot
    codex.user_prompt         -> event
    codex.tool_decision       -> event
    codex.tool_result         -> event + tool ledger snapshot

The per-conversation state snapshot accumulates tokens and tool calls, so
`GET /api/v1/channels/diff?left=<run-a>&right=<run-b>` compares two Codex
runs and the decision-trail shows the tool-execution ledger.

Codex's rollout JSONL remains the source of truth for local resume; this is a
read-only observability mirror and never modifies Codex state.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional

import aiohttp
from aiohttp import web

from ..client import ActaeClient
from .contract import adapter_checkpoint_state

logger = logging.getLogger("actae_client.adapters.codex")

# The OTLP log-record JSON path to the payload attributes. OTLP/HTTP JSON log
# export nests the codex.* event fields under:
#   resourceLogs[].scopeLogs[].logRecords[].body (string) / attributes (kv list)
_ACTOR = "codex-cli"


def _channel_for_conversation(conversation_id: str) -> str:
    """Deterministic Actae channel for a Codex conversation id."""
    return f"codex:{conversation_id}"


def _kv_to_dict(attrs: Any) -> Dict[str, Any]:
    """Convert an OTLP key/value array into a plain dict."""
    out: Dict[str, Any] = {}
    if not isinstance(attrs, list):
        return out
    for kv in attrs:
        if not isinstance(kv, dict):
            continue
        key = kv.get("key")
        value = kv.get("value", {})
        # value is a oneof: {"stringValue": ...} | {"intValue": ...} | ...
        if isinstance(value, dict):
            for vk in (
                "stringValue",
                "intValue",
                "doubleValue",
                "boolValue",
            ):
                if vk in value:
                    out[key] = value[vk]
                    break
            else:
                out[key] = value
        else:
            out[key] = value
    return out


class CodexOTLPReceiver:
    """An OTLP/HTTP log receiver that mirrors Codex CLI events into Actae.

    Listens for Codex's OTLP log export (``codex.*`` events) and records them
    to per-conversation Actae channels. State snapshots accumulate token
    usage and tool calls so Actae's diff / trail / dashboard work on Codex
    sessions.

    Args:
        actae: Connected ActaeClient instance.
        host: Bind host (default 127.0.0.1).
        port: Bind port (default 4319).
    """

    def __init__(self, actae: ActaeClient, host: str = "127.0.0.1", port: int = 4319):
        self.actae = actae
        self.host = host
        self.port = port
        self._runner: Optional[web.AppRunner] = None
        self._site: Optional[web.TCPSite] = None

    # ------------------------------------------------------------------ #
    # Lifecycle
    # ------------------------------------------------------------------ #

    async def start(self) -> None:
        """Start the OTLP receiver (binds host:port and serves /v1/logs)."""
        app = web.Application()
        app.router.add_post("/v1/logs", self._handle_logs)
        # Health/readiness for collectors.
        app.router.add_get("/health", self._handle_health)
        self._runner = web.AppRunner(app)
        await self._runner.setup()
        self._site = web.TCPSite(self._runner, self.host, self.port)
        await self._site.start()
        logger.info("Codex OTLP receiver listening on http://%s:%d/v1/logs", self.host, self.port)

    async def stop(self) -> None:
        """Stop the receiver and release the port."""
        if self._runner is not None:
            await self._runner.cleanup()
            self._runner = None
            self._site = None

    # ------------------------------------------------------------------ #
    # Handlers
    # ------------------------------------------------------------------ #

    async def _handle_health(self, _request: web.Request) -> web.Response:
        return web.json_response({"status": "ok"})

    async def _handle_logs(self, request: web.Request) -> web.Response:
        try:
            body = await request.json()
        except Exception:
            return web.json_response({"error": "invalid JSON"}, status=400)

        count = 0
        try:
            count = await self._process_logs(body)
        except Exception as exc:  # pragma: no cover - defensive
            logger.exception("failed to process OTLP logs: %s", exc)
            return web.json_response({"error": str(exc)}, status=500)

        return web.json_response({"processed": count})

    # ------------------------------------------------------------------ #
    # Parsing
    # ------------------------------------------------------------------ #

    async def _process_logs(self, body: Dict[str, Any]) -> int:
        """Parse an OTLP/HTTP JSON log export and mirror codex.* events."""
        count = 0
        resource_logs = body.get("resourceLogs") or []
        for rl in resource_logs:
            scope_logs = rl.get("scopeLogs") or []
            for sl in scope_logs:
                for lr in sl.get("logRecords") or []:
                    event = self._extract_event(lr)
                    if event is None:
                        continue
                    await self._mirror_event(event)
                    count += 1
        return count

    def _extract_event(self, log_record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Extract a codex.* event from one OTLP log record, or None."""
        attrs = _kv_to_dict(log_record.get("attributes") or [])
        # Codex emits the event name in the log body (e.g. "codex.api_request")
        # and per-event fields in attributes.
        body = log_record.get("body", {})
        if isinstance(body, dict):
            event_name = body.get("stringValue") or ""
        else:
            event_name = str(body or "")
        if not isinstance(event_name, str) or not event_name.startswith("codex."):
            return None
        # Merge attributes; conversation.id is the session key.
        event: Dict[str, Any] = {"name": event_name, "ts": log_record.get("timeUnixNano")}
        event.update(attrs)
        return event

    async def _mirror_event(self, event: Dict[str, Any]) -> None:
        """Record one codex.* event to its conversation channel."""
        conversation_id = str(event.get("conversation.id") or event.get("conversation_id") or "unknown")
        channel = _channel_for_conversation(conversation_id)
        name = event["name"]
        payload = {k: v for k, v in event.items() if k not in ("name", "ts")}

        try:
            await self.actae.record(
                channel,
                name,
                payload,
                actor=_ACTOR,
            )
        except Exception as exc:  # pragma: no cover - best-effort mirror
            logger.warning("failed to record codex event %s: %s", name, exc)
            return

        # Persist an accumulating state snapshot for diff/trail support.
        snapshot = await self._current_snapshot(channel)
        snapshot.setdefault("tokens", {})
        snapshot.setdefault("tools", [])

        if name == "codex.sse_event":
            for tok_key in ("input_token_count", "output_token_count", "cached_token_count", "reasoning_token_count", "tool_token_count"):
                if tok_key in event:
                    snapshot["tokens"][tok_key] = snapshot["tokens"].get(tok_key, 0) + int(event[tok_key] or 0)
        elif name == "codex.tool_result":
            snapshot["tools"].append({
                "tool_name": event.get("tool_name"),
                "call_id": event.get("call_id"),
                "duration_ms": event.get("duration_ms"),
                "success": event.get("success"),
            })
        elif name == "codex.conversation_starts":
            snapshot["conversation_id"] = conversation_id

        try:
            cursor = await self.actae.latest_cursor(channel) or 0
            await self.actae.save_state(
                channel,
                cursor,
                adapter_checkpoint_state(
                    snapshot,
                    framework="codex",
                    channel_id=channel,
                    portable_state={
                        "conversation_id": conversation_id,
                        "tokens": snapshot.get("tokens", {}),
                        "tools": snapshot.get("tools", []),
                    },
                    native_checkpoint={"resume_authority": "codex-rollout"},
                    event_cursor=cursor,
                ),
            )
        except Exception as exc:  # pragma: no cover
            logger.warning("failed to save codex snapshot for %s: %s", channel, exc)

    async def _current_snapshot(self, channel: str) -> Dict[str, Any]:
        try:
            snap = await self.actae.latest_state(channel)
            if snap is not None and isinstance(snap.get("state"), dict):
                return dict(snap["state"])
        except Exception:
            pass
        return {}
