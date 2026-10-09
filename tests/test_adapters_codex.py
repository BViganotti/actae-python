"""Unit tests for the Codex OTLP receiver adapter (no server needed)."""

import asyncio
import copy
from typing import Any, Dict, List, Optional

import pytest

from actae_client.adapters.codex import CodexOTLPReceiver, _channel_for_conversation


class FakeActae:
    """In-memory Actae client implementing the primitives the adapter uses."""

    def __init__(self):
        self.states: Dict[str, List[Dict[str, Any]]] = {}
        self.events: List[Dict[str, Any]] = []

    async def latest_cursor(self, channel_id: str) -> Optional[int]:
        counts = {}
        for ev in self.events:
            counts[ev["channel_id"]] = counts.get(ev["channel_id"], 0) + 1
        return counts.get(channel_id)

    async def record(self, channel_id: str, event_type: str, payload: Any, **kwargs: Any):
        cursor = (await self.latest_cursor(channel_id) or 0) + 1
        self.events.append(
            {"channel_id": channel_id, "type": event_type,
             "payload": payload, "actor": kwargs.get("actor"), "cursor": cursor}
        )

    async def save_state(self, channel_id: str, cursor: int, state: Any) -> int:
        self.states.setdefault(channel_id, []).append(
            {"version": len(self.states.get(channel_id, [])) + 1,
             "cursor": cursor, "state": copy.deepcopy(state)}
        )
        return len(self.states[channel_id])

    async def latest_state(self, channel_id: str) -> Optional[Dict[str, Any]]:
        versions = self.states.get(channel_id)
        if not versions:
            return None
        latest = versions[-1]
        return {"cursor": latest["cursor"], "state": copy.deepcopy(latest["state"])}


def _log_record(name: str, conversation_id: str = "conv-1", **extra) -> Dict[str, Any]:
    attrs = [{"key": "conversation.id", "value": {"stringValue": conversation_id}}]
    for k, v in extra.items():
        if isinstance(v, int):
            attrs.append({"key": k, "value": {"intValue": v}})
        else:
            attrs.append({"key": k, "value": {"stringValue": str(v)}})
    return {
        "timeUnixNano": "123",
        "body": {"stringValue": name},
        "attributes": attrs,
    }


def _otlp_payload(*records) -> Dict[str, Any]:
    return {"resourceLogs": [{"scopeLogs": [{"logRecords": list(records)}]}]}


def test_channel_for_conversation():
    assert _channel_for_conversation("abc").startswith("codex:")


def test_extract_only_codex_events():
    receiver = CodexOTLPReceiver(FakeActae())
    record = _log_record("codex.api_request", conversation_id="c1")
    event = receiver._extract_event(record)
    assert event["name"] == "codex.api_request"
    assert event["conversation.id"] == "c1"
    # Non-codex events are ignored.
    other = _log_record("random.log")
    assert receiver._extract_event(other) is None


def test_process_logs_mirrors_events_and_snapshot():
    actae = FakeActae()
    receiver = CodexOTLPReceiver(actae)
    payload = _otlp_payload(
        _log_record("codex.conversation_starts", conversation_id="conv-1"),
        _log_record("codex.sse_event", conversation_id="conv-1",
                    input_token_count=100, output_token_count=50),
        _log_record("codex.tool_result", conversation_id="conv-1",
                    tool_name="run_shell", call_id="call-1", success="true"),
    )
    count = asyncio.run(receiver._process_logs(payload))
    assert count == 3
    # Events recorded to the conversation channel.
    assert all(ev["channel_id"] == _channel_for_conversation("conv-1") for ev in actae.events)
    types = [ev["type"] for ev in actae.events]
    assert "codex.conversation_starts" in types
    assert "codex.sse_event" in types
    assert "codex.tool_result" in types
    # State snapshot accumulates tokens + tool ledger.
    ch = _channel_for_conversation("conv-1")
    snap = actae.states[ch][-1]["state"]
    assert snap["tokens"]["input_token_count"] == 100
    assert snap["tokens"]["output_token_count"] == 50
    assert snap["tools"][0]["tool_name"] == "run_shell"
    assert snap["conversation_id"] == "conv-1"


def test_process_logs_accumulates_tokens_across_records():
    actae = FakeActae()
    receiver = CodexOTLPReceiver(actae)
    payload = _otlp_payload(
        _log_record("codex.sse_event", conversation_id="c1", input_token_count=10),
        _log_record("codex.sse_event", conversation_id="c1", input_token_count=5, output_token_count=7),
    )
    asyncio.run(receiver._process_logs(payload))
    ch = _channel_for_conversation("c1")
    snap = actae.states[ch][-1]["state"]
    assert snap["tokens"]["input_token_count"] == 15
    assert snap["tokens"]["output_token_count"] == 7


def test_process_logs_ignores_empty_body():
    actae = FakeActae()
    receiver = CodexOTLPReceiver(actae)
    count = asyncio.run(receiver._process_logs({"resourceLogs": []}))
    assert count == 0
    assert actae.events == []


def test_http_round_trip():
    """POST /v1/logs with a real OTLP JSON body via the aiohttp handler."""
    import aiohttp

    actae = FakeActae()

    async def scenario():
        receiver = CodexOTLPReceiver(actae, host="127.0.0.1", port=0)
        await receiver.start()
        try:
            sockets = receiver._site._server.sockets
            port = sockets[0].getsockname()[1] if sockets else 0
            assert port, "failed to determine bound port"
            async with aiohttp.ClientSession() as sess:
                body = _otlp_payload(
                    _log_record("codex.sse_event", conversation_id="http-1", input_token_count=42),
                )
                async with sess.post(f"http://127.0.0.1:{port}/v1/logs", json=body) as resp:
                    assert resp.status == 200, await resp.text()
                    data = await resp.json()
                    assert data["processed"] == 1
        finally:
            await receiver.stop()

    asyncio.run(scenario())
    assert len(actae.events) == 1
    assert actae.events[0]["type"] == "codex.sse_event"
