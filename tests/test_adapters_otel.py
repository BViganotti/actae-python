"""Unit tests for the OpenTelemetry bridge adapter (no server, no otel dep).

The bridge is deliberately dependency-free: it drives any tracer-shaped object
(with ``start_as_current_span``), so these tests use an in-memory fake tracer.
The real OpenTelemetry SDK's ``Tracer`` satisfies the same shape.
"""

import asyncio
from types import SimpleNamespace

from actae_client.adapters.otel import (
    ACTAE_CHANNEL_ID,
    ACTAE_CURSOR,
    ACTAE_EVENT_TYPE,
    ACTAE_SPAN_ID,
    ACTAE_TRACE_ID,
    GEN_AI_REQUEST_MODEL,
    GEN_AI_TOOL_NAME,
    GEN_AI_USAGE_INPUT_TOKENS,
    ActaeOTelBridge,
    event_to_span,
    trace_context,
)


class _FakeSpanCM:
    def __init__(self, span):
        self._span = span

    def __enter__(self):
        return self._span

    def __exit__(self, *args):
        return None


class FakeTracer:
    def __init__(self):
        self.spans = []

    def start_as_current_span(self, name, *, attributes=None):
        span = {"name": name, "attributes": dict(attributes or {})}
        self.spans.append(span)
        return _FakeSpanCM(span)


def _event(event_type="agent.step", payload=None, metadata=None, cursor=1, actor="agent"):
    return SimpleNamespace(
        event_type=event_type,
        channel_id="ch-1",
        cursor=cursor,
        actor=actor,
        payload=payload if payload is not None else {},
        metadata=metadata,
    )


def test_trace_context_is_record_metadata():
    assert trace_context("4bf92f3577b34da6a3ce929d0e0e4736", "00f067aa0ba902b7") == {
        "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736",
        "span_id": "00f067aa0ba902b7",
    }
    assert trace_context("t") == {"trace_id": "t"}


def test_event_to_span_carries_actae_identity():
    span = event_to_span(_event(cursor=7))
    assert span.name == "agent.step"
    assert span.attributes[ACTAE_CHANNEL_ID] == "ch-1"
    assert span.attributes[ACTAE_CURSOR] == 7
    assert span.attributes[ACTAE_EVENT_TYPE] == "agent.step"


def test_event_to_span_lifts_genai_semconv():
    span = event_to_span(
        _event(
            "llm.call",
            payload={"model": "gpt-5", "input_tokens": 1200, "tool": "web_search", "latency_ms": 42},
        )
    )
    assert span.attributes[GEN_AI_REQUEST_MODEL] == "gpt-5"
    assert span.attributes[GEN_AI_USAGE_INPUT_TOKENS] == 1200
    assert span.attributes[GEN_AI_TOOL_NAME] == "web_search"
    assert span.attributes["actae.latency_ms"] == 42


def test_event_to_span_propagates_trace_context_from_metadata():
    span = event_to_span(_event(metadata={"trace_id": "abc", "span_id": "def"}))
    assert span.attributes[ACTAE_TRACE_ID] == "abc"
    assert span.attributes[ACTAE_SPAN_ID] == "def"


def test_event_to_span_ignores_non_primitive_and_missing_payload():
    span = event_to_span(_event(payload={"model": {"nested": 1}}))
    assert GEN_AI_REQUEST_MODEL not in span.attributes
    # A None/missing payload must not raise.
    assert event_to_span(_event(payload=None)).name == "agent.step"


def test_bridge_exports_spans_through_a_tracer():
    tracer = FakeTracer()
    bridge = ActaeOTelBridge(tracer)
    returned = bridge.export_event(_event("tool.started", payload={"tool": "charge"}))
    assert returned.name == "tool.started"
    assert len(tracer.spans) == 1
    assert tracer.spans[0]["name"] == "tool.started"
    assert tracer.spans[0]["attributes"][GEN_AI_TOOL_NAME] == "charge"


def test_bridge_export_channel_replays_then_exports():
    class FakeActae:
        def __init__(self):
            self.calls = []

        async def replay(self, channel_id, *, cursor, limit, event_type):
            self.calls.append((channel_id, cursor, limit, event_type))
            return [_event("a"), _event("b", cursor=2)]

    tracer = FakeTracer()
    bridge = ActaeOTelBridge(tracer)
    actae = FakeActae()
    spans = asyncio.run(bridge.export_channel(actae, "ch-1", cursor=3, limit=50, event_type="a"))
    assert [s.name for s in spans] == ["a", "b"]
    assert actae.calls == [("ch-1", 3, 50, "a")]
    assert len(tracer.spans) == 2
