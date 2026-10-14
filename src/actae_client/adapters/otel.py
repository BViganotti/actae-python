"""Export Actae execution events to OpenTelemetry as GenAI spans (optional).

Actae is the execution layer *under* observability: this bridge mirrors a
channel's durable events into an OTel tracer so a Langfuse / Phoenix / OTel
backend can chart latency, tokens and errors next to your traces. It does not
replace those tools, and it does not change execution — it only reads events.

The core SDK has **no OpenTelemetry dependency**. Pass any tracer exposing
``start_as_current_span(name, attributes=...)`` (the real OpenTelemetry SDK's
``Tracer`` qualifies); this module never imports ``opentelemetry`` itself, so
it works with the SDK, a test double, or a custom exporter.

Mapping:

- span name = the Actae ``event_type``;
- ``actae.*`` attributes (channel, cursor, actor, event type) make the span
  traceable back to the durable record;
- ``gen_ai.*`` attributes (``gen_ai.request.model``, usage tokens,
  ``gen_ai.tool.name``) are lifted from the payload when present, following the
  OpenTelemetry GenAI semantic conventions;
- ``actae.trace_id`` / ``actae.span_id`` are lifted from the event metadata when
  the caller attached them (see :func:`trace_context`), so an Actae execution
  can be correlated with an external trace.
"""

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterable, List, Optional, Protocol

# OpenTelemetry GenAI semantic-convention attribute keys (subset). Kept as
# literals so the core SDK carries no opentelemetry dependency.
GEN_AI_OPERATION_NAME = "gen_ai.operation.name"
GEN_AI_REQUEST_MODEL = "gen_ai.request.model"
GEN_AI_RESPONSE_MODEL = "gen_ai.response.model"
GEN_AI_USAGE_INPUT_TOKENS = "gen_ai.usage.input_tokens"
GEN_AI_USAGE_OUTPUT_TOKENS = "gen_ai.usage.output_tokens"
GEN_AI_TOOL_NAME = "gen_ai.tool.name"

ACTAE_CHANNEL_ID = "actae.channel_id"
ACTAE_CURSOR = "actae.cursor"
ACTAE_EVENT_TYPE = "actae.event_type"
ACTAE_ACTOR = "actae.actor"
ACTAE_TRACE_ID = "actae.trace_id"
ACTAE_SPAN_ID = "actae.span_id"
ACTAE_LATENCY_MS = "actae.latency_ms"


class _SpanContextManager(Protocol):
    def __enter__(self) -> Any: ...
    def __exit__(self, *args: Any) -> None: ...


class _Tracer(Protocol):
    def start_as_current_span(
        self, name: str, *, attributes: Optional[Dict[str, Any]] = None
    ) -> _SpanContextManager: ...


@dataclass
class ActaeSpan:
    """A span-shaped projection of one Actae event."""

    name: str
    attributes: Dict[str, Any] = field(default_factory=dict)


def trace_context(trace_id: str, span_id: Optional[str] = None) -> Dict[str, str]:
    """Build event metadata that correlates an Actae event with an OTel trace.

    Pass the result as the ``metadata`` of ``record()`` / ``transition()`` /
    ``AgentSession.step()``::

        await session.step("llm.call", metadata=trace_context(trace_id, span_id))
    """
    ctx: Dict[str, str] = {"trace_id": trace_id}
    if span_id:
        ctx["span_id"] = span_id
    return ctx


def _primitive(value: Any) -> Optional[Any]:
    if isinstance(value, (str, bool, int, float)):
        return value
    return None


def _first(payload: Dict[str, Any], *keys: str) -> Optional[Any]:
    for key in keys:
        if key in payload:
            value = _primitive(payload[key])
            if value is not None:
                return value
    return None


def event_to_span(event: Any) -> ActaeSpan:
    """Project an Actae event onto a span name + OTel attributes.

    Pure and dependency-free: the unit tests exercise this directly.
    """
    attributes: Dict[str, Any] = {
        ACTAE_CHANNEL_ID: getattr(event, "channel_id", ""),
        ACTAE_CURSOR: getattr(event, "cursor", 0),
        ACTAE_EVENT_TYPE: getattr(event, "event_type", ""),
        ACTAE_ACTOR: getattr(event, "actor", ""),
    }

    metadata = getattr(event, "metadata", None) or {}
    if isinstance(metadata, dict):
        if _primitive(metadata.get("trace_id")) is not None:
            attributes[ACTAE_TRACE_ID] = metadata["trace_id"]
        if _primitive(metadata.get("span_id")) is not None:
            attributes[ACTAE_SPAN_ID] = metadata["span_id"]

    payload = getattr(event, "payload", None)
    if isinstance(payload, dict):
        model = _first(payload, "model", "request_model", "response_model")
        if model is not None:
            attributes[GEN_AI_REQUEST_MODEL] = model
        response_model = _first(payload, "response_model")
        if response_model is not None:
            attributes[GEN_AI_RESPONSE_MODEL] = response_model
        tool = _first(payload, "tool", "tool_name")
        if tool is not None:
            attributes[GEN_AI_TOOL_NAME] = tool
        operation = _first(payload, "operation", "operation_name")
        if operation is not None:
            attributes[GEN_AI_OPERATION_NAME] = operation
        input_tokens = _first(payload, "input_tokens", "prompt_tokens", "tokens")
        if input_tokens is not None:
            attributes[GEN_AI_USAGE_INPUT_TOKENS] = input_tokens
        output_tokens = _first(payload, "output_tokens", "completion_tokens")
        if output_tokens is not None:
            attributes[GEN_AI_USAGE_OUTPUT_TOKENS] = output_tokens
        latency = _first(payload, "latency_ms", "duration_ms")
        if latency is not None:
            attributes[ACTAE_LATENCY_MS] = latency

    return ActaeSpan(name=str(getattr(event, "event_type", "actae.event")), attributes=attributes)


class ActaeOTelBridge:
    """Mirror Actae events into an OTel-shaped tracer (no runtime dependency).

    ``tracer`` is any object with ``start_as_current_span(name,
    attributes=...)`` returning a context manager. With real OpenTelemetry::

        from opentelemetry import trace
        bridge = ActaeOTelBridge(trace.get_tracer("actae"))
    """

    def __init__(self, tracer: Any) -> None:
        self._tracer = tracer

    def export_event(self, event: Any) -> ActaeSpan:
        span = event_to_span(event)
        with self._tracer.start_as_current_span(span.name, attributes=span.attributes):
            pass
        return span

    def export_events(self, events: Iterable[Any]) -> List[ActaeSpan]:
        return [self.export_event(event) for event in events]

    async def export_channel(
        self,
        actae: Any,
        channel_id: str,
        *,
        cursor: int = 0,
        limit: int = 1000,
        event_type: Optional[str] = None,
    ) -> List[ActaeSpan]:
        """Replay a channel once and export every event as a span."""
        events = await actae.replay(
            channel_id, cursor=cursor, limit=limit, event_type=event_type
        )
        return self.export_events(events)
