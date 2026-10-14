"""
OpenAI Agents SDK tracing adapter for Actae.

Implements the ``agents.tracing.TracingProcessor`` interface backed by Actae.
Every agent run (trace) and every operation within it (spans: agent runs,
LLM generations, function/tool calls, guardrails, handoffs) is streamed as a
real-time event, and each completed run is persisted as a cursor-aligned,
versioned state snapshot — so workflows are observable live, replayable,
and resumable/comparable across runs.

Usage:
    from actae_client.adapters.openai_agents import install_actae_tracing

    install_actae_tracing(actae_client)          # call once, before Runner.run

    result = await Runner.run(agent, "Hello")  # everything is recorded

    # Inspect the latest run's full span tree:
    snapshot = await actae_client.latest_state("openai_agents")
    for span in snapshot["state"]["spans"]:
        print(span["type"], span["name"])
"""

import asyncio
import concurrent.futures
import datetime
import logging
import threading
from typing import Any, Dict, List, Optional

from ..client import ActaeClient
from .contract import adapter_checkpoint_state

try:
    from agents.tracing import TracingProcessor as _SDKTracingProcessor
    from agents.tracing import add_trace_processor

    _HAS_OPENAI_AGENTS = True
except ImportError:  # pragma: no cover - exercised via subprocess test
    _SDKTracingProcessor = object
    _HAS_OPENAI_AGENTS = False

logger = logging.getLogger("actae_client.adapters.openai_agents")

_EVENT_TRACE_START = "openai.trace.start"
_EVENT_SPAN_END = "openai.span.end"
_EVENT_TRACE_END = "openai.trace.end"
_ACTOR = "openai-agents"


def _iso_now() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def _span_summary(span: Any, include_inputs_outputs: bool) -> Dict[str, Any]:
    """Extract a compact, JSON-safe summary from a tracing span.

    Duck-typed against ``agents.tracing`` span/span_data objects so the
    adapter keeps working across SDK versions.
    """
    data = getattr(span, "span_data", None)
    summary: Dict[str, Any] = {
        "type": data.__class__.__name__ if data is not None else "unknown",
        "span_id": getattr(span, "span_id", None),
        "parent_id": getattr(span, "parent_id", None),
        "started_at": getattr(span, "started_at", None),
        "ended_at": getattr(span, "ended_at", None),
    }
    started = getattr(span, "started_at", None)
    ended = getattr(span, "ended_at", None)
    if started and ended:
        try:
            s = datetime.datetime.fromisoformat(started)
            e = datetime.datetime.fromisoformat(ended)
            summary["duration_ms"] = round((e - s).total_seconds() * 1000, 3)
        except ValueError:
            pass

    error = getattr(span, "error", None)
    if error:
        summary["error"] = {
            "message": error.get("message") if isinstance(error, dict) else str(error),
            "data": error.get("data") if isinstance(error, dict) else None,
        }

    if data is None:
        return summary

    for attr in ("name", "model", "agent_name", "to_agent", "from_agent",
                 "output_type", "turn", "triggered", "handoffs", "tools", "usage"):
        if hasattr(data, attr):
            summary[attr] = getattr(data, attr)

    if include_inputs_outputs:
        for attr in ("input", "output"):
            value = getattr(data, attr, None)
            if value is not None:
                summary[attr] = value

    if hasattr(data, "metadata") and getattr(data, "metadata"):
        summary["metadata"] = data.metadata
    if hasattr(data, "model_config") and getattr(data, "model_config"):
        summary["model_config"] = data.model_config
    return summary


class ActaeTracingProcessor:
    """``agents.tracing.TracingProcessor`` that mirrors traces into Actae.

    Every span end is recorded as an ``openai.span.end`` event; every trace
    is recorded as ``openai.trace.start`` / ``openai.trace.end`` events and
    persisted as a cursor-aligned state snapshot containing the full span
    tree. Persistence runs on a dedicated background event loop so the
    processor works from both sync and async ``Runner.run`` calls without
    blocking the workflow.
    """

    def __init__(
        self,
        actae: ActaeClient,
        channel: str = "openai_agents",
        include_inputs_outputs: bool = True,
        flush_timeout: float = 5.0,
    ):
        """Create an Actae tracing processor.

        Args:
            actae: Connected ActaeClient instance.
            channel: Channel for tracing events and run snapshots
                (default ``"openai_agents"``).
            include_inputs_outputs: Persist span input/output payloads.
                Set ``False`` to keep only names/timings/errors (e.g. to
                avoid storing sensitive content).
            flush_timeout: Seconds ``force_flush``/``shutdown`` wait for
                pending Actae writes (default 5).
        """
        if not _HAS_OPENAI_AGENTS:
            raise ImportError(
                "ActaeTracingProcessor requires openai-agents; "
                "install with `pip install 'actae-client[openai-agents]'`"
            )
        self.actae = actae
        self.channel = channel
        self.include_inputs_outputs = include_inputs_outputs
        self.flush_timeout = flush_timeout
        self._traces: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.Lock()
        self._pending: List[concurrent.futures.Future] = []
        self._bg_loop: Optional[asyncio.AbstractEventLoop] = None
        self._bg_thread: Optional[threading.Thread] = None
        self._bg_start_lock = threading.Lock()

    # ------------------------------------------------------------------ #
    # TracingProcessor protocol (sync callbacks)
    # ------------------------------------------------------------------ #

    def on_trace_start(self, trace: Any) -> None:
        try:
            trace_id = trace.trace_id
            with self._lock:
                self._traces[trace_id] = {
                    "name": getattr(trace, "name", None),
                    "group_id": getattr(trace, "group_id", None),
                    "metadata": getattr(trace, "metadata", None),
                    "started_at": _iso_now(),
                    "spans": [],
                }
            self._schedule(self._record_trace_start(trace_id, trace))
        except Exception:
            logger.exception("ActaeTracingProcessor.on_trace_start failed")

    def on_span_start(self, span: Any) -> None:
        try:
            trace_id = span.trace_id
            with self._lock:
                self._traces.setdefault(trace_id, {
                    "name": None,
                    "group_id": None,
                    "metadata": None,
                    "started_at": _iso_now(),
                    "spans": [],
                })
        except Exception:
            logger.exception("ActaeTracingProcessor.on_span_start failed")

    def on_span_end(self, span: Any) -> None:
        try:
            trace_id = span.trace_id
            summary = _span_summary(span, self.include_inputs_outputs)
            with self._lock:
                entry = self._traces.get(trace_id)
                if entry is None:
                    entry = self._traces[trace_id] = {
                        "name": None,
                        "group_id": None,
                        "metadata": None,
                        "started_at": _iso_now(),
                        "spans": [],
                    }
                entry["spans"].append(summary)
            self._schedule(self._record_span_end(trace_id, summary))
        except Exception:
            logger.exception("ActaeTracingProcessor.on_span_end failed")

    def on_trace_end(self, trace: Any) -> None:
        try:
            trace_id = trace.trace_id
            with self._lock:
                entry = self._traces.pop(trace_id, None)
            if entry is None:
                return
            entry["ended_at"] = _iso_now()
            self._schedule(self._persist_trace(trace_id, trace, entry))
        except Exception:
            logger.exception("ActaeTracingProcessor.on_trace_end failed")

    def force_flush(self) -> None:
        """Block until pending Actae writes complete (best effort).

        Synchronous counterpart of :meth:`flush` — waits up to
        ``flush_timeout`` seconds for all posted writes.
        """
        self._wait_pending(self.flush_timeout)

    def shutdown(self, timeout: Optional[float] = None) -> None:
        """Flush pending writes and stop the background loop."""
        deadline = timeout if timeout is not None else self.flush_timeout
        self._wait_pending(deadline)
        loop = self._bg_loop
        if loop is not None:
            try:
                loop.call_soon_threadsafe(loop.stop)
            except RuntimeError:
                pass
            self._bg_loop = None

    # ------------------------------------------------------------------ #
    # Async helpers
    # ------------------------------------------------------------------ #

    async def flush(self) -> None:
        """Await all pending Actae writes (call during graceful shutdown)."""
        pending = list(self._pending)
        if not pending:
            return
        futures = [asyncio.wrap_future(f) for f in pending]
        await asyncio.gather(*futures, return_exceptions=True)

    async def fork_trace(
        self,
        trace_id: str,
        new_channel: str,
        *,
        reason: Optional[str] = None,
    ) -> None:
        """Fork a specific run's snapshot into a new channel.

        The OpenAI Agents SDK is stateless (traces only, no native resume), so
        Actae's value is observability + cross-run comparison: this copies the
        snapshot of one completed run (``trace_id``) into ``new_channel`` so
        you can compare two runs structurally (``diff``) or keep a fork of a
        run for reference. The Agents SDK does not resume — it re-runs — so
        this is a fork-for-comparison, not a fork-for-resume.

        Args:
            trace_id: The run's trace id to fork.
            new_channel: Channel ID for the fork.
            reason: Optional human-readable reason recorded on the fork.

        Raises:
            ValueError: If no snapshot for ``trace_id`` is found.
        """
        # Locate the version whose snapshot carries this trace_id (newest first).
        target_cursor: Optional[int] = None
        offset = 0
        while True:
            versions = await self.actae.list_states(self.channel, limit=100, offset=offset)
            if not versions:
                break
            for sv in versions:
                snap = await self.actae.get_state(self.channel, sv["version"])
                state = (snap or {}).get("state", {})
                if state.get("trace_id") == trace_id:
                    target_cursor = sv["cursor"]
                    break
            if target_cursor is not None:
                break
            offset += len(versions)
            if len(versions) < 100:
                break
        if target_cursor is None:
            raise ValueError(f"no Actae snapshot found for trace_id '{trace_id}'")

        await self.actae.fork(
            self.channel,
            new_channel,
            target_cursor,
            display_name=new_channel,
            reason=reason or f"Forked run {trace_id} → {new_channel}",
        )

    async def _record_trace_start(self, trace_id: str, trace: Any) -> None:
        try:
            await self.actae.record(
                self.channel,
                _EVENT_TRACE_START,
                {
                    "trace_id": trace_id,
                    "workflow_name": getattr(trace, "name", None),
                    "group_id": getattr(trace, "group_id", None),
                },
                actor=_ACTOR,
            )
        except Exception:
            logger.exception("failed to record openai trace start")

    async def _record_span_end(self, trace_id: str, summary: Dict[str, Any]) -> None:
        try:
            await self.actae.record(
                self.channel,
                _EVENT_SPAN_END,
                {"trace_id": trace_id, "span": summary},
                actor=_ACTOR,
            )
        except Exception:
            logger.exception("failed to record openai span end")

    async def _persist_trace(
        self,
        trace_id: str,
        trace: Any,
        entry: Dict[str, Any],
    ) -> None:
        try:
            snapshot = {
                "trace_id": trace_id,
                "workflow_name": entry["name"],
                "group_id": entry["group_id"],
                "metadata": entry["metadata"],
                "started_at": entry["started_at"],
                "ended_at": entry["ended_at"],
                "spans": entry["spans"],
            }
            snapshot = adapter_checkpoint_state(
                snapshot,
                framework="openai-agents",
                channel_id=self.channel,
                portable_state={
                    "trace_id": trace_id,
                    "workflow_name": entry["name"],
                    "span_count": len(entry["spans"]),
                },
                native_checkpoint={"resume": "unavailable"},
            )
            await self.actae.transition(
                self.channel,
                _EVENT_TRACE_END,
                {
                    "trace_id": trace_id,
                    "workflow_name": entry["name"],
                    "group_id": entry["group_id"],
                    "span_count": len(entry["spans"]),
                    "started_at": entry["started_at"],
                    "ended_at": entry["ended_at"],
                },
                snapshot,
                actor=_ACTOR,
            )
        except Exception:
            logger.exception("failed to persist openai trace")

    # ------------------------------------------------------------------ #
    # Background loop plumbing
    # ------------------------------------------------------------------ #

    def _schedule(self, coro: Any) -> None:
        """Post a coroutine to the background loop without blocking."""
        loop = self._get_bg_loop()
        future = asyncio.run_coroutine_threadsafe(coro, loop)
        with self._lock:
            self._pending.append(future)
            while len(self._pending) > 1000:
                self._pending.pop(0)

    def _get_bg_loop(self) -> asyncio.AbstractEventLoop:
        if self._bg_loop is not None:
            return self._bg_loop
        with self._bg_start_lock:
            if self._bg_loop is None:
                loop = asyncio.new_event_loop()

                def _run() -> None:
                    asyncio.set_event_loop(loop)
                    loop.run_forever()

                thread = threading.Thread(
                    target=_run, name="actae-openai-agents", daemon=True
                )
                thread.start()
                self._bg_loop = loop
                self._bg_thread = thread
        return self._bg_loop

    def _wait_pending(self, timeout: Optional[float]) -> None:
        if not self._pending:
            return
        if timeout is None or timeout <= 0:
            timeout = 0.1
        try:
            done, _ = concurrent.futures.wait(
                self._pending, timeout=timeout, return_when=concurrent.futures.ALL_COMPLETED
            )
            self._pending = [f for f in self._pending if f not in done]
        except Exception:
            logger.exception("ActaeTracingProcessor flush failed")


def install_actae_tracing(
    actae: ActaeClient,
    channel: str = "openai_agents",
    include_inputs_outputs: bool = True,
) -> ActaeTracingProcessor:
    """Register an Actae tracing processor with the OpenAI Agents SDK.

    Adds the processor alongside any existing processors (OpenAI's default
    backend tracing keeps working). Call once before ``Runner.run`` and keep
    the returned processor if you need :meth:`ActaeTracingProcessor.flush`
    during shutdown.

    Raises:
        ImportError: If ``openai-agents`` is not installed (install with
            ``pip install 'actae-client[openai-agents]'``).
    """
    if not _HAS_OPENAI_AGENTS:
        raise ImportError(
            "install_actae_tracing requires openai-agents; "
            "install with `pip install 'actae-client[openai-agents]'`"
        )
    processor = ActaeTracingProcessor(
        actae, channel=channel, include_inputs_outputs=include_inputs_outputs
    )
    add_trace_processor(processor)
    return processor


def uninstall_actae_tracing(processor: ActaeTracingProcessor) -> bool:
    """Best-effort removal of an Actae tracing processor.

    The OpenAI Agents SDK has no public removal API; this reaches into the
    current trace provider's processor list. Returns ``False`` if the
    processor could not be removed (SDK internals changed).
    """
    try:
        from agents.tracing import get_trace_provider

        provider = get_trace_provider()
        multi = provider._multi_processor
        with multi._lock:
            multi._processors = tuple(
                p for p in multi._processors if p is not processor
            )
        return True
    except Exception:
        logger.warning("uninstall_actae_tracing failed", exc_info=True)
        return False
