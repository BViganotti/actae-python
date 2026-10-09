"""Framework-independent building blocks for Actae agent integrations.

Framework adapters intentionally remain thin: the framework owns execution,
while Actae owns durable events, checkpoints, forks, coordination and the
idempotent tool ledger.  This module defines the stable seam between them.
"""

from __future__ import annotations

import asyncio
import inspect
import json
import traceback
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Awaitable, Callable, Dict, FrozenSet, Mapping, Optional, Union

from ..client import ActaeClient


CHECKPOINT_SCHEMA = "actae.framework-checkpoint/v1"
CHECKPOINT_METADATA_KEY = "_actae_adapter"


class ActaeFeature(str, Enum):
    """Actae capabilities available at a framework integration boundary."""

    EVENTS = "events"
    REPLAY = "replay"
    CHECKPOINTS = "checkpoints"
    RESUME = "resume"
    FORKS = "forks"
    TOOL_EXECUTIONS = "tool_executions"
    EXPERIMENTS = "experiments"
    EXECUTION_GROUPS = "execution_groups"
    WAKEUPS = "wakeups"
    CAUSAL_LINEAGE = "causal_lineage"


FULL_FEATURE_SET: FrozenSet[ActaeFeature] = frozenset(ActaeFeature)


class AdapterSupportLevel(str, Enum):
    CERTIFIED = "certified"
    PREVIEW = "preview"
    OBSERVABILITY = "observability"


class ResumeFidelity(str, Enum):
    """How faithfully a framework can restart from an Actae snapshot."""

    CHECKPOINT_EXACT = "checkpoint_exact"
    SESSION_NATIVE = "session_native"
    RECONSTRUCTED = "reconstructed"
    CONTEXT_SEEDED = "context_seeded"
    OBSERVE_ONLY = "observe_only"


class AdapterContractError(ValueError):
    """The framework adapter supplied invalid or incompatible state."""


class ToolExecutionInProgressError(RuntimeError):
    """Another worker owns an unexpired lease for the same tool operation."""

    def __init__(self, execution_id: str, key_name: str):
        self.execution_id = execution_id
        self.key_name = key_name
        super().__init__(
            "tool execution {!r} is already in progress (execution_id={})".format(
                key_name, execution_id
            )
        )


@dataclass(frozen=True)
class AdapterCapabilities:
    """Machine-readable, testable support claim for one framework adapter."""

    framework: str
    adapter_version: str
    support_level: AdapterSupportLevel
    resume_fidelity: ResumeFidelity
    features: FrozenSet[ActaeFeature]
    native_checkpoint: bool
    notes: str = ""

    def __post_init__(self) -> None:
        if not self.framework.strip():
            raise AdapterContractError("framework must not be empty")
        if not self.adapter_version.strip():
            raise AdapterContractError("adapter_version must not be empty")
        object.__setattr__(self, "features", frozenset(self.features))

        if ActaeFeature.RESUME in self.features and ActaeFeature.CHECKPOINTS not in self.features:
            raise AdapterContractError("resume support requires checkpoint support")
        if ActaeFeature.FORKS in self.features and ActaeFeature.CHECKPOINTS not in self.features:
            raise AdapterContractError("fork support requires checkpoint support")
        if self.native_checkpoint and ActaeFeature.CHECKPOINTS not in self.features:
            raise AdapterContractError("native_checkpoint requires checkpoint support")

    def supports(self, feature: ActaeFeature) -> bool:
        return feature in self.features

    @property
    def missing_features(self) -> FrozenSet[ActaeFeature]:
        return FULL_FEATURE_SET.difference(self.features)

    @property
    def full_parity(self) -> bool:
        return not self.missing_features

    def to_dict(self) -> Dict[str, Any]:
        return {
            "framework": self.framework,
            "adapter_version": self.adapter_version,
            "support_level": self.support_level.value,
            "resume_fidelity": self.resume_fidelity.value,
            "features": sorted(feature.value for feature in self.features),
            "native_checkpoint": self.native_checkpoint,
            "full_parity": self.full_parity,
            "notes": self.notes,
        }


@dataclass(frozen=True)
class FrameworkEvent:
    """Portable event emitted by adapters in addition to framework details."""

    kind: str
    framework: str
    run_id: str
    payload: Mapping[str, Any] = field(default_factory=dict)
    parent_run_id: Optional[str] = None
    agent_id: Optional[str] = None
    occurred_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    def to_payload(self) -> Dict[str, Any]:
        value = {
            "schema": "actae.framework-event/v1",
            "kind": self.kind,
            "framework": self.framework,
            "run_id": self.run_id,
            "parent_run_id": self.parent_run_id,
            "agent_id": self.agent_id,
            "occurred_at": self.occurred_at,
            "data": dict(self.payload),
        }
        _assert_json(value, "framework event")
        return value


@dataclass(frozen=True)
class CheckpointEnvelope:
    """Versioned checkpoint shared by all adapters.

    ``portable_state`` contains stable, cross-framework data.  The optional
    ``native_checkpoint`` remains opaque and is interpreted only by the
    adapter that created it.  Actae stores both as JSON without taking over
    execution from the framework.
    """

    framework: str
    adapter_version: str
    channel_id: str
    portable_state: Mapping[str, Any] = field(default_factory=dict)
    native_checkpoint: Any = None
    application_state: Mapping[str, Any] = field(default_factory=dict)
    pending_work: Mapping[str, Any] = field(default_factory=dict)
    manifest: Mapping[str, Any] = field(default_factory=dict)
    event_cursor: Optional[int] = None
    event_id: Optional[str] = None
    framework_version: Optional[str] = None
    created_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    schema: str = CHECKPOINT_SCHEMA

    def __post_init__(self) -> None:
        if self.schema != CHECKPOINT_SCHEMA:
            raise AdapterContractError("unsupported checkpoint schema: {}".format(self.schema))
        for label, value in (
            ("framework", self.framework),
            ("adapter_version", self.adapter_version),
            ("channel_id", self.channel_id),
        ):
            if not isinstance(value, str) or not value.strip():
                raise AdapterContractError("{} must not be empty".format(label))
        if self.event_cursor is not None and self.event_cursor < 0:
            raise AdapterContractError("event_cursor must be non-negative")
        _assert_json(self.to_dict(), "checkpoint envelope")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "schema": self.schema,
            "framework": self.framework,
            "framework_version": self.framework_version,
            "adapter_version": self.adapter_version,
            "channel_id": self.channel_id,
            "event_cursor": self.event_cursor,
            "event_id": self.event_id,
            "created_at": self.created_at,
            "portable_state": dict(self.portable_state),
            "native_checkpoint": self.native_checkpoint,
            "application_state": dict(self.application_state),
            "pending_work": dict(self.pending_work),
            "manifest": dict(self.manifest),
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "CheckpointEnvelope":
        if not isinstance(value, Mapping):
            raise AdapterContractError("checkpoint envelope must be an object")
        try:
            return cls(
                schema=value.get("schema", ""),
                framework=value["framework"],
                framework_version=value.get("framework_version"),
                adapter_version=value["adapter_version"],
                channel_id=value["channel_id"],
                event_cursor=value.get("event_cursor"),
                event_id=value.get("event_id"),
                created_at=value.get("created_at") or datetime.now(timezone.utc).isoformat(),
                portable_state=_mapping(value.get("portable_state"), "portable_state"),
                native_checkpoint=value.get("native_checkpoint"),
                application_state=_mapping(value.get("application_state"), "application_state"),
                pending_work=_mapping(value.get("pending_work"), "pending_work"),
                manifest=_mapping(value.get("manifest"), "manifest"),
            )
        except KeyError as exc:
            raise AdapterContractError("checkpoint envelope is missing {}".format(exc.args[0])) from exc


def embed_checkpoint_metadata(
    state: Mapping[str, Any], envelope: CheckpointEnvelope
) -> Dict[str, Any]:
    """Return a copy of framework state carrying the standard envelope."""

    if not isinstance(state, Mapping):
        raise AdapterContractError("framework state must be an object")
    result = dict(state)
    result[CHECKPOINT_METADATA_KEY] = envelope.to_dict()
    _assert_json(result, "framework state")
    return result


def extract_checkpoint_envelope(state: Mapping[str, Any]) -> Optional[CheckpointEnvelope]:
    """Read an embedded envelope; return ``None`` for legacy snapshots."""

    if not isinstance(state, Mapping):
        raise AdapterContractError("framework state must be an object")
    raw = state.get(CHECKPOINT_METADATA_KEY)
    if raw is None:
        return None
    if not isinstance(raw, Mapping):
        raise AdapterContractError("{} must be an object".format(CHECKPOINT_METADATA_KEY))
    return CheckpointEnvelope.from_dict(raw)


def strip_checkpoint_metadata(state: Mapping[str, Any]) -> Dict[str, Any]:
    """Return framework-owned state without Actae's reserved metadata key."""

    result = dict(state)
    result.pop(CHECKPOINT_METADATA_KEY, None)
    return result


def adapter_checkpoint_state(
    state: Mapping[str, Any],
    *,
    framework: str,
    channel_id: str,
    portable_state: Optional[Mapping[str, Any]] = None,
    native_checkpoint: Any = None,
    pending_work: Optional[Mapping[str, Any]] = None,
    manifest: Optional[Mapping[str, Any]] = None,
    event_cursor: Optional[int] = None,
    event_id: Optional[str] = None,
    framework_version: Optional[str] = None,
    adapter_version: str = "2",
) -> Dict[str, Any]:
    """Attach v1 contract metadata to an adapter's existing state shape."""

    return embed_checkpoint_metadata(
        state,
        CheckpointEnvelope(
            framework=framework,
            framework_version=framework_version,
            adapter_version=adapter_version,
            channel_id=channel_id,
            portable_state=portable_state or {},
            native_checkpoint=native_checkpoint,
            pending_work=pending_work or {},
            manifest=manifest or {},
            event_cursor=event_cursor,
            event_id=event_id,
        ),
    )


ToolCallable = Callable[[], Union[Any, Awaitable[Any]]]


class ActaeToolExecutor:
    """Framework-neutral exactly-once-effect wrapper for external tools.

    The caller supplies a stable operation key and the framework's normal
    callable.  A completed operation is replayed from Actae; an owned claim
    runs the callable and records its result.  Long operations keep their
    lease alive automatically.
    """

    def __init__(self, actae: ActaeClient, channel_id: str, actor: str = "framework-tool"):
        if not channel_id:
            raise ValueError("channel_id must not be empty")
        self.actae = actae
        self.channel_id = channel_id
        self.actor = actor

    async def execute(
        self,
        key_name: str,
        tool_name: str,
        params: Any,
        invoke: ToolCallable,
        *,
        dedup_fields: Optional[list] = None,
        lease_seconds: int = 60,
        heartbeat_interval: Optional[float] = None,
        emit_replay_event: bool = True,
        cancellation_event: Optional[asyncio.Event] = None,
    ) -> Any:
        if not key_name or not tool_name:
            raise ValueError("key_name and tool_name must not be empty")
        if lease_seconds < 1:
            raise ValueError("lease_seconds must be positive")
        interval = heartbeat_interval
        if interval is None:
            interval = max(0.25, min(float(lease_seconds) / 3.0, 30.0))
        if interval <= 0:
            raise ValueError("heartbeat_interval must be positive")

        claim = await self.actae.claim_execution(
            self.channel_id,
            key_name,
            tool_name,
            params,
            dedup_fields=dedup_fields,
            lease_seconds=lease_seconds,
            emit_replay_event=emit_replay_event,
            actor=self.actor,
        )
        if claim.status == "replayed":
            return claim.result
        if claim.status == "in_progress":
            raise ToolExecutionInProgressError(claim.execution.id, key_name)
        if claim.status not in ("claimed", "reclaimed"):
            raise AdapterContractError("unknown execution claim status: {}".format(claim.status))
        if not claim.claim_token:
            raise AdapterContractError("owned execution claim is missing claim_token")

        stop = asyncio.Event()
        heartbeat_failure = asyncio.get_running_loop().create_future()
        heartbeat = asyncio.create_task(
            self._heartbeat(
                claim.execution.id,
                claim.claim_token,
                lease_seconds,
                interval,
                stop,
                heartbeat_failure,
            )
        )
        invocation = asyncio.create_task(self._invoke(invoke))
        try:
            done, _ = await asyncio.wait(
                (invocation, heartbeat_failure),
                return_when=asyncio.FIRST_COMPLETED,
            )
            if heartbeat_failure in done:
                if cancellation_event is not None:
                    cancellation_event.set()
                invocation.cancel()
                try:
                    await invocation
                except asyncio.CancelledError:
                    pass
                raise heartbeat_failure.result()
            result = await invocation
            _assert_json(result, "tool result")
            if heartbeat_failure.done():
                raise heartbeat_failure.result()
            await self.actae.complete_execution(claim.execution.id, claim.claim_token, result)
            return result
        except asyncio.CancelledError:
            if cancellation_event is not None:
                cancellation_event.set()
            invocation.cancel()
            try:
                await invocation
            except (asyncio.CancelledError, Exception):
                pass
            await self._best_effort_cancel(claim.execution.id, claim.claim_token)
            raise
        except BaseException as exc:
            if cancellation_event is not None:
                cancellation_event.set()
            if not invocation.done():
                invocation.cancel()
            await self._best_effort_fail(claim.execution.id, claim.claim_token, exc)
            raise
        finally:
            stop.set()
            await heartbeat
            if not heartbeat_failure.done():
                heartbeat_failure.cancel()

    @staticmethod
    async def _invoke(invoke: ToolCallable) -> Any:
        result = await asyncio.to_thread(invoke)
        if inspect.isawaitable(result):
            return await result
        return result

    async def _heartbeat(
        self,
        execution_id: str,
        claim_token: str,
        lease_seconds: int,
        interval: float,
        stop: asyncio.Event,
        failure: asyncio.Future,
    ) -> None:
        while True:
            try:
                await asyncio.wait_for(stop.wait(), timeout=interval)
                return
            except asyncio.TimeoutError:
                try:
                    await self.actae.heartbeat_execution(
                        execution_id, claim_token, lease_seconds
                    )
                except Exception as exc:
                    if not failure.done():
                        failure.set_result(exc)
                    return

    async def _best_effort_cancel(self, execution_id: str, claim_token: str) -> None:
        try:
            await self.actae.cancel_execution(execution_id, claim_token)
        except Exception:
            return

    async def _best_effort_fail(
        self, execution_id: str, claim_token: str, exc: BaseException
    ) -> None:
        try:
            await self.actae.fail_execution(
                execution_id,
                str(exc) or exc.__class__.__name__,
                claim_token=claim_token,
                error_type=exc.__class__.__name__,
                stack="".join(traceback.format_exception(type(exc), exc, exc.__traceback__)),
            )
        except Exception:
            return


@dataclass(frozen=True)
class ActaeRunContext:
    """The same Actae surface passed to any supported framework adapter."""

    actae: ActaeClient
    channel_id: str
    framework: str
    run_id: str
    parent_run_id: Optional[str] = None
    actor: Optional[str] = None

    def __post_init__(self) -> None:
        for name in ("channel_id", "framework", "run_id"):
            if not getattr(self, name):
                raise ValueError("{} must not be empty".format(name))

    @property
    def tools(self) -> ActaeToolExecutor:
        return ActaeToolExecutor(
            self.actae, self.channel_id, self.actor or "{}-tool".format(self.framework)
        )

    async def record(
        self,
        kind: str,
        payload: Mapping[str, Any],
        *,
        operation_id: Optional[str] = None,
    ) -> Any:
        event = FrameworkEvent(
            kind=kind,
            framework=self.framework,
            run_id=self.run_id,
            parent_run_id=self.parent_run_id,
            payload=payload,
        )
        options = {"actor": self.actor or self.framework}
        if operation_id is not None:
            options["operation_id"] = operation_id
        return await self.actae.record(
            self.channel_id,
            "framework.{}".format(kind),
            event.to_payload(),
            **options
        )

    async def save_checkpoint(self, envelope: CheckpointEnvelope) -> int:
        if envelope.channel_id != self.channel_id or envelope.framework != self.framework:
            raise AdapterContractError("checkpoint does not belong to this run context")
        cursor = envelope.event_cursor
        if cursor is None:
            cursor = await self.actae.latest_cursor(self.channel_id) or 0
        return await self.actae.save_state(self.channel_id, cursor, envelope.to_dict())

    async def load_checkpoint(self) -> Optional[CheckpointEnvelope]:
        snapshot = await self.actae.latest_state(self.channel_id)
        if snapshot is None:
            return None
        state = snapshot.get("state")
        if not isinstance(state, Mapping):
            raise AdapterContractError("stored checkpoint must be an object")
        if state.get("schema") == CHECKPOINT_SCHEMA:
            return CheckpointEnvelope.from_dict(state)
        return extract_checkpoint_envelope(state)

    async def replay(self, *, cursor: Optional[int] = None, limit: int = 100) -> Any:
        """Replay this framework run's normalized and native events."""

        return await self.actae.replay(self.channel_id, cursor=cursor, limit=limit)

    async def fork(
        self,
        new_channel_id: str,
        *,
        at_cursor: int = 0,
        reason: Optional[str] = None,
        manifest: Optional[Dict[str, Any]] = None,
        operation_id: Optional[str] = None,
    ) -> "ActaeRunContext":
        """Fork this run and return the same framework-neutral context."""

        await self.actae.fork(
            self.channel_id,
            new_channel_id,
            at_cursor,
            display_name=new_channel_id,
            reason=reason,
            manifest=manifest,
            operation_id=operation_id,
        )
        return replace(
            self,
            channel_id=new_channel_id,
            run_id=new_channel_id,
            parent_run_id=self.run_id,
        )

    async def create_experiment(
        self, name: str, *, description: Optional[str] = None
    ) -> Dict[str, Any]:
        """Create an experiment with this run as its baseline."""

        return await self.actae.create_experiment(
            name,
            description=description,
            baseline_channel_id=self.channel_id,
        )

    async def add_to_experiment(
        self,
        group_id: str,
        *,
        role: str = "variant",
        declared_delta: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        return await self.actae.add_experiment_member(
            group_id,
            self.channel_id,
            role=role,
            declared_delta=declared_delta,
        )

    async def create_execution_group(
        self, group_id: str, *, metadata: Optional[Dict[str, Any]] = None
    ) -> Any:
        """Create durable multi-agent coordination without framework coupling."""

        return await self.actae.create_execution_group(group_id, metadata=metadata)

    async def schedule_wakeup(
        self, run_at: Any, *, payload: Optional[Dict[str, Any]] = None
    ) -> Any:
        """Schedule a durable wake-up on this run's channel."""

        return await self.actae.schedule_wakeup(
            self.channel_id, run_at, payload=payload
        )


def _mapping(value: Any, label: str) -> Mapping[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise AdapterContractError("{} must be an object".format(label))
    return value


def _assert_json(value: Any, label: str) -> None:
    try:
        json.dumps(value, allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise AdapterContractError("{} must be JSON-serializable: {}".format(label, exc)) from exc
