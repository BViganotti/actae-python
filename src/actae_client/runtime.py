"""Low-friction, orchestrator-safe Actae integration surface.

Execution remains in the caller's process.  This module only establishes an
ambient Actae run and delegates persistence/effect ownership to ActaeClient.
"""

import asyncio
import contextvars
import functools
import hashlib
import inspect
import json
import logging
import re
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterator, Mapping, Optional, Tuple, Union

from .client import ActaeClient, validate_channel_id
from .deterministic import deterministic_operation_key
from .adapters.contract import ActaeRunContext


logger = logging.getLogger("actae_client.runtime")
_CHANNEL_SAFE_RE = re.compile(r"^[A-Za-z0-9._:\-]{1,256}$")
_INVALID_CHANNEL_RE = re.compile(r"[^A-Za-z0-9._:\-]+")
_current_scope = contextvars.ContextVar("actae_current_scope", default=None)
RUN_CARRIER_SCHEMA = "actae.run-carrier/v1"


def channel_for_run(run_id: str, namespace: str = "run") -> str:
    """Map an arbitrary native run ID to a stable Actae channel ID.

    Human-readable valid IDs remain visible. Invalid/long IDs receive a
    readable slug plus a digest, preventing collisions after sanitization.
    """

    if not isinstance(run_id, str) or not run_id:
        raise ValueError("run_id must be a non-empty string")
    if not isinstance(namespace, str) or not namespace:
        raise ValueError("namespace must be a non-empty string")
    candidate = "{}:{}".format(namespace, run_id)
    if _CHANNEL_SAFE_RE.fullmatch(candidate) and len(candidate.encode("utf-8")) <= 256:
        return candidate
    safe_namespace = _INVALID_CHANNEL_RE.sub("-", namespace).strip("-._:") or "run"
    slug = _INVALID_CHANNEL_RE.sub("-", run_id).strip("-._:")[:80] or "run"
    digest = hashlib.sha256(run_id.encode("utf-8")).hexdigest()[:16]
    result = "{}:{}:{}".format(safe_namespace[:80], slug, digest)
    validate_channel_id(result)
    return result


def _json_mapping(value: Optional[Mapping[str, Any]], label: str) -> Dict[str, Any]:
    result = dict(value or {})
    try:
        json.dumps(result, allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise TypeError("{} must be JSON-serializable: {}".format(label, exc)) from exc
    return result


def _call_params(
    fn: Callable[..., Any],
    args: Tuple[Any, ...],
    kwargs: Dict[str, Any],
    *,
    allow_missing_tail: bool = False,
) -> Dict[str, Any]:
    signature = inspect.signature(fn)
    bound = (
        signature.bind_partial(*args, **kwargs)
        if allow_missing_tail
        else signature.bind(*args, **kwargs)
    )
    bound.apply_defaults()
    values = {key: value for key, value in bound.arguments.items() if key not in ("self", "cls")}
    return _json_mapping(values, "effect parameters")


def _invoke_key(key: Union[str, Callable[..., str]], args: Tuple[Any, ...], kwargs: Dict[str, Any], values: Mapping[str, Any]) -> str:
    if callable(key):
        resolved = key(*args, **kwargs)
    else:
        try:
            resolved = key.format_map(dict(values))
        except (KeyError, AttributeError, IndexError) as exc:
            raise ValueError("effect key template could not be resolved: {}".format(exc)) from exc
    if not isinstance(resolved, str) or not resolved:
        raise ValueError("effect key must resolve to a non-empty string")
    if len(resolved.encode("utf-8")) > 512:
        raise ValueError("effect key must not exceed 512 UTF-8 bytes")
    return resolved


async def _invoke(fn: Callable[..., Any], args: Tuple[Any, ...], kwargs: Dict[str, Any]) -> Any:
    if inspect.iscoroutinefunction(fn):
        return await fn(*args, **kwargs)
    result = await asyncio.to_thread(functools.partial(fn, *args, **kwargs))
    if inspect.isawaitable(result):
        return await result
    return result


@dataclass(frozen=True)
class ActaeCarrier:
    """Credential-free run identity safe to pass through task queues."""

    channel_id: str
    run_id: str
    framework: str
    workflow_id: Optional[str] = None

    def __post_init__(self) -> None:
        validate_channel_id(self.channel_id)
        for name in ("run_id", "framework"):
            if not isinstance(getattr(self, name), str) or not getattr(self, name):
                raise ValueError("{} must be a non-empty string".format(name))
        if self.workflow_id is not None and (
            not isinstance(self.workflow_id, str) or not self.workflow_id
        ):
            raise ValueError("workflow_id must be a non-empty string or None")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "schema": RUN_CARRIER_SCHEMA,
            "channel_id": self.channel_id,
            "run_id": self.run_id,
            "framework": self.framework,
            "workflow_id": self.workflow_id,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "ActaeCarrier":
        if not isinstance(value, Mapping) or value.get("schema") != RUN_CARRIER_SCHEMA:
            raise ValueError("unsupported or invalid Actae run carrier")
        unexpected = set(value) - {
            "schema", "channel_id", "run_id", "framework", "workflow_id"
        }
        if unexpected:
            raise ValueError(
                "Actae run carrier contains unsupported fields: {}".format(
                    ", ".join(sorted(str(item) for item in unexpected))
                )
            )
        return cls(
            channel_id=value.get("channel_id"),
            run_id=value.get("run_id"),
            framework=value.get("framework"),
            workflow_id=value.get("workflow_id"),
        )


@dataclass(frozen=True)
class ActaeScope:
    """Current logical run, propagated without threading SDK arguments."""

    owner: "Actae" = field(repr=False, compare=False)
    context: ActaeRunContext
    workflow_id: Optional[str] = None
    attempt: Optional[int] = None
    native_run_id: Optional[str] = None
    invocation_id: str = ""
    metadata: Mapping[str, Any] = field(default_factory=dict)

    @property
    def channel_id(self) -> str:
        return self.context.channel_id

    @property
    def run_id(self) -> str:
        return self.context.run_id

    @property
    def parent_run_id(self) -> Optional[str]:
        return self.context.parent_run_id

    @property
    def framework(self) -> str:
        return self.context.framework

    @property
    def tools(self):
        return self.context.tools

    def carrier(self) -> ActaeCarrier:
        return ActaeCarrier(
            channel_id=self.channel_id,
            run_id=self.run_id,
            framework=self.framework,
            workflow_id=self.workflow_id,
        )

    async def record(self, kind: str, payload: Mapping[str, Any], *, operation_id: Optional[str] = None) -> Any:
        return await self.context.record(kind, payload, operation_id=operation_id)

    def record_sync(self, kind: str, payload: Mapping[str, Any], *, operation_id: Optional[str] = None) -> Any:
        """Blocking record path for scopes opened with ``run_sync``."""
        from ._sync import run_sync
        return run_sync(self.record(kind, payload, operation_id=operation_id))

    async def effect(
        self,
        key: str,
        tool_name: str,
        params: Any,
        invoke: Callable[[], Any],
        **options: Any
    ) -> Any:
        return await self.tools.execute(key, tool_name, params, invoke, **options)

    def effect_sync(
        self,
        key: str,
        tool_name: str,
        params: Any,
        invoke: Callable[[], Any],
        **options: Any
    ) -> Any:
        """Blocking idempotent-effect path for synchronous frameworks."""
        from ._sync import run_sync
        return run_sync(self.effect(key, tool_name, params, invoke, **options))

    def child(self, run_id: str, **options: Any):
        """Create a nested run whose parent is this run."""

        options.setdefault("parent_run_id", self.run_id)
        options.setdefault("framework", self.framework)
        return self.owner.run(run_id, **options)

    def child_sync(self, run_id: str, **options: Any):
        """Synchronous nested run with inherited framework and lineage."""
        options.setdefault("parent_run_id", self.run_id)
        options.setdefault("framework", self.framework)
        return self.owner.run_sync(run_id, **options)


class _RunScope:
    def __init__(self, owner: "Actae", run_id: str, **options: Any) -> None:
        self.owner = owner
        self.run_id = run_id
        self.options = options
        self.scope = None
        self.token = None

    async def __aenter__(self) -> ActaeScope:
        self.scope = self.owner._make_scope(self.run_id, self.options)
        self.token = _current_scope.set(self.scope)
        try:
            await self.owner._lifecycle(self.scope, "run.started", self._payload())
        except BaseException:
            _current_scope.reset(self.token)
            self.token = None
            raise
        return self.scope

    def _payload(self) -> Dict[str, Any]:
        assert self.scope is not None
        return {
            "workflow_id": self.scope.workflow_id,
            "native_run_id": self.scope.native_run_id,
            "attempt": self.scope.attempt,
            "invocation_id": self.scope.invocation_id,
            "metadata": dict(self.scope.metadata),
        }

    async def __aexit__(self, exc_type: Any, exc: Any, _tb: Any) -> bool:
        assert self.scope is not None
        payload = self._payload()
        if exc is None:
            kind = "run.completed"
        elif exc_type is not None and issubclass(exc_type, asyncio.CancelledError):
            kind = "run.cancelled"
            payload["cancellation"] = {"type": exc_type.__name__}
        else:
            kind = "run.failed"
            payload["error"] = {
                "type": getattr(exc_type, "__name__", "Error"),
                "message": str(exc)[:4000],
            }
        try:
            await self.owner._lifecycle(self.scope, kind, payload)
        except BaseException:
            if exc is None:
                raise
            logger.exception("Actae failed to record run failure; preserving application error")
        finally:
            if self.token is not None:
                _current_scope.reset(self.token)
        return False


class _SyncRunScope:
    """Synchronous counterpart for CrewAI/LangChain and legacy services."""

    def __init__(self, owner: "Actae", run_id: str, **options: Any) -> None:
        self.owner = owner
        self.run_id = run_id
        self.options = options
        self.scope = None
        self.token = None

    def __enter__(self) -> ActaeScope:
        from ._sync import run_sync

        self.scope = self.owner._make_scope(self.run_id, self.options)
        self.token = _current_scope.set(self.scope)
        try:
            run_sync(self.owner._lifecycle(self.scope, "run.started", self.owner._scope_payload(self.scope)))
        except BaseException:
            _current_scope.reset(self.token)
            self.token = None
            raise
        return self.scope

    def __exit__(self, exc_type: Any, exc: Any, _tb: Any) -> bool:
        from ._sync import run_sync

        assert self.scope is not None
        payload = self.owner._scope_payload(self.scope)
        if exc is None:
            kind = "run.completed"
        elif exc_type is not None and issubclass(exc_type, (KeyboardInterrupt, GeneratorExit)):
            kind = "run.cancelled"
            payload["cancellation"] = {"type": exc_type.__name__}
        else:
            kind = "run.failed"
        if exc is not None and kind == "run.failed":
            payload["error"] = {
                "type": getattr(exc_type, "__name__", "Error"),
                "message": str(exc)[:4000],
            }
        try:
            run_sync(self.owner._lifecycle(self.scope, kind, payload))
        except BaseException:
            if exc is None:
                raise
            logger.exception("Actae failed to record run failure; preserving application error")
        finally:
            if self.token is not None:
                _current_scope.reset(self.token)
        return False


class _LangGraphProvider:
    def __init__(self, owner: "Actae") -> None:
        self.owner = owner

    @property
    def capabilities(self):
        from .adapters.profiles import LANGGRAPH_CAPABILITIES
        return LANGGRAPH_CAPABILITIES

    def checkpointer(self, channel: str = "langgraph", **options: Any):
        from .adapters.langgraph import ActaeCheckpointSaver
        return ActaeCheckpointSaver(self.owner.client, channel=channel, **options)

    @staticmethod
    def config(thread_id: str, checkpoint_ns: str = "", **configurable: Any) -> Dict[str, Any]:
        if not thread_id:
            raise ValueError("thread_id must be a non-empty string")
        return {"configurable": {"thread_id": thread_id, "checkpoint_ns": checkpoint_ns, **configurable}}


class _ClaudeProvider:
    def __init__(self, owner: "Actae") -> None:
        self.owner = owner

    @property
    def capabilities(self):
        from .adapters.profiles import CLAUDE_CAPABILITIES
        return CLAUDE_CAPABILITIES

    def session_store(self):
        from .adapters.claude import ActaeClaudeSessionStore
        return ActaeClaudeSessionStore(self.owner.client)

    def hook(self, channel_prefix: str = "claude-hooks"):
        from .adapters.claude import ActaeClaudeHook
        return ActaeClaudeHook(self.owner.client, channel_prefix=channel_prefix)


class _LangChainProvider:
    def __init__(self, owner: "Actae") -> None:
        self.owner = owner

    @property
    def capabilities(self):
        from .adapters.profiles import LANGCHAIN_CAPABILITIES
        return LANGCHAIN_CAPABILITIES

    def callback(self, channel: str = "langchain", save_every_n: int = 3):
        from .adapters.langchain import ActaeContextSaver
        return ActaeContextSaver(self.owner.client, channel=channel, save_every_n=save_every_n)

    def callbacks(self, existing: Optional[list] = None, **options: Any) -> list:
        """Compose with application callbacks instead of replacing them."""
        return list(existing or []) + [self.callback(**options)]

    def resumer(self, channel: str = "langchain"):
        from .adapters.langchain import ChainResumer
        return ChainResumer(self.owner.client, channel=channel)


class _CrewAIProvider:
    def __init__(self, owner: "Actae") -> None:
        self.owner = owner

    @property
    def capabilities(self):
        from .adapters.profiles import CREWAI_CAPABILITIES
        return CREWAI_CAPABILITIES

    def hook(self, channel: str = "crewai"):
        from .adapters.crewai import ActaeCrewStateHook
        return ActaeCrewStateHook(self.owner.client, channel=channel)

    def resumer(self, channel: str = "crewai"):
        from .adapters.crewai import CrewAIResumer
        return CrewAIResumer(self.owner.client, channel=channel)


class _OpenAIProvider:
    def __init__(self, owner: "Actae") -> None:
        self.owner = owner

    @property
    def capabilities(self):
        from .adapters.profiles import OPENAI_AGENTS_CAPABILITIES
        return OPENAI_AGENTS_CAPABILITIES

    def tracing(self, channel: str = "openai_agents", include_inputs_outputs: bool = True):
        from .adapters.openai_agents import install_actae_tracing
        return install_actae_tracing(self.owner.client, channel, include_inputs_outputs)

    async def run(self, agent: Any, input: Any, *, run_id: str, channel_id: Optional[str] = None, **runner_options: Any) -> Any:
        """Delegate to ``Runner.run`` and persist its native ``RunState``."""
        try:
            from agents import Runner
        except ImportError as exc:
            raise ImportError("install with `pip install 'actae-client[openai-agents]'`") from exc
        serializer = runner_options.pop("context_serializer", None)
        strict_context = runner_options.pop("strict_context", False)
        async with self.owner.run(run_id, framework="openai-agents", channel_id=channel_id) as scope:
            result = await Runner.run(agent, input, **runner_options)
            state = result.to_state().to_json(
                context_serializer=serializer,
                strict_context=strict_context,
            )
            await self._save_state(scope, state)
            return result

    async def resume(self, agent: Any, *, run_id: str, channel_id: Optional[str] = None, **runner_options: Any) -> Any:
        """Restore Actae's latest native ``RunState`` and continue it."""
        try:
            from agents import Runner, RunState
        except ImportError as exc:
            raise ImportError("install with `pip install 'actae-client[openai-agents]'`") from exc
        context_override = runner_options.pop("context_override", None)
        context_deserializer = runner_options.pop("context_deserializer", None)
        context_serializer = runner_options.pop("context_serializer", None)
        strict_context = runner_options.pop("strict_context", False)
        channel = channel_id or channel_for_run(run_id, "openai-agents")
        async with self.owner.run(run_id, framework="openai-agents", channel_id=channel) as scope:
            envelope = await scope.context.load_checkpoint()
            if envelope is None or not isinstance(envelope.native_checkpoint, Mapping):
                raise RuntimeError("no restorable OpenAI Agents RunState for {!r}".format(run_id))
            state = await RunState.from_json(
                agent,
                dict(envelope.native_checkpoint),
                context_override=context_override,
                context_deserializer=context_deserializer,
                strict_context=strict_context,
            )
            result = await Runner.run(agent, state, **runner_options)
            await self._save_state(scope, result.to_state().to_json(
                context_serializer=context_serializer,
                strict_context=strict_context,
            ))
            return result

    async def fork(
        self,
        source_run_id: str,
        new_run_id: str,
        *,
        at_cursor: int = 0,
        reason: Optional[str] = None,
    ) -> str:
        """Fork a persisted RunState and return the child channel ID."""
        source = channel_for_run(source_run_id, "openai-agents")
        target = channel_for_run(new_run_id, "openai-agents")
        await self.owner.client.fork(
            source,
            target,
            at_cursor,
            display_name=new_run_id,
            reason=reason,
            manifest={"framework": "openai-agents", "native": "RunState"},
        )
        return target

    async def _save_state(self, scope: ActaeScope, state: Mapping[str, Any]) -> None:
        from .adapters.contract import CheckpointEnvelope
        cursor = await self.owner.client.latest_cursor(scope.channel_id) or 0
        await scope.context.save_checkpoint(CheckpointEnvelope(
            framework="openai-agents",
            adapter_version="3",
            channel_id=scope.channel_id,
            event_cursor=cursor,
            portable_state={"current_turn": state.get("current_turn")},
            native_checkpoint=dict(state),
            manifest={"native": "RunState"},
        ))


class _CodexProvider:
    def __init__(self, owner: "Actae") -> None:
        self.owner = owner

    @property
    def capabilities(self):
        from .adapters.profiles import CODEX_CAPABILITIES
        return CODEX_CAPABILITIES

    def receiver(self, host: str = "127.0.0.1", port: int = 4319):
        from .adapters.codex import CodexOTLPReceiver
        return CodexOTLPReceiver(self.owner.client, host=host, port=port)


class _OrchestratorProvider:
    """Serializable workflow/activity boundaries for any orchestrator."""

    _KNOWN_DETERMINISTIC = frozenset(("temporal", "durable-functions", "azure-durable-functions"))

    def __init__(self, owner: "Actae", name: str, deterministic: Optional[bool] = None) -> None:
        if not name:
            raise ValueError("orchestrator name must not be empty")
        self.owner = owner
        self.name = name
        self.deterministic = name.lower() in self._KNOWN_DETERMINISTIC if deterministic is None else deterministic

    def carrier(self, workflow_id: str) -> ActaeCarrier:
        """Pure identity construction: safe inside deterministic replay code."""
        return ActaeCarrier(
            channel_id=channel_for_run(workflow_id, self.name),
            run_id=workflow_id,
            framework=self.name,
            workflow_id=workflow_id,
        )

    def workflow(self, workflow_id: str, *, attempt: Optional[int] = None, **options: Any):
        if self.deterministic:
            raise RuntimeError(
                "{} workflow code must not perform Actae network I/O during deterministic replay; "
                "use orchestrator.carrier(workflow_id) in the workflow and orchestrator.activity(...) "
                "inside the activity/worker boundary".format(self.name)
            )
        return self.owner.workflow(workflow_id, orchestrator=self.name, attempt=attempt, **options)

    def activity(self, activity_id: str, *, parent: Union[ActaeCarrier, Mapping[str, Any]], attempt: Optional[int] = None, **options: Any):
        carrier = parent if isinstance(parent, ActaeCarrier) else ActaeCarrier.from_dict(parent)
        options.setdefault("framework", "{}.activity".format(self.name))
        options.setdefault("parent_run_id", carrier.run_id)
        options.setdefault("workflow_id", carrier.workflow_id or carrier.run_id)
        options.setdefault("native_run_id", activity_id)
        options["attempt"] = attempt
        return self.owner.run(activity_id, **options)

    @staticmethod
    def effect_key(logical_task_id: str, effect: str, business_id: str) -> str:
        """Stable across attempts; attempt numbers intentionally never enter."""
        return deterministic_operation_key("actae.orchestrator.effect", effect, logical_task_id, business_id)


class _FrameworkProviders:
    def __init__(self, owner: "Actae") -> None:
        self.owner = owner
        self.langgraph = _LangGraphProvider(owner)
        self.claude = _ClaudeProvider(owner)
        self.langchain = _LangChainProvider(owner)
        self.crewai = _CrewAIProvider(owner)
        self.openai = _OpenAIProvider(owner)
        self.codex = _CodexProvider(owner)

    # Compatibility aliases for the first contract release.
    def claude_session_store(self):
        return self.claude.session_store()

    def claude_hook(self, channel_prefix: str = "claude-hooks"):
        return self.claude.hook(channel_prefix)

    def langchain_callback(self, channel: str = "langchain", save_every_n: int = 3):
        return self.langchain.callback(channel, save_every_n)

    def langchain_resumer(self, channel: str = "langchain"):
        return self.langchain.resumer(channel)

    def crewai_hook(self, channel: str = "crewai"):
        return self.crewai.hook(channel)

    def crewai_resumer(self, channel: str = "crewai"):
        return self.crewai.resumer(channel)

    def openai_tracing(self, channel: str = "openai_agents", include_inputs_outputs: bool = True):
        return self.openai.tracing(channel, include_inputs_outputs)

    def codex_receiver(self, host: str = "127.0.0.1", port: int = 4319):
        return self.codex.receiver(host, port)


class Actae:
    """One high-level entry point for native frameworks and orchestrators."""

    def __init__(self, client: ActaeClient, *, lifecycle_errors: str = "raise") -> None:
        if not isinstance(client, ActaeClient) and not callable(getattr(client, "record", None)):
            raise TypeError("client must be an ActaeClient-compatible object")
        if lifecycle_errors not in ("raise", "warn"):
            raise ValueError("lifecycle_errors must be 'raise' or 'warn'")
        self.client = client
        self.lifecycle_errors = lifecycle_errors
        self.frameworks = _FrameworkProviders(self)

    @classmethod
    def from_env(cls, *, lifecycle_errors: str = "raise", **client_options: Any) -> "Actae":
        return cls(ActaeClient.from_env(**client_options), lifecycle_errors=lifecycle_errors)

    @property
    def langgraph(self) -> _LangGraphProvider:
        return self.frameworks.langgraph

    @property
    def claude(self) -> _ClaudeProvider:
        return self.frameworks.claude

    @property
    def langchain(self) -> _LangChainProvider:
        return self.frameworks.langchain

    @property
    def crewai(self) -> _CrewAIProvider:
        return self.frameworks.crewai

    @property
    def openai(self) -> _OpenAIProvider:
        return self.frameworks.openai

    @property
    def codex(self) -> _CodexProvider:
        return self.frameworks.codex

    async def __aenter__(self) -> "Actae":
        return self

    async def __aexit__(self, *_args: Any) -> None:
        await self.close()

    def __enter__(self) -> "Actae":
        return self

    def __exit__(self, *_args: Any) -> None:
        self.close_sync()

    async def close(self) -> None:
        """Close WebSocket and every lazily-created HTTP connector."""
        await self.client.disconnect()

    def close_sync(self) -> None:
        self.client.disconnect_sync()

    def current(self, *, required: bool = True) -> Optional[ActaeScope]:
        scope = _current_scope.get()
        if scope is not None and scope.owner is not self:
            scope = None
        if scope is None and required:
            raise RuntimeError("no active Actae run; use 'async with actae.run(...)'")
        return scope

    @contextmanager
    def use(self, scope: ActaeScope) -> Iterator[ActaeScope]:
        if scope.owner is not self:
            raise ValueError("scope belongs to another Actae instance")
        token = _current_scope.set(scope)
        try:
            yield scope
        finally:
            _current_scope.reset(token)

    def run(self, run_id: str, **options: Any) -> _RunScope:
        return _RunScope(self, run_id, **options)

    def run_sync(self, run_id: str, **options: Any) -> _SyncRunScope:
        return _SyncRunScope(self, run_id, **options)

    def workflow(self, workflow_id: str, *, orchestrator: str = "orchestrator", attempt: Optional[int] = None, **options: Any) -> _RunScope:
        if orchestrator.lower() in _OrchestratorProvider._KNOWN_DETERMINISTIC:
            raise RuntimeError(
                "{} workflow code is replayed deterministically; use "
                "actae.orchestrator({!r}).carrier(workflow_id) in the workflow "
                "and .activity(...) at the worker boundary".format(orchestrator, orchestrator)
            )
        options.setdefault("framework", orchestrator)
        options.setdefault("workflow_id", workflow_id)
        options.setdefault("native_run_id", workflow_id)
        options["attempt"] = attempt
        return self.run(workflow_id, **options)

    def workflow_sync(self, workflow_id: str, *, orchestrator: str = "orchestrator", attempt: Optional[int] = None, **options: Any) -> _SyncRunScope:
        if orchestrator.lower() in _OrchestratorProvider._KNOWN_DETERMINISTIC:
            raise RuntimeError(
                "{} workflow code is replayed deterministically; use "
                "actae.orchestrator({!r}).carrier(workflow_id) in the workflow "
                "and .activity(...) at the worker boundary".format(orchestrator, orchestrator)
            )
        options.setdefault("framework", orchestrator)
        options.setdefault("workflow_id", workflow_id)
        options.setdefault("native_run_id", workflow_id)
        options["attempt"] = attempt
        return self.run_sync(workflow_id, **options)

    def orchestrator(self, name: str, *, deterministic: Optional[bool] = None) -> _OrchestratorProvider:
        return _OrchestratorProvider(self, name, deterministic=deterministic)

    def observe(
        self,
        run_id: Union[str, Callable[..., str]],
        *,
        framework: str = "custom",
        attempt: Union[None, int, Callable[..., Optional[int]]] = None,
        **run_options: Any
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """Wrap an application entry point in a run without changing its body."""

        def decorate(fn: Callable[..., Any]) -> Callable[..., Any]:
            def resolved(args: Tuple[Any, ...], kwargs: Dict[str, Any]) -> Tuple[str, Optional[int]]:
                rid = run_id(*args, **kwargs) if callable(run_id) else run_id
                value = attempt(*args, **kwargs) if callable(attempt) else attempt
                if not isinstance(rid, str) or not rid:
                    raise ValueError("observe run_id must resolve to a non-empty string")
                return rid, value

            if inspect.iscoroutinefunction(fn):
                @functools.wraps(fn)
                async def async_wrapped(*args: Any, **kwargs: Any) -> Any:
                    rid, attempt_value = resolved(args, kwargs)
                    async with self.run(rid, framework=framework, attempt=attempt_value, **run_options):
                        return await fn(*args, **kwargs)
                return async_wrapped

            @functools.wraps(fn)
            def sync_wrapped(*args: Any, **kwargs: Any) -> Any:
                rid, attempt_value = resolved(args, kwargs)
                with self.run_sync(rid, framework=framework, attempt=attempt_value, **run_options):
                    return fn(*args, **kwargs)
            return sync_wrapped

        return decorate

    def _make_scope(self, run_id: str, options: Mapping[str, Any]) -> ActaeScope:
        framework = options.get("framework") or "custom"
        if not isinstance(framework, str) or not framework:
            raise ValueError("framework must be a non-empty string")
        parent = options.get("parent_run_id")
        if parent is None:
            ambient = self.current(required=False)
            parent = ambient.run_id if ambient is not None else None
        channel_id = options.get("channel_id") or channel_for_run(run_id, framework)
        validate_channel_id(channel_id)
        attempt = options.get("attempt")
        if attempt is not None and (not isinstance(attempt, int) or isinstance(attempt, bool) or attempt < 0):
            raise ValueError("attempt must be a non-negative integer")
        metadata = _json_mapping(options.get("metadata"), "run metadata")
        for name in ("parent_run_id", "workflow_id", "native_run_id", "execution_id"):
            value = options.get(name)
            if value is not None and (not isinstance(value, str) or not value):
                raise ValueError("{} must be a non-empty string when provided".format(name))
        context = ActaeRunContext(
            self.client,
            channel_id,
            framework,
            run_id,
            parent_run_id=parent,
            actor=options.get("actor"),
        )
        return ActaeScope(
            self,
            context,
            workflow_id=options.get("workflow_id"),
            attempt=attempt,
            native_run_id=options.get("native_run_id"),
            invocation_id=options.get("execution_id") or (
                "attempt:{}".format(attempt) if attempt is not None else str(uuid.uuid4())
            ),
            metadata=metadata,
        )

    @staticmethod
    def _scope_payload(scope: ActaeScope) -> Dict[str, Any]:
        return {
            "workflow_id": scope.workflow_id,
            "native_run_id": scope.native_run_id,
            "attempt": scope.attempt,
            "invocation_id": scope.invocation_id,
            "metadata": dict(scope.metadata),
        }

    async def _lifecycle(self, scope: ActaeScope, kind: str, payload: Mapping[str, Any]) -> None:
        operation_id = deterministic_operation_key(
            "actae.runtime",
            kind,
            scope.channel_id,
            scope.run_id,
            scope.invocation_id,
        )
        try:
            await scope.record(kind, payload, operation_id=operation_id)
        except Exception:
            if self.lifecycle_errors == "raise":
                raise
            logger.warning("Actae lifecycle write failed (%s)", kind, exc_info=True)

    def effect(
        self,
        key: Union[str, Callable[..., str]],
        *,
        name: Optional[str] = None,
        params: Optional[Callable[..., Mapping[str, Any]]] = None,
        dedup_fields: Optional[list] = None,
        lease_seconds: int = 60,
        heartbeat_interval: Optional[float] = None,
        emit_replay_event: bool = True,
        pass_cancel_event: bool = False,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """Decorate a side-effecting callable with Actae ownership/replay."""

        def decorate(fn: Callable[..., Any]) -> Callable[..., Any]:
            tool_name = name or getattr(fn, "__name__", "tool")

            async def execute(args: Tuple[Any, ...], kwargs: Dict[str, Any]) -> Any:
                scope = self.current()
                assert scope is not None
                values = (
                    _json_mapping(params(*args, **kwargs), "effect parameters")
                    if params
                    else _call_params(fn, args, kwargs, allow_missing_tail=pass_cancel_event)
                )
                key_name = _invoke_key(key, args, kwargs, values)
                cancellation_event = asyncio.Event()
                invoke_args = args + (cancellation_event,) if pass_cancel_event else args
                return await scope.tools.execute(
                    key_name,
                    tool_name,
                    values,
                    lambda: _invoke(fn, invoke_args, kwargs),
                    dedup_fields=dedup_fields,
                    lease_seconds=lease_seconds,
                    heartbeat_interval=heartbeat_interval,
                    emit_replay_event=emit_replay_event,
                    cancellation_event=cancellation_event,
                )

            if inspect.iscoroutinefunction(fn):
                @functools.wraps(fn)
                async def async_wrapped(*args: Any, **kwargs: Any) -> Any:
                    return await execute(args, kwargs)
                return async_wrapped

            @functools.wraps(fn)
            def sync_wrapped(*args: Any, **kwargs: Any) -> Any:
                from ._sync import run_sync
                return run_sync(execute(args, kwargs))
            return sync_wrapped

        return decorate

    def tool(
        self,
        fn: Optional[Callable[..., Any]] = None,
        *,
        name: Optional[str] = None,
        capture: bool = True,
    ):
        """Record a read-only tool call without claiming effect ownership."""

        def decorate(target: Callable[..., Any]) -> Callable[..., Any]:
            tool_name = name or getattr(target, "__name__", "tool")

            async def execute(args: Tuple[Any, ...], kwargs: Dict[str, Any]) -> Any:
                scope = self.current()
                assert scope is not None
                call_id = str(uuid.uuid4())
                values = _call_params(target, args, kwargs) if capture else {}
                await scope.record("tool.started", {"tool": tool_name, "tool_call_id": call_id, "params": values})
                try:
                    result = await _invoke(target, args, kwargs)
                except BaseException as exc:
                    cancelled = isinstance(
                        exc, (asyncio.CancelledError, KeyboardInterrupt, GeneratorExit)
                    )
                    try:
                        await scope.record(
                            "tool.cancelled" if cancelled else "tool.failed",
                            {
                                "tool": tool_name,
                                "tool_call_id": call_id,
                                ("cancellation" if cancelled else "error"): {
                                    "type": exc.__class__.__name__,
                                    "message": str(exc)[:4000],
                                },
                            },
                        )
                    except BaseException:
                        logger.exception("Actae failed to record tool failure; preserving tool error")
                    raise
                payload = {"tool": tool_name, "tool_call_id": call_id}
                if capture:
                    try:
                        json.dumps(result, allow_nan=False)
                    except (TypeError, ValueError) as exc:
                        raise TypeError("tool result must be JSON-serializable: {}".format(exc)) from exc
                    payload["result"] = result
                await scope.record("tool.completed", payload)
                return result

            if inspect.iscoroutinefunction(target):
                @functools.wraps(target)
                async def async_wrapped(*args: Any, **kwargs: Any) -> Any:
                    return await execute(args, kwargs)
                return async_wrapped

            @functools.wraps(target)
            def sync_wrapped(*args: Any, **kwargs: Any) -> Any:
                from ._sync import run_sync
                return run_sync(execute(args, kwargs))
            return sync_wrapped

        return decorate(fn) if fn is not None else decorate
