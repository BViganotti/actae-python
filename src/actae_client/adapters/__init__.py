"""
Actae Framework Adapters — cursor-aligned state management for agent frameworks.

Provides save/load/checkpoint adapters for LangGraph, CrewAI, LangChain,
Claude Agent SDK, OpenAI Agents SDK, Codex CLI, and a generic StateManager
for custom agents. All adapters use Actae's ``save_state()`` /
``latest_state()`` primitives under the hood.

Usage:
    from actae_client.adapters.base import StateManager
    from actae_client.adapters.langgraph import (
        ActaeCheckpointSaver,
        channel_for_config,
        ActaeLangGraphError,
        ActaeLangGraphSyncError,
    )
    from actae_client.adapters.crewai import ActaeCrewStateHook, CrewAIResumer
    from actae_client.adapters.langchain import ActaeContextSaver, ChainResumer
    from actae_client.adapters.claude import (
        ActaeClaudeSessionStore,
        ActaeClaudeHook,
        channel_for_session,
    )
    from actae_client.adapters.openai_agents import (
        ActaeTracingProcessor,
        install_actae_tracing,
        uninstall_actae_tracing,
    )
    from actae_client.adapters.codex import CodexOTLPReceiver
"""

from .base import StateManager
from .contract import (
    CHECKPOINT_METADATA_KEY,
    CHECKPOINT_SCHEMA,
    FULL_FEATURE_SET,
    ActaeFeature,
    ActaeRunContext,
    ActaeToolExecutor,
    AdapterCapabilities,
    AdapterContractError,
    AdapterSupportLevel,
    CheckpointEnvelope,
    FrameworkEvent,
    ResumeFidelity,
    ToolExecutionInProgressError,
    adapter_checkpoint_state,
    embed_checkpoint_metadata,
    extract_checkpoint_envelope,
    strip_checkpoint_metadata,
)
from .langgraph import (
    ActaeCheckpointSaver,
    ActaeLangGraphError,
    ActaeLangGraphSyncError,
    channel_for_config,
)
from .crewai import ActaeCrewStateHook, CrewAIResumer
from .langchain import ActaeContextSaver, ChainResumer
from .claude import ActaeClaudeHook, ActaeClaudeSessionStore, channel_for_session
from .openai_agents import (
    ActaeTracingProcessor,
    install_actae_tracing,
    uninstall_actae_tracing,
)
from .codex import CodexOTLPReceiver
from .otel import (
    ActaeOTelBridge,
    ActaeSpan,
    event_to_span,
    trace_context,
)
from .profiles import (
    ADAPTER_CAPABILITIES,
    CLAUDE_CAPABILITIES,
    CODEX_CAPABILITIES,
    CREWAI_CAPABILITIES,
    LANGCHAIN_CAPABILITIES,
    LANGGRAPH_CAPABILITIES,
    OPENAI_AGENTS_CAPABILITIES,
    common_surface_capabilities,
    get_adapter_capabilities,
)

__all__ = [
    "StateManager",
    "CHECKPOINT_METADATA_KEY",
    "CHECKPOINT_SCHEMA",
    "FULL_FEATURE_SET",
    "ActaeFeature",
    "ActaeRunContext",
    "ActaeToolExecutor",
    "AdapterCapabilities",
    "AdapterContractError",
    "AdapterSupportLevel",
    "CheckpointEnvelope",
    "FrameworkEvent",
    "ResumeFidelity",
    "ToolExecutionInProgressError",
    "adapter_checkpoint_state",
    "embed_checkpoint_metadata",
    "extract_checkpoint_envelope",
    "strip_checkpoint_metadata",
    "ActaeCheckpointSaver",
    "channel_for_config",
    "ActaeLangGraphError",
    "ActaeLangGraphSyncError",
    "ActaeCrewStateHook",
    "CrewAIResumer",
    "ActaeContextSaver",
    "ChainResumer",
    "ActaeClaudeSessionStore",
    "ActaeClaudeHook",
    "channel_for_session",
    "ActaeTracingProcessor",
    "install_actae_tracing",
    "uninstall_actae_tracing",
    "CodexOTLPReceiver",
    "ActaeOTelBridge",
    "ActaeSpan",
    "event_to_span",
    "trace_context",
    "ADAPTER_CAPABILITIES",
    "CLAUDE_CAPABILITIES",
    "CODEX_CAPABILITIES",
    "CREWAI_CAPABILITIES",
    "LANGCHAIN_CAPABILITIES",
    "LANGGRAPH_CAPABILITIES",
    "OPENAI_AGENTS_CAPABILITIES",
    "common_surface_capabilities",
    "get_adapter_capabilities",
]
