"""Published support profiles for Actae's Python framework adapters.

These claims deliberately describe framework-native lifecycle integration,
not merely what a caller could reach through the underlying ``ActaeClient``.
The universal :class:`ActaeRunContext` remains available beside every
adapter for tools, experiments, groups and wakeups.
"""

from types import MappingProxyType
from typing import Mapping

from .contract import (
    FULL_FEATURE_SET,
    ActaeFeature,
    AdapterCapabilities,
    AdapterSupportLevel,
    ResumeFidelity,
)


_DURABLE = frozenset(
    {
        ActaeFeature.EVENTS,
        ActaeFeature.REPLAY,
        ActaeFeature.CHECKPOINTS,
        ActaeFeature.RESUME,
        ActaeFeature.FORKS,
        ActaeFeature.CAUSAL_LINEAGE,
    }
)
_OBSERVABLE = frozenset(
    {
        ActaeFeature.EVENTS,
        ActaeFeature.REPLAY,
        ActaeFeature.CHECKPOINTS,
        ActaeFeature.FORKS,
        ActaeFeature.CAUSAL_LINEAGE,
    }
)
_RECONSTRUCTED = frozenset(
    {
        ActaeFeature.CHECKPOINTS,
        ActaeFeature.RESUME,
        ActaeFeature.FORKS,
        ActaeFeature.CAUSAL_LINEAGE,
    }
)


LANGGRAPH_CAPABILITIES = AdapterCapabilities(
    framework="langgraph",
    adapter_version="2",
    support_level=AdapterSupportLevel.CERTIFIED,
    resume_fidelity=ResumeFidelity.CHECKPOINT_EXACT,
    features=_DURABLE,
    native_checkpoint=True,
    notes="Native checkpoint, pending-write, historical fork and resume integration.",
)
CLAUDE_CAPABILITIES = AdapterCapabilities(
    framework="claude-agent-sdk",
    adapter_version="2",
    support_level=AdapterSupportLevel.CERTIFIED,
    resume_fidelity=ResumeFidelity.SESSION_NATIVE,
    features=_DURABLE,
    native_checkpoint=True,
    notes="SDK-native SessionStore transcripts, subkeys, summaries and forks.",
)
LANGCHAIN_CAPABILITIES = AdapterCapabilities(
    framework="langchain",
    adapter_version="2",
    support_level=AdapterSupportLevel.PREVIEW,
    resume_fidelity=ResumeFidelity.CONTEXT_SEEDED,
    features=_RECONSTRUCTED,
    native_checkpoint=False,
    notes="Restores captured messages and tool outputs; LangChain runnable internals are not checkpointed.",
)
CREWAI_CAPABILITIES = AdapterCapabilities(
    framework="crewai",
    adapter_version="2",
    support_level=AdapterSupportLevel.PREVIEW,
    resume_fidelity=ResumeFidelity.RECONSTRUCTED,
    features=_RECONSTRUCTED,
    native_checkpoint=False,
    notes="Restores task completion/output state and resumes remaining tasks.",
)
OPENAI_AGENTS_CAPABILITIES = AdapterCapabilities(
    framework="openai-agents",
    adapter_version="3",
    support_level=AdapterSupportLevel.PREVIEW,
    resume_fidelity=ResumeFidelity.CHECKPOINT_EXACT,
    features=_DURABLE,
    native_checkpoint=True,
    notes="Tracing plus native serializable RunState persistence, resume and Actae channel forks.",
)
CODEX_CAPABILITIES = AdapterCapabilities(
    framework="codex",
    adapter_version="2",
    support_level=AdapterSupportLevel.OBSERVABILITY,
    resume_fidelity=ResumeFidelity.OBSERVE_ONLY,
    features=_OBSERVABLE,
    native_checkpoint=False,
    notes="OTLP mirrors Codex runs; Codex rollout storage remains the native resume authority.",
)


ADAPTER_CAPABILITIES: Mapping[str, AdapterCapabilities] = MappingProxyType(
    {
        profile.framework: profile
        for profile in (
            LANGGRAPH_CAPABILITIES,
            CLAUDE_CAPABILITIES,
            LANGCHAIN_CAPABILITIES,
            CREWAI_CAPABILITIES,
            OPENAI_AGENTS_CAPABILITIES,
            CODEX_CAPABILITIES,
        )
    }
)


def get_adapter_capabilities(framework: str) -> AdapterCapabilities:
    """Return the declared adapter profile or raise a useful ``KeyError``."""

    try:
        return ADAPTER_CAPABILITIES[framework]
    except KeyError as exc:
        raise KeyError(
            "unknown framework {!r}; available: {}".format(
                framework, ", ".join(sorted(ADAPTER_CAPABILITIES))
            )
        ) from exc


def common_surface_capabilities(framework: str) -> AdapterCapabilities:
    """Capabilities available when the adapter is paired with RunContext.

    This does not upgrade resume fidelity.  It records that every framework
    can use the complete Actae service surface without framework-specific
    imports or dependencies.
    """

    native = get_adapter_capabilities(framework)
    return AdapterCapabilities(
        framework=native.framework,
        adapter_version=native.adapter_version,
        support_level=native.support_level,
        resume_fidelity=native.resume_fidelity,
        features=FULL_FEATURE_SET,
        native_checkpoint=native.native_checkpoint,
        notes=native.notes + " Full Actae service surface is available through ActaeRunContext.",
    )
