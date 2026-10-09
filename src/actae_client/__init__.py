"""Actae Python Client — real-time event store for agent workflows.

Import everything from this top-level package:

    from actae_client import ActaeClient, Event, AgentSession

Sub-packages:
    actae_client.adapters — framework adapters for LangGraph, CrewAI, LangChain
"""

from .client import ActaeClient
from .fleet import FleetClient, FleetSessionTransport, InstanceError, Page, PartialResult, UnsupportedOperationError
from .runtime import Actae, ActaeCarrier, ActaeScope, RUN_CARRIER_SCHEMA, channel_for_run
from .deterministic import deterministic_operation_key
from .errors import (
    ActaeError,
    AuthError,
    ActaeConnectionError,
    APIError,
    RateLimitError,
    SnapshotBoundaryError,
    VersionConflictError,
    CounterfactualBlockedError,
    ForkToolBlockedError,
    ConsumerError,
    IdempotencyKeyMismatchError,
    ExecutionNotOwnedError,
    ExecutionNotFoundError,
    ActaeLangGraphError,
    ActaeLangGraphSyncError,
)
from .types import (
    Event,
    HealthStatus,
    HealthComponent,
    ReadinessResult,
    MetricsSnapshot,
    AuthResult,
    UserInfo,
    ChannelMetadata,
    ForkInfo,
    ForkReceipt,
    TransitionResult,
    GroupInfo,
    GroupOffset,
    ClaimedWork,
    Wakeup,
    ExecutionInfo,
    ExecutionClaim,
    ExecutionGroup,
    ExecutionGroupMember,
    MemberLease,
    GroupMessage,
)
from .groups import GroupSession, GroupMemberSession
from .session import AgentSession, SessionError, SessionCompletedError, SessionLockedError
from .manifest import ManifestOperation, MANIFEST_OPERATIONS, manifest_operation

__all__ = [
    "Actae",
    "ActaeCarrier",
    "ActaeScope",
    "RUN_CARRIER_SCHEMA",
    "ActaeClient",
    "FleetClient",
    "FleetSessionTransport",
    "InstanceError",
    "Page",
    "PartialResult",
    "UnsupportedOperationError",
    "channel_for_run",
    "deterministic_operation_key",
    "ActaeError",
    "AuthError",
    "ActaeConnectionError",
    "APIError",
    "RateLimitError",
    "SnapshotBoundaryError",
    "VersionConflictError",
    "CounterfactualBlockedError",
    "ForkToolBlockedError",
    "ConsumerError",
    "IdempotencyKeyMismatchError",
    "ExecutionNotOwnedError",
    "ExecutionNotFoundError",
    "Event",
    "HealthStatus",
    "HealthComponent",
    "ReadinessResult",
    "MetricsSnapshot",
    "AuthResult",
    "UserInfo",
    "ChannelMetadata",
    "ForkInfo",
    "ForkReceipt",
    "TransitionResult",
    "GroupInfo",
    "GroupOffset",
    "ClaimedWork",
    "Wakeup",
    "ExecutionInfo",
    "ExecutionClaim",
    "ExecutionGroup",
    "ExecutionGroupMember",
    "MemberLease",
    "GroupMessage",
    "GroupSession",
    "GroupMemberSession",
    "AgentSession",
    "SessionError",
    "SessionCompletedError",
    "SessionLockedError",
    "ManifestOperation",
    "MANIFEST_OPERATIONS",
    "manifest_operation",
    "ActaeLangGraphError",
    "ActaeLangGraphSyncError",
]
