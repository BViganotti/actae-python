"""Generated from spec/api-parity/v1/manifest.json; do not edit manually."""
from dataclasses import dataclass
from typing import Dict, Optional

@dataclass(frozen=True)
class ManifestOperation:
    id: str
    method: str
    path: str
    action: str
    confirmation: bool
    idempotency: str
    transport: str
    scope: str
    fleet_supported: bool
    request_schema: Optional[str]
    response_schema: Optional[str]

MANIFEST_OPERATIONS = (
    ManifestOperation('actae.api.v1.auth.capabilities.get', 'GET', '/api/v1/auth/capabilities', 'read', False, 'n/a', 'http', 'events:read', True, None, 'actae.api.v1.Capabilities'),
    ManifestOperation('actae.api.v1.auth.signup', 'POST', '/api/v1/auth/signup', 'write', False, 'none', 'http', 'public', False, 'actae.api.v1.SignupRequest', 'actae.api.v1.AuthTokenResponse'),
    ManifestOperation('actae.api.v1.auth.login', 'POST', '/api/v1/auth/login', 'write', False, 'none', 'http', 'public', False, 'actae.api.v1.LoginRequest', 'actae.api.v1.AuthTokenResponse'),
    ManifestOperation('actae.api.v1.auth.api_key_login', 'POST', '/api/v1/auth/api-key-login', 'write', False, 'none', 'http', 'public', False, 'actae.api.v1.ApiKeyLoginRequest', 'actae.api.v1.AuthTokenResponse'),
    ManifestOperation('actae.api.v1.auth.config.get', 'GET', '/api/v1/auth/config', 'read', False, 'n/a', 'http', 'public', False, None, 'actae.api.v1.AuthConfig'),
    ManifestOperation('actae.api.v1.auth.me.get', 'GET', '/api/v1/auth/me', 'read', False, 'n/a', 'http', 'events:read', True, None, 'actae.api.v1.MeResponse'),
    ManifestOperation('actae.api.v1.auth.logout', 'POST', '/api/v1/auth/logout', 'write', False, 'none', 'http', 'public', False, None, 'actae.api.v1.OkResponse'),
    ManifestOperation('actae.api.v1.user.preferences.get', 'GET', '/api/v1/user/preferences', 'read', False, 'n/a', 'http', 'events:read', True, None, 'actae.api.v1.UserPreferences'),
    ManifestOperation('actae.api.v1.user.preferences.put', 'PUT', '/api/v1/user/preferences', 'write', False, 'none', 'http', 'events:write', True, 'actae.api.v1.UserPreferencesPut', 'actae.api.v1.UserPreferences'),
    ManifestOperation('actae.api.v1.instance.get', 'GET', '/api/v1/instance', 'read', False, 'n/a', 'http', 'events:read', True, None, 'actae.api.v1.InstanceInfo'),
    ManifestOperation('actae.api.v1.events.record', 'POST', '/api/v1/events/record', 'write', False, 'operation_id', 'http', 'events:write', True, 'actae.api.v1.PublishEventRequest', 'actae.api.v1.RecordResponse'),
    ManifestOperation('actae.api.v1.events.transition', 'POST', '/api/v1/events/transition', 'write', False, 'operation_id', 'http', 'events:write', True, 'actae.api.v1.TransitionRequest', 'actae.api.v1.RecordResponse'),
    ManifestOperation('actae.api.v1.events.replay', 'GET', '/api/v1/events/replay/{channel_id}', 'read', False, 'n/a', 'http', 'events:read', True, None, 'actae.api.v1.EventList'),
    ManifestOperation('actae.api.v1.events.query', 'POST', '/api/v1/events/query', 'read', False, 'n/a', 'http', 'events:read', True, 'actae.api.v1.QueryRequest', 'actae.api.v1.QueryResponse'),
    ManifestOperation('actae.api.v1.events.cursor.get', 'GET', '/api/v1/events/cursor/{channel_id}', 'read', False, 'n/a', 'http', 'events:read', True, None, 'actae.api.v1.CursorResponse'),
    ManifestOperation('actae.api.v1.events.verify_chain', 'GET', '/api/v1/events/verify/{channel_id}', 'read', False, 'n/a', 'http', 'payloads:read', True, None, 'actae.api.v1.VerifyChainResponse'),
    ManifestOperation('actae.api.v1.events.causal_graph.get', 'GET', '/api/v1/events/{event_id}/causal-graph', 'read', False, 'n/a', 'http', 'events:read', True, None, 'actae.api.v1.CausalGraphResponse'),
    ManifestOperation('actae.api.v1.channels.list', 'GET', '/api/v1/channels', 'read', False, 'n/a', 'http', 'channels:read', True, None, 'actae.api.v1.ChannelList'),
    ManifestOperation('actae.api.v1.state.save', 'POST', '/api/v1/state/{channel_id}', 'write', False, 'operation_id', 'http', 'state:write', True, 'actae.api.v1.SaveStateRequest', 'actae.api.v1.SaveStateResponse'),
    ManifestOperation('actae.api.v1.state.load', 'GET', '/api/v1/state/{channel_id}', 'read', False, 'n/a', 'http', 'state:read', True, None, 'actae.api.v1.StateResponse'),
    ManifestOperation('actae.api.v1.state.versions.list', 'GET', '/api/v1/state/{channel_id}/versions', 'read', False, 'n/a', 'http', 'state:read', True, None, 'actae.api.v1.StateVersionList'),
    ManifestOperation('actae.api.v1.state.version.get', 'GET', '/api/v1/state/{channel_id}/version/{version}', 'read', False, 'n/a', 'http', 'state:read', True, None, 'actae.api.v1.StateResponse'),
    ManifestOperation('actae.api.v1.state.version.delete', 'DELETE', '/api/v1/state/{channel_id}/version/{version}', 'delete', False, 'none', 'http', 'state:delete', True, None, 'actae.api.v1.OkResponse'),
    ManifestOperation('actae.api.v1.forks.create', 'POST', '/api/v1/channels/fork', 'write', False, 'operation_id', 'http', 'forks:create', True, 'actae.api.v1.ForkRequest', 'actae.api.v1.ForkResponse'),
    ManifestOperation('actae.api.v1.channels.diff', 'GET', '/api/v1/channels/diff', 'read', False, 'n/a', 'http', 'state:read', True, None, 'actae.api.v1.StateDiffResponse'),
    ManifestOperation('actae.api.v1.channels.compare', 'GET', '/api/v1/channels/compare', 'read', False, 'n/a', 'http', 'channels:read', True, None, 'actae.api.v1.CompareResponse'),
    ManifestOperation('actae.api.v1.channels.outcome.set', 'PATCH', '/api/v1/channels/{channel_id}/outcome', 'write', False, 'operation_id', 'http', 'channels:write', True, 'actae.api.v1.SetOutcomeRequest', 'actae.api.v1.OkResponse'),
    ManifestOperation('actae.api.v1.forks.promote', 'POST', '/api/v1/channels/{channel_id}/promote', 'write', False, 'operation_id', 'http', 'forks:promote', True, None, 'actae.api.v1.PromoteResponse'),
    ManifestOperation('actae.api.v1.channels.delete', 'DELETE', '/api/v1/channels/{channel_id}', 'delete', False, 'none', 'http', 'channels:delete', True, None, 'actae.api.v1.OkResponse'),
    ManifestOperation('actae.api.v1.channels.metadata.get', 'GET', '/api/v1/channels/{channel_id}/metadata', 'read', False, 'n/a', 'http', 'channels:read', True, None, 'actae.api.v1.ChannelMetadata'),
    ManifestOperation('actae.api.v1.channels.metadata.put', 'PUT', '/api/v1/channels/{channel_id}/metadata', 'write', False, 'none', 'http', 'channels:write', True, 'actae.api.v1.UpdateMetadataRequest', 'actae.api.v1.OkResponse'),
    ManifestOperation('actae.api.v1.channels.steps.latest', 'GET', '/api/v1/channels/{channel_id}/steps', 'read', False, 'n/a', 'http', 'channels:read', True, None, 'actae.api.v1.LatestStepResponse'),
    ManifestOperation('actae.api.v1.channels.steps.resolve', 'GET', '/api/v1/channels/{channel_id}/steps/{step_number}', 'read', False, 'n/a', 'http', 'channels:read', True, None, 'actae.api.v1.ResolveStepResponse'),
    ManifestOperation('actae.api.v1.channels.forks.list', 'GET', '/api/v1/channels/{channel_id}/forks', 'read', False, 'n/a', 'http', 'channels:read', True, None, 'actae.api.v1.ForkList'),
    ManifestOperation('actae.api.v1.channels.fork_tree.get', 'GET', '/api/v1/channels/{channel_id}/fork-tree', 'read', False, 'n/a', 'http', 'channels:read', True, None, 'actae.api.v1.ExecutionTree'),
    ManifestOperation('actae.api.v1.channels.trail.get', 'GET', '/api/v1/channels/{channel_id}/trail', 'read', False, 'n/a', 'http', 'channels:read', True, None, 'actae.api.v1.DecisionTrail'),
    ManifestOperation('actae.api.v1.channels.receipt.get', 'GET', '/api/v1/channels/{channel_id}/receipt', 'read', False, 'n/a', 'http', 'channels:read', True, None, 'actae.api.v1.ForkReceipt'),
    ManifestOperation('actae.api.v1.channels.close', 'POST', '/api/v1/channels/{channel_id}/close', 'write', False, 'operation_id', 'http', 'channels:write', True, None, 'actae.api.v1.OkResponse'),
    ManifestOperation('actae.api.v1.groups.create', 'POST', '/api/v1/groups', 'write', False, 'operation_id', 'http', 'groups:manage', True, 'actae.api.v1.CreateGroupRequest', 'actae.api.v1.GroupInfo'),
    ManifestOperation('actae.api.v1.groups.list', 'GET', '/api/v1/groups', 'read', False, 'n/a', 'http', 'groups:manage', True, None, 'actae.api.v1.GroupList'),
    ManifestOperation('actae.api.v1.groups.delete', 'DELETE', '/api/v1/groups/{group_id}', 'delete', False, 'none', 'http', 'groups:manage', True, None, 'actae.api.v1.OkResponse'),
    ManifestOperation('actae.api.v1.groups.join', 'POST', '/api/v1/groups/{group_id}/join', 'write', False, 'operation_id', 'http', 'groups:manage', True, 'actae.api.v1.JoinGroupRequest', 'actae.api.v1.JoinGroupResponse'),
    ManifestOperation('actae.api.v1.groups.claim_work', 'POST', '/api/v1/groups/{group_id}/work', 'write', False, 'operation_id', 'http', 'groups:manage', True, 'actae.api.v1.ClaimWorkRequest', 'actae.api.v1.ClaimWorkResponse'),
    ManifestOperation('actae.api.v1.groups.ack', 'POST', '/api/v1/groups/{group_id}/ack', 'write', False, 'operation_id', 'http', 'groups:manage', True, 'actae.api.v1.AckRequest', 'actae.api.v1.OkResponse'),
    ManifestOperation('actae.api.v1.groups.heartbeat', 'POST', '/api/v1/groups/{group_id}/heartbeat', 'write', False, 'operation_id', 'http', 'groups:manage', True, 'actae.api.v1.HeartbeatRequest', 'actae.api.v1.OkResponse'),
    ManifestOperation('actae.api.v1.groups.offsets.get', 'GET', '/api/v1/groups/{group_id}/offsets', 'read', False, 'n/a', 'http', 'groups:manage', True, None, 'actae.api.v1.OffsetsResponse'),
    ManifestOperation('actae.api.v1.execution_groups.create', 'POST', '/api/v1/execution-groups', 'write', False, 'operation_id', 'http', 'execution_groups:manage', True, 'actae.api.v1.CreateExecutionGroupRequest', 'actae.api.v1.ExecutionGroup'),
    ManifestOperation('actae.api.v1.execution_groups.list', 'GET', '/api/v1/execution-groups', 'read', False, 'n/a', 'http', 'execution_groups:manage', True, None, 'actae.api.v1.ExecutionGroupList'),
    ManifestOperation('actae.api.v1.execution_groups.members.add', 'POST', '/api/v1/execution-groups/{group_id}/members', 'write', False, 'operation_id', 'http', 'execution_groups:manage', True, 'actae.api.v1.AddExecutionGroupMemberRequest', 'actae.api.v1.ExecutionGroupMember'),
    ManifestOperation('actae.api.v1.execution_groups.members.list', 'GET', '/api/v1/execution-groups/{group_id}/members', 'read', False, 'n/a', 'http', 'execution_groups:manage', True, None, 'actae.api.v1.ExecutionGroupMemberList'),
    ManifestOperation('actae.api.v1.execution_groups.members.claim', 'POST', '/api/v1/execution-groups/{group_id}/members/{member_id}/claim', 'write', False, 'operation_id', 'http', 'execution_groups:manage', True, None, 'actae.api.v1.ExecutionGroupMember'),
    ManifestOperation('actae.api.v1.execution_groups.members.heartbeat', 'POST', '/api/v1/execution-groups/{group_id}/members/{member_id}/heartbeat', 'write', False, 'operation_id', 'http', 'execution_groups:manage', True, None, 'actae.api.v1.ExecutionGroupMember'),
    ManifestOperation('actae.api.v1.execution_groups.members.release', 'POST', '/api/v1/execution-groups/{group_id}/members/{member_id}/release', 'write', False, 'operation_id', 'http', 'execution_groups:manage', True, None, 'actae.api.v1.OkResponse'),
    ManifestOperation('actae.api.v1.execution_groups.messages.send', 'POST', '/api/v1/execution-groups/{group_id}/messages', 'write', False, 'operation_id', 'http', 'execution_groups:manage', True, 'actae.api.v1.SendExecutionGroupMessageRequest', 'actae.api.v1.OkResponse'),
    ManifestOperation('actae.api.v1.execution_groups.messages.list', 'GET', '/api/v1/execution-groups/{group_id}/members/{member_id}/messages', 'read', False, 'n/a', 'http', 'execution_groups:manage', True, None, 'actae.api.v1.ExecutionGroupMessageList'),
    ManifestOperation('actae.api.v1.execution_groups.messages.ack', 'POST', '/api/v1/execution-group-messages/{message_id}/ack', 'write', False, 'operation_id', 'http', 'execution_groups:manage', True, None, 'actae.api.v1.OkResponse'),
    ManifestOperation('actae.api.v1.execution_groups.forks.create', 'POST', '/api/v1/execution-group-forks', 'write', False, 'operation_id', 'http', 'forks:create', True, 'actae.api.v1.CreateExecutionGroupForkRequest', 'actae.api.v1.ExecutionGroupFork'),
    ManifestOperation('actae.api.v1.execution_groups.forks.get', 'GET', '/api/v1/execution-group-forks/{fork_group_id}', 'read', False, 'n/a', 'http', 'forks:create', True, None, 'actae.api.v1.ExecutionGroupFork'),
    ManifestOperation('actae.api.v1.execution_groups.forks.promote', 'POST', '/api/v1/execution-group-forks/{fork_group_id}/members/{member_id}/promote', 'write', False, 'operation_id', 'http', 'forks:promote', True, None, 'actae.api.v1.OkResponse'),
    ManifestOperation('actae.api.v1.experiments.create', 'POST', '/api/v1/experiments', 'write', False, 'operation_id', 'http', 'experiments:manage', True, 'actae.api.v1.CreateExperimentRequest', 'actae.api.v1.ExperimentGroup'),
    ManifestOperation('actae.api.v1.experiments.list', 'GET', '/api/v1/experiments', 'read', False, 'n/a', 'http', 'experiments:manage', True, None, 'actae.api.v1.ExperimentGroupList'),
    ManifestOperation('actae.api.v1.experiments.get', 'GET', '/api/v1/experiments/{group_id}', 'read', False, 'n/a', 'http', 'experiments:manage', True, None, 'actae.api.v1.ExperimentGroup'),
    ManifestOperation('actae.api.v1.experiments.members.add', 'POST', '/api/v1/experiments/{group_id}/members', 'write', False, 'operation_id', 'http', 'experiments:manage', True, 'actae.api.v1.AddExperimentMemberRequest', 'actae.api.v1.ExperimentGroup'),
    ManifestOperation('actae.api.v1.experiments.rank', 'GET', '/api/v1/experiments/{group_id}/rank', 'read', False, 'n/a', 'http', 'experiments:manage', True, None, 'actae.api.v1.RankResponse'),
    ManifestOperation('actae.api.v1.scheduler.wakeups.create', 'POST', '/api/v1/scheduler/wakeups', 'write', False, 'operation_id', 'http', 'scheduler:manage', True, 'actae.api.v1.CreateWakeupRequest', 'actae.api.v1.WakeupInfo'),
    ManifestOperation('actae.api.v1.scheduler.wakeups.list', 'GET', '/api/v1/scheduler/wakeups', 'read', False, 'n/a', 'http', 'scheduler:manage', True, None, 'actae.api.v1.WakeupList'),
    ManifestOperation('actae.api.v1.scheduler.wakeups.get', 'GET', '/api/v1/scheduler/wakeups/{id}', 'read', False, 'n/a', 'http', 'scheduler:manage', True, None, 'actae.api.v1.WakeupInfo'),
    ManifestOperation('actae.api.v1.scheduler.wakeups.cancel', 'DELETE', '/api/v1/scheduler/wakeups/{id}', 'delete', False, 'none', 'http', 'scheduler:manage', True, None, 'actae.api.v1.OkResponse'),
    ManifestOperation('actae.api.v1.executions.claim', 'POST', '/api/v1/executions/claim', 'write', False, 'execution_key', 'http', 'executions:manage', True, 'actae.api.v1.ClaimExecutionRequest', 'actae.api.v1.Execution'),
    ManifestOperation('actae.api.v1.executions.list', 'GET', '/api/v1/executions', 'read', False, 'n/a', 'http', 'executions:manage', True, None, 'actae.api.v1.ExecutionList'),
    ManifestOperation('actae.api.v1.executions.get', 'GET', '/api/v1/executions/{id}', 'read', False, 'n/a', 'http', 'executions:manage', True, None, 'actae.api.v1.Execution'),
    ManifestOperation('actae.api.v1.executions.delete', 'DELETE', '/api/v1/executions/{id}', 'delete', False, 'none', 'http', 'executions:manage', True, None, 'actae.api.v1.OkResponse'),
    ManifestOperation('actae.api.v1.executions.complete', 'POST', '/api/v1/executions/{id}/complete', 'write', False, 'execution_key', 'http', 'executions:manage', True, 'actae.api.v1.CompleteExecutionRequest', 'actae.api.v1.Execution'),
    ManifestOperation('actae.api.v1.executions.fail', 'POST', '/api/v1/executions/{id}/fail', 'write', False, 'execution_key', 'http', 'executions:manage', True, 'actae.api.v1.FailExecutionRequest', 'actae.api.v1.Execution'),
    ManifestOperation('actae.api.v1.executions.heartbeat', 'POST', '/api/v1/executions/{id}/heartbeat', 'write', False, 'execution_key', 'http', 'executions:manage', True, 'actae.api.v1.HeartbeatExecutionRequest', 'actae.api.v1.Execution'),
    ManifestOperation('actae.api.v1.executions.cancel', 'POST', '/api/v1/executions/{id}/cancel', 'write', False, 'execution_key', 'http', 'executions:manage', True, 'actae.api.v1.CancelExecutionRequest', 'actae.api.v1.Execution'),
)
MANIFEST_BY_ID: Dict[str, ManifestOperation] = {op.id: op for op in MANIFEST_OPERATIONS}

def manifest_operation(operation_id: str) -> ManifestOperation:
    try:
        return MANIFEST_BY_ID[operation_id]
    except KeyError as exc:
        raise ValueError(f"unknown API manifest operation: {operation_id}") from exc
