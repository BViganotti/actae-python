"""Unit tests for SDK dataclass parsers — no server required.

Verifies that every dataclass in actae_client.types correctly parses
the response shapes returned by the Actae server.
"""

import json

import pytest

from actae_client.types import (
    Event,
    HealthStatus,
    HealthComponent,
    ReadinessResult,
    MetricsSnapshot,
    AuthResult,
    UserInfo,
    ChannelMetadata,
    ForkInfo,
    _unwrap_sonic,
)


#  _unwrap_sonic 

class TestUnwrapSonic:
    def test_unwraps_sonic_number(self):
        assert _unwrap_sonic({"$sonic_rs::private::JsonNumber": "42"}) == 42

    def test_unwraps_sonic_float(self):
        assert _unwrap_sonic({"$sonic_rs::private::JsonNumber": "3.14"}) == 3.14

    def test_unwraps_sonic_negative(self):
        assert _unwrap_sonic({"$sonic_rs::private::JsonNumber": "-7"}) == -7

    def test_unwraps_nested(self):
        data = {"a": {"$sonic_rs::private::JsonNumber": "10"}, "b": [{"$sonic_rs::private::JsonNumber": "20"}]}
        result = _unwrap_sonic(data)
        assert result["a"] == 10
        assert result["b"][0] == 20

    def test_passes_plain_types(self):
        assert _unwrap_sonic("hello") == "hello"
        assert _unwrap_sonic(42) == 42
        assert _unwrap_sonic(3.14) == 3.14
        assert _unwrap_sonic(True) is True
        assert _unwrap_sonic(None) is None

    def test_handles_empty_sonic_wrapper(self):
        # A malformed wrapper must never leak the internal key: the raw
        # string is returned instead.
        assert _unwrap_sonic({"$sonic_rs::private::JsonNumber": ""}) == ""

    def test_malformed_sonic_wrapper_does_not_leak_internal_key(self):
        assert _unwrap_sonic({"$sonic_rs::private::JsonNumber": "not-a-number"}) == "not-a-number"

    def test_native_number_inside_wrapper_passes_through(self):
        assert _unwrap_sonic({"$sonic_rs::private::JsonNumber": 12}) == 12
        assert _unwrap_sonic({"$sonic_rs::private::JsonNumber": 3.5}) == 3.5


#  Event 

class TestEvent:
    def test_from_record(self):
        data = {
            "id": "evt-1",
            "channel_id": "ch-1",
            "type": "test.event",
            "payload": {"msg": "hello"},
            "actor": "agent-1",
            "cursor": 42,
            "channel_cursor": 5,
            "timestamp": "2026-07-28T00:00:00Z",
        }
        ev = Event.from_record(data)
        assert ev.id == "evt-1"
        assert ev.event_type == "test.event"
        assert ev.payload == {"msg": "hello"}
        assert ev.cursor == 42
        assert ev.channel_cursor == 5

    def test_from_replay(self):
        data = {
            "id": "evt-2",
            "channel_id": "ch-1",
            "type": "test.event",
            "payload": {"seq": 1},
            "actor": "agent-1",
            "cursor": 10,
            "channel_cursor": 3,
            "timestamp": "2026-07-28T00:00:00Z",
            "agent_id": "ag-1",
            "metadata": {"source": "test"},
        }
        ev = Event.from_replay(data)
        assert ev.agent_id == "ag-1"
        assert ev.metadata == {"source": "test"}

    def test_from_broadcast(self):
        data = {
            "id": "evt-3",
            "channel_id": "ch-1",
            "type": "broadcast",
            "payload": {"msg": "ws"},
            "actor": "agent-1",
            "cursor": 20,
            "timestamp": "2026-07-28T00:00:00Z",
            "delivery_id": "del-1",  # legacy server field — ignored
        }
        ev = Event.from_broadcast(data)
        assert ev.id == "evt-3"
        assert not hasattr(ev, "delivery_id")


#  HealthStatus 

class TestHealthStatus:
    def test_from_response(self):
        data = {
            "status": "ok",
            "timestamp": "2026-07-28T00:00:00Z",
            "instance_id": "actae-1",
            "check_duration_ms": 5,
            "components": {
                "database": {"status": "ok", "main_pool_healthy": True},
                "connections": {"status": "ok", "active_connections": 10},
            },
        }
        hs = HealthStatus.from_response(data)
        assert hs.status == "ok"
        assert hs.instance_id == "actae-1"
        assert hs.check_duration_ms == 5
        assert hs.components["database"].status == "ok"
        assert hs.components["database"].details["main_pool_healthy"] is True


#  ReadinessResult 

class TestReadinessResult:
    def test_from_response(self):
        data = {
            "status": "ready",
            "timestamp": "2026-07-28T00:00:00Z",
            "instance_id": "actae-1",
            "check_duration_ms": 3,
            "readiness_checks": {
                "database_ready": True,
                "capacity_available": True,
                "uptime_seconds": 3600,
                "connection_utilization": 45.2,
                "active_connections": 50,
                "max_connections": 1000,
            },
        }
        rr = ReadinessResult.from_response(data)
        assert rr.status == "ready"
        assert rr.database_ready is True
        assert rr.uptime_seconds == 3600
        assert rr.connection_utilization == 45.2

    def test_from_response_missing_checks(self):
        data = {"status": "ready"}
        rr = ReadinessResult.from_response(data)
        assert rr.status == "ready"
        assert rr.database_ready is False


#  MetricsSnapshot 

class TestMetricsSnapshot:
    def test_from_response(self):
        data = {
            "status": "ok",
            "timestamp": "2026-07-28T00:00:00Z",
            "uptime_seconds": 7200,
            "websocket": {
                "connections": 25,
                "topics": 10,
                "messages_sent": 1000,
                "messages_received": 500,
                "total_messages": 1500,
                "back_pressure": {
                    "total_consumers": 25,
                    "slow_consumers": 0,
                    "total_queued_messages": 0,
                },
            },
        }
        ms = MetricsSnapshot.from_response(data)
        assert ms.uptime_seconds == 7200
        assert ms.websocket_connections == 25
        assert ms.topic_count == 10
        assert ms.messages_sent == 1000
        assert ms.total_messages == 1500
        assert ms.back_pressure["slow_consumers"] == 0


#  AuthResult / UserInfo 

class TestAuthResult:
    def test_from_response(self):
        data = {
            "user": {
                "id": "u-1",
                "email": "test@test.com",
                "email_verified": False,
                "name": "Test User",
                "image": None,
            },
            "token": "jwt-token-here",
        }
        ar = AuthResult.from_response(data)
        assert ar.user.email == "test@test.com"
        assert ar.user.name == "Test User"
        assert ar.token == "jwt-token-here"

    def test_from_response_no_name(self):
        data = {
            "user": {"id": "u-2", "email": "anon@test.com", "email_verified": True, "name": None, "image": None},
            "token": "jwt-2",
        }
        ar = AuthResult.from_response(data)
        assert ar.user.name is None


class TestUserInfo:
    def test_from_dict_minimal(self):
        u = UserInfo.from_dict({"id": "u-1", "email": "a@b.com", "email_verified": False, "name": None, "image": None})
        assert u.id == "u-1"
        assert u.name is None

    def test_from_dict_with_created_at(self):
        u = UserInfo.from_dict({"id": "u-1", "email": "a@b.com", "email_verified": True, "name": "A", "image": "img", "created_at": "2026-01-01T00:00:00Z"})
        assert u.created_at == "2026-01-01T00:00:00Z"
        assert u.image == "img"


#  ChannelMetadata 

class TestChannelMetadata:
    def test_from_dict(self):
        data = {
            "channel_id": "ch-1",
            "parent_channel_id": "ch-0",
            "origin_run_id": "run-1",
            "forked_at_cursor": 42,
            "forked_at": "2026-07-28T00:00:00Z",
            "display_name": "Experiment A",
            "reason": "testing",
            "experiment_metadata": {"model": "gpt-4"},
        }
        cm = ChannelMetadata(**data)
        assert cm.channel_id == "ch-1"
        assert cm.parent_channel_id == "ch-0"
        assert cm.forked_at_cursor == 42
        assert cm.experiment_metadata["model"] == "gpt-4"


#  ForkInfo 

class TestForkInfo:
    def test_from_dict_flat(self):
        data = {
            "channel_id": "ch-1",
            "display_name": "Experiment A",
            "reason": None,
            "forked_at_cursor": 10,
            "event_count": 5,
            "latest_cursor": 15,
            "children": [],
        }
        bi = ForkInfo(**data)
        assert bi.channel_id == "ch-1"
        assert bi.event_count == 5
        assert bi.children == []

    def test_from_dict_with_children(self):
        """Children are plain dicts by default (recursion happens in client._parse_fork_info)."""
        data = {
            "channel_id": "root",
            "display_name": "Root",
            "reason": None,
            "forked_at_cursor": 0,
            "event_count": 10,
            "latest_cursor": 20,
            "children": [
                {
                    "channel_id": "fork-1",
                    "display_name": "Fork A",
                    "reason": "test",
                    "forked_at_cursor": 10,
                    "event_count": 5,
                    "latest_cursor": 15,
                    "children": [],
                }
            ],
        }
        bi = ForkInfo(**data)
        assert len(bi.children) == 1
        assert bi.children[0]["channel_id"] == "fork-1"


class TestExecutionTypes:
    _EXEC = {
        "id": "exec-1", "channel_id": "ch-1", "key_name": "pay-123",
        "tool_name": "charge_customer", "status": "running", "attempts": 2,
        "lease_until": "2026-02-01T00:00:00Z", "started_cursor": 5,
        "completed_cursor": None, "started_event_id": "evt-1",
        "completed_event_id": None, "replay_emitted": False,
        "params": {"customer": "abc"}, "result": None, "error": None,
        "created_at": "2026-02-01T00:00:00Z",
        "updated_at": "2026-02-01T00:00:00Z", "completed_at": None,
    }

    def test_execution_info_full(self):
        from actae_client import ExecutionInfo
        info = ExecutionInfo.from_response(self._EXEC)
        assert info.id == "exec-1"
        assert info.channel_id == "ch-1"
        assert info.key_name == "pay-123"
        assert info.tool_name == "charge_customer"
        assert info.status == "running"
        assert info.attempts == 2
        assert info.lease_until == "2026-02-01T00:00:00Z"
        assert info.started_cursor == 5
        assert info.started_event_id == "evt-1"
        assert info.params == {"customer": "abc"}
        assert info.replay_emitted is False
        assert info.completed_at is None

    def test_execution_info_completed(self):
        from actae_client import ExecutionInfo
        info = ExecutionInfo.from_response({
            **self._EXEC, "status": "completed", "attempts": 1,
            "result": {"ok": True}, "replay_emitted": True,
            "completed_event_id": "evt-2", "completed_cursor": 9,
            "completed_at": "2026-02-01T00:01:00Z",
        })
        assert info.status == "completed"
        assert info.result == {"ok": True}
        assert info.replay_emitted is True
        assert info.completed_event_id == "evt-2"

    def test_execution_info_missing_fields_default(self):
        from actae_client import ExecutionInfo
        info = ExecutionInfo.from_response({})
        assert info.id == ""
        assert info.attempts == 0
        assert info.status == ""
        assert info.replay_emitted is False

    def test_execution_info_unwraps_sonic_number(self):
        from actae_client import ExecutionInfo
        info = ExecutionInfo.from_response({
            **self._EXEC,
            "result": {"$sonic_rs::private::JsonNumber": "12345"},
        })
        assert info.result == 12345

    def test_claim_replayed(self):
        from actae_client import ExecutionClaim
        claim = ExecutionClaim.from_response({
            "status": "replayed", "execution": self._EXEC,
            "result": {"ok": True}, "claim_token": None,
        })
        assert claim.status == "replayed"
        assert claim.result == {"ok": True}
        assert claim.claim_token is None
        assert claim.execution.id == "exec-1"

    def test_claim_owned(self):
        from actae_client import ExecutionClaim
        claim = ExecutionClaim.from_response({
            "status": "claimed", "execution": self._EXEC,
            "result": None, "claim_token": "tok-1",
        })
        assert claim.status == "claimed"
        assert claim.claim_token == "tok-1"
        assert claim.result is None

    def test_claim_in_progress(self):
        from actae_client import ExecutionClaim
        claim = ExecutionClaim.from_response({
            "status": "in_progress", "execution": self._EXEC,
            "result": None, "claim_token": None,
        })
        assert claim.status == "in_progress"
