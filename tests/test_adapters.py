import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from actae_client.adapters.base import StateManager


def _make_mock_actae():
    actae = MagicMock()
    actae.latest_cursor = AsyncMock(return_value=42)
    actae.save_state = AsyncMock(return_value=3)
    actae.latest_state = AsyncMock(return_value={"cursor": 42, "state": {"key": "val"}})
    actae.list_states = AsyncMock(return_value=[
        {"version": 1, "cursor": 10, "timestamp": "2026-01-01T00:00:00Z"},
        {"version": 2, "cursor": 20, "timestamp": "2026-01-02T00:00:00Z"},
    ])
    actae.get_state = AsyncMock(return_value={"cursor": 42, "version": 1, "state": {"key": "val"}})
    actae.delete_state = AsyncMock(return_value=None)
    return actae


class TestStateManager:
    def test_save(self):
        actae = _make_mock_actae()
        mgr = StateManager(actae, "test-channel")
        version = asyncio.run(mgr.save({"key": "val"}))
        assert version == 3
        actae.latest_cursor.assert_awaited_once_with("test-channel")
        actae.save_state.assert_awaited_once_with("test-channel", 42, {"key": "val"})

    def test_save_with_no_cursor(self):
        actae = _make_mock_actae()
        actae.latest_cursor = AsyncMock(return_value=None)
        mgr = StateManager(actae, "test-channel")
        version = asyncio.run(mgr.save({"key": "val"}))
        assert version == 3
        actae.save_state.assert_awaited_once_with("test-channel", 0, {"key": "val"})

    def test_load(self):
        actae = _make_mock_actae()
        mgr = StateManager(actae, "test-channel")
        state = asyncio.run(mgr.load())
        assert state == {"key": "val"}

    def test_load_none(self):
        actae = _make_mock_actae()
        actae.latest_state = AsyncMock(return_value=None)
        mgr = StateManager(actae, "test-channel")
        state = asyncio.run(mgr.load())
        assert state is None

    def test_resume_with_state(self):
        actae = _make_mock_actae()
        mgr = StateManager(actae, "test-channel")
        state = asyncio.run(mgr.resume(default_state={"default": True}))
        assert state == {"key": "val"}

    def test_resume_without_state(self):
        actae = _make_mock_actae()
        actae.latest_state = AsyncMock(return_value=None)
        mgr = StateManager(actae, "test-channel")
        state = asyncio.run(mgr.resume(default_state={"default": True}))
        assert state == {"default": True}

    def test_resume_without_default(self):
        actae = _make_mock_actae()
        actae.latest_state = AsyncMock(return_value=None)
        mgr = StateManager(actae, "test-channel")
        state = asyncio.run(mgr.resume())
        assert state == {}

    def test_fork_returns_manager_for_new_channel(self):
        actae = _make_mock_actae()
        actae.fork = AsyncMock(return_value={"channel_id": "forked"})
        mgr = StateManager(actae, "test-channel")
        forked = asyncio.run(mgr.fork("forked"))
        assert isinstance(forked, StateManager)
        assert forked.channel == "forked"
        actae.fork.assert_awaited_once()
        # The fork uses the latest cursor on the source channel.
        args = actae.fork.await_args
        assert args[0][0] == "test-channel"
        assert args[0][1] == "forked"
        assert args[0][2] == 42

    def test_fork_inherits_state_on_load(self):
        actae = _make_mock_actae()
        # Source has state; the fork copies it so the fork manager loads it.
        actae.fork = AsyncMock(return_value={"channel_id": "forked"})
        actae.latest_state = AsyncMock(side_effect=[
            {"cursor": 42, "state": {"key": "val"}},  # source
            {"cursor": 42, "state": {"key": "val"}},  # fork (inherited)
        ])
        mgr = StateManager(actae, "test-channel")
        forked = asyncio.run(mgr.fork("forked"))
        assert asyncio.run(forked.load()) == {"key": "val"}

    def test_list_versions(self):
        actae = _make_mock_actae()
        mgr = StateManager(actae, "test-channel")
        versions = asyncio.run(mgr.list_versions())
        assert len(versions) == 2
        assert versions[0]["version"] == 1

    def test_get_version(self):
        actae = _make_mock_actae()
        mgr = StateManager(actae, "test-channel")
        state = asyncio.run(mgr.get_version(1))
        assert state["version"] == 1

    def test_get_version_none(self):
        actae = _make_mock_actae()
        actae.get_state = AsyncMock(return_value=None)
        mgr = StateManager(actae, "test-channel")
        state = asyncio.run(mgr.get_version(99))
        assert state is None

    def test_delete_version(self):
        actae = _make_mock_actae()
        mgr = StateManager(actae, "test-channel")
        asyncio.run(mgr.delete_version(1))
        actae.delete_state.assert_awaited_once_with("test-channel", 1)
