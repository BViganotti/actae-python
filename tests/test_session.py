import asyncio
from unittest.mock import AsyncMock, MagicMock, patch, call

import pytest

from actae_client import ActaeClient, Event, ChannelMetadata
from actae_client.session import AgentSession, SessionError, SessionCompletedError


def _make_mock_actae():
    actae = MagicMock(spec=ActaeClient)
    actae.record = AsyncMock(return_value=Event(
        id="evt-1", channel_id="ch-1", event_type="session.started",
        payload={}, actor="agent_session", cursor=1, channel_cursor=1,
        timestamp="2026-01-01T00:00:00Z",
    ))
    actae.save_state = AsyncMock(return_value=1)
    actae.latest_state = AsyncMock(return_value=None)
    actae.get_cursor = AsyncMock(return_value=None)
    actae.latest_cursor = AsyncMock(return_value=1)
    actae.fork = AsyncMock(return_value=ChannelMetadata(
        channel_id="fork-1", parent_channel_id="ch-1", origin_run_id="ch-1",
        forked_at_cursor=5, forked_at="2026-01-01T00:00:00Z",
    ))
    actae.get_channel_metadata = AsyncMock(return_value=None)
    actae.update_metadata = AsyncMock()
    actae.replay = AsyncMock(return_value=[])
    # Step index not populated by default → session falls back to the legacy
    # cursors-list/replay resolution (mimics a pre-step-index server).
    actae.resolve_step = AsyncMock(return_value=None)
    return actae


class TestLifecycle:
    def test_created_status(self):
        actae = _make_mock_actae()
        session = AgentSession(actae, "test-session")
        assert session.status == "created"
        assert session.step_count == 0
        assert session.name == "test-session"
        assert session.channel_id is None

    def test_start_via_context_manager(self):
        actae = _make_mock_actae()
        session = AgentSession(actae, "test-session")

        async def run():
            async with session:
                assert session.status in ("started", "stepping")
                assert session.channel_id is not None
            assert session.status == "completed"

        asyncio.run(run())

    def test_start_records_session_started_event(self):
        actae = _make_mock_actae()
        session = AgentSession(actae, "test-session")

        async def run():
            async with session:
                pass
            actae.record.assert_any_call(
                channel_id="test-session",
                event_type="session.started",
                payload={"session_name": "test-session", "params": {}},
                actor="agent_session",
                metadata={
                    "session_name": "test-session",
                    "snapshot_interval": 1,
                    "has_state_fn": False,
                },
            )

        asyncio.run(run())

    def test_complete_records_session_completed(self):
        actae = _make_mock_actae()
        session = AgentSession(actae, "test-session")

        async def run():
            async with session:
                pass
            # After context exit, completed event should be recorded
            completed_calls = [c for c in actae.record.await_args_list if c.kwargs.get("event_type") == "session.completed"]
            assert len(completed_calls) >= 1

        asyncio.run(run())

    def test_exception_sets_crashed(self):
        actae = _make_mock_actae()
        session = AgentSession(actae, "test-session")

        async def run():
            try:
                async with session:
                    raise ValueError("simulated crash")
            except ValueError:
                pass
            assert session.status == "crashed"

        asyncio.run(run())

    def test_step_on_completed_raises(self):
        actae = _make_mock_actae()
        session = AgentSession(actae, "test-session")

        async def run():
            async with session:
                await session.step("inference", input="p", output="r")
            with pytest.raises(SessionError, match="completed"):
                await session.step("another")

        asyncio.run(run())


class TestStep:
    def test_step_records_event(self):
        actae = _make_mock_actae()
        session = AgentSession(actae, "test-session")

        async def run():
            async with session:
                event = await session.step("inference", input="prompt", output="result")
                assert event.cursor == 1
                assert session.step_count == 1
                assert session.cursors == [1]

        asyncio.run(run())

    def test_step_with_metadata(self):
        actae = _make_mock_actae()
        session = AgentSession(actae, "test-session")

        async def run():
            async with session:
                await session.step("inference", input="p", output="r", metadata={"model": "gpt-4"})
                record_call = actae.record.await_args_list[-1]
                assert record_call.kwargs["metadata"]["model"] == "gpt-4"

        asyncio.run(run())

    def test_step_increments_count(self):
        actae = _make_mock_actae()
        session = AgentSession(actae, "test-session")

        async def run():
            async with session:
                for i in range(5):
                    await session.step("step", input=i, output=i * 2)
                assert session.step_count == 5
                assert len(session.cursors) == 5

        asyncio.run(run())

    def test_step_requires_started_status(self):
        actae = _make_mock_actae()
        session = AgentSession(actae, "test-session")

        with pytest.raises(SessionError, match="created"):
            asyncio.run(session.step("step"))

    def test_step_with_state_fn(self):
        actae = _make_mock_actae()
        state = {"memory": "test"}
        session = AgentSession(actae, "test-session", state_fn=lambda: state)

        async def run():
            async with session:
                await session.step("inference", input="p", output="r")
            actae.save_state.assert_awaited()

        asyncio.run(run())

    def test_step_with_async_state_fn(self):
        """state_fn may be an async def — the coroutine must be awaited and
        its result saved, not serialized as a coroutine object."""
        actae = _make_mock_actae()

        async def async_state_fn():
            return {"memory": "async-test"}

        session = AgentSession(actae, "test-session", state_fn=async_state_fn)

        async def run():
            async with session:
                await session.step("inference", input="p", output="r")
            actae.save_state.assert_awaited()
            saved_state = actae.save_state.await_args[0][2]
            assert saved_state == {"memory": "async-test"}

        asyncio.run(run())

    def test_step_state_fn_exception_does_not_crash(self):
        actae = _make_mock_actae()

        def failing_state_fn():
            raise ValueError("state error")

        session = AgentSession(actae, "test-session", state_fn=failing_state_fn)

        async def run():
            async with session:
                await session.step("inference", input="p", output="r")
            # Step should have succeeded despite state_fn failure
            assert session.step_count == 1

        asyncio.run(run())

    def test_step_snapshot_interval(self):
        actae = _make_mock_actae()
        state = {"mem": "test"}
        session = AgentSession(actae, "test-session", state_fn=lambda: state, snapshot_interval=3)

        async def run():
            async with session:
                for i in range(3):
                    await session.step("step", input=i, output=i)
            # save_state should have been called at step 3
            assert actae.save_state.await_count >= 1

        asyncio.run(run())


class TestStepContext:
    def test_step_context_stored_in_payload(self):
        actae = _make_mock_actae()
        session = AgentSession(actae, "test-session")
        ctx = {"messages": [{"role": "assistant", "content": "Hello"}], "thinking": "I should respond"}

        async def run():
            async with session:
                await session.step("inference", input="Hi", output="Hello back", context=ctx)
            step_calls = [c for c in actae.record.await_args_list
                          if c.kwargs.get("event_type") == "inference"]
            assert len(step_calls) == 1
            payload = step_calls[0].kwargs["payload"]
            assert payload["context"] == ctx
            assert payload["input"] == "Hi"
            assert payload["output"] == "Hello back"
            assert payload["step_number"] == 1

        asyncio.run(run())

    def test_step_context_does_not_trigger_save_state(self):
        actae = _make_mock_actae()
        session = AgentSession(actae, "test-session")
        ctx = {"messages": [{"role": "assistant", "content": "Test"}]}

        async def run():
            async with session:
                await session.step("inference", input="x", output="y", context=ctx)
            # save_state should NOT be called — context is delta in payload only
            assert actae.save_state.await_count == 0

        asyncio.run(run())

    def test_step_context_no_key_when_not_provided(self):
        actae = _make_mock_actae()
        session = AgentSession(actae, "test-session")

        async def run():
            async with session:
                await session.step("inference", input="a", output="b")
            step_calls = [c for c in actae.record.await_args_list
                          if c.kwargs.get("event_type") == "inference"]
            assert len(step_calls) == 1
            payload = step_calls[0].kwargs["payload"]
            assert "context" not in payload

        asyncio.run(run())

    def test_step_context_with_state_fn(self):
        actae = _make_mock_actae()
        state = {"full_memory": "accumulated"}
        session = AgentSession(actae, "test-session", state_fn=lambda: state)
        ctx = {"messages": [{"role": "assistant", "content": "Delta message"}]}

        async def run():
            async with session:
                await session.step("inference", input="q", output="a", context=ctx)
            # Both context in payload and state snapshot via state_fn
            step_calls = [c for c in actae.record.await_args_list
                          if c.kwargs.get("event_type") == "inference"]
            assert len(step_calls) == 1
            assert step_calls[0].kwargs["payload"]["context"] == ctx
            actae.save_state.assert_awaited()

        asyncio.run(run())

    def test_multiple_steps_context_deltas_independent(self):
        actae = _make_mock_actae()
        session = AgentSession(actae, "test-session")

        ctx1 = {"messages": [{"role": "assistant", "content": "First response"}]}
        ctx2 = {"messages": [{"role": "assistant", "content": "Second response"}]}

        async def run():
            async with session:
                await session.step("inference", input="q1", output="a1", context=ctx1)
                await session.step("inference", input="q2", output="a2", context=ctx2)

            calls = actae.record.await_args_list
            # Filter to step events (exclude session.started and session.completed)
            step_calls = [c for c in calls if c.kwargs.get("event_type") == "inference"]
            assert len(step_calls) == 2
            assert step_calls[0].kwargs["payload"]["context"] == ctx1
            assert step_calls[1].kwargs["payload"]["context"] == ctx2
            # Each context delta is independent — not accumulated
            assert step_calls[0].kwargs["payload"]["context"] != step_calls[1].kwargs["payload"]["context"]

        asyncio.run(run())

    def test_step_retry_on_transient_error(self):
        from actae_client.errors import ActaeConnectionError as ActaeActaeConnectionError

        actae = _make_mock_actae()
        first_step = True
        step_operation_ids = set()

        async def record_side(*args, **kwargs):
            event_type = kwargs.get("event_type", "")
            if event_type == "session.started":
                return Event(
                    id="evt-start", channel_id="ch-1", event_type="session.started",
                    payload={}, actor="agent_session", cursor=1, channel_cursor=1,
                    timestamp="2026-01-01T00:00:00Z",
                )
            nonlocal first_step
            # Only step events carry an operation_id — session.completed
            # (from __aexit__) intentionally does not.
            if event_type == "step":
                step_operation_ids.add(kwargs.get("operation_id"))
            if first_step:
                first_step = False
                raise ActaeActaeConnectionError("Connection dropped")
            return Event(
                id="evt-step", channel_id="ch-1", event_type="step",
                payload={}, actor="agent_session", cursor=5, channel_cursor=2,
                timestamp="2026-01-01T00:00:00Z",
            )

        actae.record = AsyncMock(side_effect=record_side)
        session = AgentSession(actae, "test-session")

        async def run():
            async with session:
                event = await session.step("step", input="p", output="r")
                assert event.cursor == 5
                assert session.step_count == 1

        asyncio.run(run())

        # The retry must reuse the SAME operation_id so the server can
        # replay the original event instead of duplicating it.
        assert len(step_operation_ids) == 1
        assert step_operation_ids.pop()


class TestCursors:
    def test_cursors_property(self):
        actae = _make_mock_actae()
        session = AgentSession(actae, "test-session")

        async def run():
            async with session:
                await session.step("s1", input=1, output=1)
                await session.step("s2", input=2, output=2)
            assert session.cursors == [1, 1]  # Mock always returns cursor=1

        asyncio.run(run())

    def test_cursors_returned_copy(self):
        actae = _make_mock_actae()
        session = AgentSession(actae, "test-session")

        async def run():
            async with session:
                await session.step("s1", input=1, output=1)
                await session.step("s2", input=2, output=2)
            # cursors returns a copy; mutating it shouldn't affect internal state
            cursors_copy = session.cursors
            cursors_copy.append(99)
            assert session.cursors == [1, 1]  # Unchanged (mock returns cursor=1)

        asyncio.run(run())

    def test_cursors_empty_before_steps(self):
        actae = _make_mock_actae()
        session = AgentSession(actae, "test-session")
        assert session.cursors == []


class TestFork:
    def test_fork_creates_new_session(self):
        actae = _make_mock_actae()
        session = AgentSession(actae, "test-session")

        async def run():
            async with session:
                await session.step("s1", input=1, output=1)
                fork_session = await session.fork(at_step=1, name="fork-1")
                assert isinstance(fork_session, AgentSession)
                assert fork_session.name == "fork-1"
                assert fork_session.channel_id == "fork-1"

        asyncio.run(run())

    def test_fork_continues_at_step_and_inherits_state(self):
        # The whole point of forking: fork at step N, continue at step N+1
        # with the parent's state — without re-running steps 1..N.
        actae = _make_mock_actae()
        actae.latest_state = AsyncMock(return_value={
            "cursor": 5, "state": {"step": 5, "accumulator": "steps 1-5"},
        })
        session = AgentSession(actae, "parent")

        async def run():
            async with session:
                for i in range(1, 6):
                    await session.step(f"s{i}", input=i, output=i)
                fork_session = await session.fork(at_step=5, name="fork-5")
                # The fork continues at step 6, not step 1.
                assert fork_session.step_count == 5
                # It inherited the parent's state at the fork boundary.
                assert fork_session.inherited_state == {
                    "step": 5, "accumulator": "steps 1-5",
                }
                # The next recorded step on the fork is step 6.
                async with fork_session:
                    ev = await fork_session.step("s6", input=6, output=6)
                    # step() increments after recording, so step_count is now 6.
                    assert fork_session.step_count == 6

        asyncio.run(run())

    def test_fork_exposes_inherited_state_not_state_fn_override(self):
        # The fork's state_fn must reflect the caller's LIVE state (so later
        # snapshots capture the fork's progress), and inherited_state is the
        # seed the caller folds in. The fork must NOT permanently return the
        # inherited snapshot from state_fn (that would freeze later saves).
        actae = _make_mock_actae()
        actae.latest_state = AsyncMock(return_value={
            "cursor": 3, "state": {"model": "gpt-4", "data": "inherited"},
        })
        session = AgentSession(actae, "parent", state_fn=lambda: {"live": True})

        async def run():
            async with session:
                for i in range(1, 4):
                    await session.step(f"s{i}", input=i, output=i)
                fork_session = await session.fork(at_step=3, name="fork-3")
                # inherited_state carries steps 1-3's data.
                assert fork_session.inherited_state == {
                    "model": "gpt-4", "data": "inherited",
                }
                # The fork's own state_fn returns the caller's live dict, not
                # a frozen inherited snapshot.
                assert fork_session._state_fn() == {"live": True}

        asyncio.run(run())

    def test_fork_requires_at_least_step_1(self):
        actae = _make_mock_actae()
        session = AgentSession(actae, "test-session")

        async def run():
            async with session:
                with pytest.raises(SessionError, match=">= 1"):
                    await session.fork(at_step=0, name="fork-1")

        asyncio.run(run())

    def test_fork_with_params_merge(self):
        actae = _make_mock_actae()
        session = AgentSession(actae, "test-session", params={"model": "gpt-4"})

        async def run():
            async with session:
                await session.step("s1", input=1, output=1)
                fork_session = await session.fork(at_step=1, name="fork-1", params={"model": "gpt-5"})
                assert fork_session.params == {"model": "gpt-5"}

        asyncio.run(run())

    def test_fork_with_reason(self):
        actae = _make_mock_actae()
        session = AgentSession(actae, "test-session")

        async def run():
            async with session:
                await session.step("s1", input=1, output=1)
                await session.fork(at_step=1, name="fork-1", reason="Testing new model")
                actae.fork.assert_called_once()
                call_args = actae.fork.call_args
                assert "Testing" in call_args.kwargs.get("reason", "")

        asyncio.run(run())


class TestResume:
    def test_resume_crashed_session(self):
        actae = _make_mock_actae()
        actae.get_channel_metadata = AsyncMock(return_value=ChannelMetadata(
            channel_id="test-session", parent_channel_id=None, origin_run_id="test-session",
            forked_at_cursor=0, forked_at="2026-01-01T00:00:00Z",
            experiment_metadata={
                "status": "crashed", "step_count": 3, "cursors": [1, 2, 3],
                "params": {}, "session_name": "test-session",
            },
        ))

        async def run():
            session = await AgentSession.resume(actae, "test-session")
            assert session.status == "started"
            assert session.step_count == 3
            assert session.cursors == [1, 2, 3]
            assert session.name == "test-session"

        asyncio.run(run())

    def test_resume_completed_raises(self):
        actae = _make_mock_actae()
        actae.get_channel_metadata = AsyncMock(return_value=ChannelMetadata(
            channel_id="test-session", parent_channel_id=None, origin_run_id="test-session",
            forked_at_cursor=0, forked_at="2026-01-01T00:00:00Z",
            experiment_metadata={
                "status": "completed", "step_count": 5, "cursors": [], "params": {},
            },
        ))

        async def run():
            with pytest.raises(SessionCompletedError, match="already completed"):
                await AgentSession.resume(actae, "test-session")

        asyncio.run(run())

    def test_resume_not_found(self):
        actae = _make_mock_actae()
        actae.get_channel_metadata = AsyncMock(return_value=None)

        async def run():
            with pytest.raises(SessionError, match="not found"):
                await AgentSession.resume(actae, "nonexistent")

        asyncio.run(run())

    def test_resume_fork_at_step(self):
        actae = _make_mock_actae()

        actae_fork = MagicMock(spec=ActaeClient)
        actae_fork.record = AsyncMock(return_value=Event(
            id="evt-1", channel_id="ch-1", event_type="t",
            payload={}, actor="agent_session", cursor=1, channel_cursor=1,
            timestamp="2026-01-01T00:00:00Z",
        ))
        actae_fork.get_channel_metadata = AsyncMock(return_value=ChannelMetadata(
            channel_id="source", parent_channel_id=None, origin_run_id="source",
            forked_at_cursor=0, forked_at="2026-01-01T00:00:00Z",
            experiment_metadata={
                "cursors": [10, 20, 30],
                "session_name": "source", "params": {}, "status": "completed",
            },
        ))
        actae_fork.fork = AsyncMock(return_value=ChannelMetadata(
            channel_id="fork-1", parent_channel_id="source", origin_run_id="source",
            forked_at_cursor=20, forked_at="2026-01-01T00:00:00Z",
        ))
        actae_fork.latest_state = AsyncMock(return_value={
            "cursor": 20, "state": {"step": 2, "data": "inherited"},
        })

        async def run():
            session = await AgentSession.resume(actae_fork, "source", fork_at_step=2, name="fork-1")
            assert session.name == "fork-1"
            actae_fork.fork.assert_called_once()
            # The fork session continues at step 3 (not step 1).
            assert session.step_count == 2, "fork session must continue at step 3"
            assert session.inherited_state == {"step": 2, "data": "inherited"}

        asyncio.run(run())

    def test_resume_fork_at_step_requires_name(self):
        actae = _make_mock_actae()
        actae.get_channel_metadata = AsyncMock(return_value=ChannelMetadata(
            channel_id="source", parent_channel_id=None, origin_run_id="source",
            forked_at_cursor=0, forked_at="2026-01-01T00:00:00Z",
            experiment_metadata={"status": "completed", "cursors": []},
        ))

        async def run():
            with pytest.raises(SessionError, match="name"):
                await AgentSession.resume(actae, "source", fork_at_step=2)

        asyncio.run(run())


class TestCursorForStep:
    def test_cursor_for_step_validation(self):
        actae = _make_mock_actae()
        session = AgentSession(actae, "test-session")

        with pytest.raises(SessionError, match="int"):
            session._cursor_for_step(1.5)

    def test_cursor_for_step_negative(self):
        actae = _make_mock_actae()
        session = AgentSession(actae, "test-session")

        with pytest.raises(SessionError, match=">= 1"):
            session._cursor_for_step(-1)

    def test_cursor_for_step_no_cursors(self):
        actae = _make_mock_actae()
        session = AgentSession(actae, "test-session")

        with pytest.raises(SessionError, match="no recorded steps"):
            session._cursor_for_step(1)

    def test_cursor_for_step_exceeds(self):
        actae = _make_mock_actae()
        session = AgentSession(actae, "test-session")
        session._cursors = [10, 20]

        with pytest.raises(SessionError, match="exceeds"):
            session._cursor_for_step(3)


class TestEnsureChannel:
    def test_ensure_channel_raises_if_not_started(self):
        actae = _make_mock_actae()
        session = AgentSession(actae, "test-session")

        with pytest.raises(SessionError, match="no channel"):
            session._ensure_channel()

    def test_ensure_channel_returns_channel_id(self):
        actae = _make_mock_actae()
        session = AgentSession(actae, "test-session")
        session._channel_id = "ch-1"
        assert session._ensure_channel() == "ch-1"


class TestParams:
    def test_params_default_empty(self):
        actae = _make_mock_actae()
        session = AgentSession(actae, "test-session")
        assert session.params == {}

    def test_params_custom(self):
        actae = _make_mock_actae()
        session = AgentSession(actae, "test-session", params={"model": "gpt-4", "temperature": 0.7})
        assert session.params["model"] == "gpt-4"

    def test_params_returned_copy(self):
        actae = _make_mock_actae()
        session = AgentSession(actae, "test-session", params={"key": "val"})
        # params returns a copy; mutating it shouldn't affect internal state
        params_copy = session.params
        params_copy["key"] = "new-val"
        assert session.params["key"] == "val"  # Original unchanged


class TestRepr:
    def test_repr(self):
        actae = _make_mock_actae()
        session = AgentSession(actae, "test-session")
        r = repr(session)
        assert "test-session" in r
        assert "created" in r


class TestIsRetryable:
    def test_connection_error_is_retryable(self):
        from actae_client.session import _is_retryable
        from actae_client.errors import ActaeConnectionError

        assert _is_retryable(ActaeConnectionError("drop")) is True

    def test_api_error_500_is_retryable(self):
        from actae_client.session import _is_retryable
        from actae_client.errors import APIError

        assert _is_retryable(APIError(500)) is True

    def test_api_error_503_is_retryable(self):
        from actae_client.session import _is_retryable
        from actae_client.errors import APIError

        assert _is_retryable(APIError(503)) is True

    def test_api_error_429_is_retryable(self):
        from actae_client.session import _is_retryable
        from actae_client.errors import APIError

        assert _is_retryable(APIError(429)) is True

    def test_rate_limit_error_is_retryable(self):
        from actae_client.session import _is_retryable
        from actae_client.errors import RateLimitError

        # The client raises RateLimitError (a plain ActaeError, not APIError)
        # for HTTP 429 — it must still be retried with backoff.
        assert _is_retryable(RateLimitError(retry_after_seconds=30)) is True

    def test_api_error_400_is_not_retryable(self):
        from actae_client.session import _is_retryable
        from actae_client.errors import APIError

        assert _is_retryable(APIError(400)) is False

    def test_api_error_403_is_not_retryable(self):
        from actae_client.session import _is_retryable
        from actae_client.errors import APIError

        assert _is_retryable(APIError(403)) is False

    def test_timeout_error_is_retryable(self):
        from actae_client.session import _is_retryable

        assert _is_retryable(asyncio.TimeoutError()) is True

    def test_value_error_is_not_retryable(self):
        from actae_client.session import _is_retryable

        assert _is_retryable(ValueError("bad")) is False

    def test_type_error_is_not_retryable(self):
        from actae_client.session import _is_retryable

        assert _is_retryable(TypeError()) is False


class TestRetry:
    def test_retry_succeeds_on_first_attempt(self):
        from actae_client.session import _retry

        async def succeed():
            return 42

        async def run():
            result = await _retry("test", succeed, "session-1")
            assert result == 42

        asyncio.run(run())

    def test_retry_fails_on_non_retryable(self):
        from actae_client.session import _retry

        async def run():
            with pytest.raises(ValueError, match="bad"):
                await _retry("test", lambda: (_ for _ in ()).throw(ValueError("bad")), "session-1")

        asyncio.run(run())

    def test_retry_gives_up_after_max_attempts(self):
        from actae_client.session import _retry, _MAX_RETRIES
        from actae_client.errors import ActaeConnectionError

        calls = 0

        async def failing():
            nonlocal calls
            calls += 1
            raise ActaeConnectionError("persistent")

        async def run():
            with pytest.raises(ActaeConnectionError):
                await _retry("test", failing, "session-1")
            assert calls == _MAX_RETRIES + 1  # N retries + 1 initial

        asyncio.run(run())

    def test_retry_does_not_exceed_max_delay(self):
        from actae_client.session import _retry, _RETRY_MAX_DELAY, _RETRY_BASE_DELAY
        from actae_client.errors import ActaeConnectionError

        import time

        calls = 0

        async def failing():
            nonlocal calls
            calls += 1
            raise ActaeConnectionError("persistent")

        async def run():
            start = time.monotonic()
            with pytest.raises(ActaeConnectionError):
                await _retry("test", failing, "session-1")
            elapsed = time.monotonic() - start
            # Each retry doubles delay, capped at _RETRY_MAX_DELAY
            max_expected = _RETRY_MAX_DELAY * 3 + 1  # generous upper bound
            assert elapsed < max_expected

        asyncio.run(run())


class TestForkFromChannel:
    def test_fork_from_channel_replay_fallback(self):
        actae = _make_mock_actae()
        # No cursors in metadata → triggers replay fallback
        actae.get_channel_metadata = AsyncMock(return_value=ChannelMetadata(
            channel_id="source", parent_channel_id=None, origin_run_id="source",
            forked_at_cursor=0, forked_at="2026-01-01T00:00:00Z",
            experiment_metadata={"status": "completed"},
        ))
        actae.replay = AsyncMock(return_value=[
            Event(id="e1", channel_id="source", event_type="t", payload={},
                  actor="a", cursor=1, channel_cursor=1, timestamp="2026-01-01T00:00:00Z"),
            Event(id="e2", channel_id="source", event_type="t", payload={},
                  actor="a", cursor=2, channel_cursor=2, timestamp="2026-01-02T00:00:00Z"),
        ])

        from actae_client.session import AgentSession
        session = AgentSession(actae, "fork-name", params={"model": "gpt-5"})
        session._channel_id = "fork-name"

        async def run():
            await session._fork_from_channel("source", at_step=2)
            actae.fork.assert_called_once()
            # at_cursor is the 3rd positional arg (source_channel, new_channel, at_cursor)
            assert actae.fork.call_args[0][2] == 2  # cursor of second event

        asyncio.run(run())

    def test_fork_from_channel_replay_fallback_paginates(self):
        actae = _make_mock_actae()
        actae.get_channel_metadata = AsyncMock(return_value=ChannelMetadata(
            channel_id="source", parent_channel_id=None, origin_run_id="source",
            forked_at_cursor=0, forked_at="2026-01-01T00:00:00Z",
            experiment_metadata={"status": "started"},
        ))

        def make(cursor: int) -> Event:
            return Event(
                id=f"e{cursor}", channel_id="source", event_type="t", payload={},
                actor="a", cursor=cursor, channel_cursor=cursor,
                timestamp="2026-01-01T00:00:00Z",
            )

        # The server caps replay per page: simulate a cap of 3.
        def replay_side(channel_id, *, cursor=None, limit=100, event_type=None):
            all_events = [make(c) for c in range(1, 6)]
            if cursor is not None:
                all_events = [e for e in all_events if e.cursor > cursor]
            return all_events[:min(limit, 3)]

        actae.replay = AsyncMock(side_effect=replay_side)

        session = AgentSession(actae, "fork-name")
        session._channel_id = "fork-name"

        async def run():
            await session._fork_from_channel("source", at_step=5)
            # Resolution must have paged (2 replay calls) and forked at the
            # 5th event's cursor.
            assert actae.replay.call_count >= 2, "expected paginated replay"
            assert actae.fork.call_args[0][2] == 5

        asyncio.run(run())

    def test_fork_from_channel_excludes_fork_marker(self):
        actae = _make_mock_actae()
        actae.get_channel_metadata = AsyncMock(return_value=ChannelMetadata(
            channel_id="source", parent_channel_id=None, origin_run_id="source",
            forked_at_cursor=0, forked_at="2026-01-01T00:00:00Z",
            experiment_metadata={"status": "started"},
        ))
        actae.replay = AsyncMock(return_value=[
            Event(id="marker", channel_id="source", event_type="fork.started", payload={},
                  actor="system", cursor=1, channel_cursor=1, timestamp="t"),
            Event(id="e1", channel_id="source", event_type="t", payload={},
                  actor="a", cursor=2, channel_cursor=2, timestamp="t"),
            Event(id="e2", channel_id="source", event_type="t", payload={},
                  actor="a", cursor=3, channel_cursor=3, timestamp="t"),
        ])

        session = AgentSession(actae, "fork-name")
        session._channel_id = "fork-name"

        async def run():
            await session._fork_from_channel("source", at_step=2)
            # step 2 maps to the SECOND user event (cursor 3), not shifted
            # by the fork.started marker.
            assert actae.fork.call_args[0][2] == 3

        asyncio.run(run())

    def test_fork_from_channel_boundary_strict_raises(self):
        # Strict boundary policy (default): a missing snapshot must NOT
        # silently fall forward to the latest state.
        actae = _make_mock_actae()
        actae.get_channel_metadata = AsyncMock(return_value=ChannelMetadata(
            channel_id="source", parent_channel_id=None, origin_run_id="source",
            forked_at_cursor=0, forked_at="2026-01-01T00:00:00Z",
            experiment_metadata={"status": "started"},
        ))
        actae.replay = AsyncMock(return_value=[
            Event(id="e1", channel_id="source", event_type="t", payload={},
                  actor="a", cursor=1, channel_cursor=1, timestamp="t"),
        ])
        from actae_client.errors import NoRestorableCheckpointError, SnapshotBoundaryError

        async def fork_side(*args, **kwargs):
            if args[2] != 0:  # at_cursor > 0 → no boundary
                raise SnapshotBoundaryError("no boundary")
            return ChannelMetadata(
                channel_id=args[1], parent_channel_id=args[0], origin_run_id=args[0],
                forked_at_cursor=0, forked_at="t",
                experiment_metadata={},
            )

        actae.fork = AsyncMock(side_effect=fork_side)

        session = AgentSession(actae, "fork-name")
        session._channel_id = "fork-name"

        async def run():
            with pytest.raises(NoRestorableCheckpointError):
                await session._fork_from_channel("source", at_step=1)
            # The fall-forward to at_cursor=0 must NEVER happen implicitly.
            assert actae.fork.call_count == 1
            assert actae.fork.call_args[0][2] == 1

        asyncio.run(run())

    def test_fork_from_channel_boundary_approximate_falls_back(self):
        # Explicit approximate mode: falls back to the latest state once and
        # reports the resolved boundary drift on the session.
        actae = _make_mock_actae()
        actae.get_channel_metadata = AsyncMock(return_value=ChannelMetadata(
            channel_id="source", parent_channel_id=None, origin_run_id="source",
            forked_at_cursor=0, forked_at="2026-01-01T00:00:00Z",
            experiment_metadata={"status": "started"},
        ))
        actae.replay = AsyncMock(return_value=[
            Event(id="e1", channel_id="source", event_type="t", payload={},
                  actor="a", cursor=1, channel_cursor=1, timestamp="t"),
        ])
        from actae_client.errors import SnapshotBoundaryError
        from actae_client.types import ForkReceipt

        calls = []

        async def fork_side(*args, **kwargs):
            calls.append(args[2])
            if args[2] != 0:  # at_cursor > 0 → no boundary
                raise SnapshotBoundaryError("no boundary")
            return ForkReceipt(
                fork_id=args[1], source_channel_id=args[0], child_channel_id=args[1],
                requested_cursor=0, resolved_cursor=5, source_state_version=2,
                restorable=True, replayed=False,
            )

        actae.fork = AsyncMock(side_effect=fork_side)

        session = AgentSession(actae, "fork-name")
        session._channel_id = "fork-name"

        async def run():
            await session._fork_from_channel(
                "source", at_step=1, boundary_mode="approximate"
            )
            # boundary miss at the step cursor falls back to at_cursor=0 —
            # but ONLY because the caller explicitly opted into approximate.
            assert calls == [1, 0]
            assert session.boundary_restorable is True
            # The requested boundary reflects the user's INTENDED boundary
            # (step 1's cursor), while resolved is the drift-causing latest.
            assert session.requested_boundary_cursor == 1
            assert session.resolved_boundary_cursor == 5

        asyncio.run(run())

    def test_fork_from_channel_boundary_lineage_only(self):
        actae = _make_mock_actae()
        actae.get_channel_metadata = AsyncMock(return_value=ChannelMetadata(
            channel_id="source", parent_channel_id=None, origin_run_id="source",
            forked_at_cursor=0, forked_at="2026-01-01T00:00:00Z",
            experiment_metadata={"status": "started"},
        ))
        actae.replay = AsyncMock(return_value=[
            Event(id="e1", channel_id="source", event_type="t", payload={},
                  actor="a", cursor=1, channel_cursor=1, timestamp="t"),
        ])
        from actae_client.errors import SnapshotBoundaryError
        from actae_client.types import ForkReceipt

        async def fork_side(*args, **kwargs):
            if args[2] != 0:
                raise SnapshotBoundaryError("no boundary")
            return ForkReceipt(
                fork_id=args[1], source_channel_id=args[0], child_channel_id=args[1],
                requested_cursor=0, resolved_cursor=0, source_state_version=0,
                restorable=False, replayed=False,
            )

        actae.fork = AsyncMock(side_effect=fork_side)

        session = AgentSession(actae, "fork-name")
        session._channel_id = "fork-name"

        async def run():
            await session._fork_from_channel("source", at_step=1, boundary_mode="lineage_only")
            assert session.boundary_restorable is False
            assert session.resolved_boundary_cursor == 0

        asyncio.run(run())

    def test_prime_fork_refuses_contaminated_state(self):
        """When an approximate fork resolves BEYOND the requested boundary, the
        inherited snapshot contains data from after the fork point. The SDK
        must NOT expose it as steps 1..N's data — inherited_state is None."""
        actae = _make_mock_actae()
        # The server copied the *latest* (post-boundary) state into the child.
        actae.latest_state = AsyncMock(return_value={
            "state": {"s1_out": "a", "s2_out": "b"},
            "version": 2,
        })

        session = AgentSession(actae, "fork-name")
        session._channel_id = "fork-name"
        # Approximate fallback: requested step 1's cursor, resolved to the
        # latest (drift — the contamination case).
        session._requested_boundary_cursor = 1
        session._resolved_boundary_cursor = 5
        session._boundary_restorable = True

        async def run():
            await session._prime_fork_session("source", at_step=1)
            assert session.inherited_state is None, (
                "contaminated inherited state must be refused"
            )

        asyncio.run(run())

    def test_prime_fork_accepts_exact_state(self):
        """A fork that resolved AT the requested boundary (no drift) inherits
        the copied snapshot normally."""
        actae = _make_mock_actae()
        actae.latest_state = AsyncMock(return_value={
            "state": {"s1_out": "a"},
            "version": 1,
        })

        session = AgentSession(actae, "fork-name")
        session._channel_id = "fork-name"
        session._requested_boundary_cursor = 1
        session._resolved_boundary_cursor = 1
        session._boundary_restorable = True

        async def run():
            await session._prime_fork_session("source", at_step=1)
            assert session.inherited_state == {"s1_out": "a"}

        asyncio.run(run())

    def test_prime_fork_latest_request_never_contaminated(self):
        """requested cursor 0 = 'latest' is self-consistent: the guard must
        not fire even when the resolved cursor is > 0."""
        actae = _make_mock_actae()
        actae.latest_state = AsyncMock(return_value={
            "state": {"s9_out": "z"},
            "version": 9,
        })

        session = AgentSession(actae, "fork-name")
        session._channel_id = "fork-name"
        # Fork at "latest": requested 0, resolved to the latest snapshot.
        session._requested_boundary_cursor = 0
        session._resolved_boundary_cursor = 9
        session._boundary_restorable = True

        async def run():
            await session._prime_fork_session("source", at_step=9)
            assert session.inherited_state == {"s9_out": "z"}, (
                "requested=0 latest fork must inherit normally"
            )

        asyncio.run(run())

    def test_fork_from_channel_uses_cursors_list(self):
        actae = _make_mock_actae()
        actae.get_channel_metadata = AsyncMock(return_value=ChannelMetadata(
            channel_id="source", parent_channel_id=None, origin_run_id="source",
            forked_at_cursor=0, forked_at="2026-01-01T00:00:00Z",
            experiment_metadata={
                "status": "completed", "cursors": [10, 20, 30], "step_count": 3,
            },
        ))

        session = AgentSession(actae, "fork-name")
        session._channel_id = "fork-name"

        async def run():
            await session._fork_from_channel("source", at_step=2)
            actae.fork.assert_called_once()
            # at_cursor is the 3rd positional arg
            assert actae.fork.call_args[0][2] == 20

        asyncio.run(run())

    def test_fork_from_channel_raises_on_non_monotonic_cursors(self):
        actae = _make_mock_actae()
        actae.get_channel_metadata = AsyncMock(return_value=ChannelMetadata(
            channel_id="source", parent_channel_id=None, origin_run_id="source",
            forked_at_cursor=0, forked_at="2026-01-01T00:00:00Z",
            experiment_metadata={"status": "completed"},
        ))
        # Non-monotonic cursors
        actae.replay = AsyncMock(return_value=[
            Event(id="e1", channel_id="source", event_type="t", payload={},
                  actor="a", cursor=5, channel_cursor=1, timestamp=""),
            Event(id="e2", channel_id="source", event_type="t", payload={},
                  actor="a", cursor=3, channel_cursor=2, timestamp=""),
        ])

        session = AgentSession(actae, "fork-name")
        session._channel_id = "fork-name"

        async def run():
            with pytest.raises(SessionError, match="non-monotonic"):
                await session._fork_from_channel("source", at_step=2)

        asyncio.run(run())

    def test_fork_from_channel_step_too_high(self):
        actae = _make_mock_actae()
        actae.get_channel_metadata = AsyncMock(return_value=ChannelMetadata(
            channel_id="source", parent_channel_id=None, origin_run_id="source",
            forked_at_cursor=0, forked_at="",
            experiment_metadata={"status": "completed"},
        ))
        actae.replay = AsyncMock(return_value=[
            Event(id="e1", channel_id="source", event_type="t", payload={},
                  actor="a", cursor=1, channel_cursor=1, timestamp=""),
        ])

        session = AgentSession(actae, "fork-name")
        session._channel_id = "fork-name"

        async def run():
            with pytest.raises(SessionError, match="exceeds"):
                await session._fork_from_channel("source", at_step=5)

        asyncio.run(run())

    def test_fork_from_channel_empty_source(self):
        actae = _make_mock_actae()
        actae.get_channel_metadata = AsyncMock(return_value=ChannelMetadata(
            channel_id="source", parent_channel_id=None, origin_run_id="source",
            forked_at_cursor=0, forked_at="",
            experiment_metadata={"status": "completed"},
        ))
        actae.replay = AsyncMock(return_value=[])

        session = AgentSession(actae, "fork-name")
        session._channel_id = "fork-name"

        async def run():
            with pytest.raises(SessionError, match="no events"):
                await session._fork_from_channel("source", at_step=1)

        asyncio.run(run())

    def test_fork_from_channel_step_less_than_one(self):
        actae = _make_mock_actae()
        session = AgentSession(actae, "fork-name")
        session._channel_id = "fork-name"

        async def run():
            with pytest.raises(SessionError, match=">= 1"):
                await session._fork_from_channel("source", at_step=0)

        asyncio.run(run())


class TestSessionMetadataUpdate:
    def test_update_metadata_called_on_start(self):
        actae = _make_mock_actae()
        session = AgentSession(actae, "test-session")

        async def run():
            async with session:
                pass
            actae.update_metadata.assert_called()

    def test_update_metadata_called_on_complete(self):
        actae = _make_mock_actae()
        session = AgentSession(actae, "test-session")

        async def run():
            async with session:
                pass
            # Find the call with "completed" in its experiment metadata
            completed_calls = [
                c for c in actae.update_metadata.await_args_list
                if "completed" in str(c.kwargs.get("experiment_metadata", {}))
            ]
            assert len(completed_calls) >= 1

        asyncio.run(run())

    def test_update_metadata_merges_existing(self):
        actae = _make_mock_actae()
        actae.get_channel_metadata = AsyncMock(return_value=ChannelMetadata(
            channel_id="test-session", parent_channel_id=None, origin_run_id="test-session",
            forked_at_cursor=0, forked_at="2026-01-01T00:00:00Z",
            experiment_metadata={"preserved_key": "preserved_value"},
        ))
        session = AgentSession(actae, "test-session")

        async def run():
            async with session:
                pass
            # Verify preserved_key was kept during merge
            for call in actae.update_metadata.await_args_list:
                meta = call.kwargs.get("experiment_metadata", {})
                if meta.get("preserved_key") == "preserved_value":
                    return
            pytest.fail("preserved_key not found in any update_metadata call")

        asyncio.run(run())

    def test_update_metadata_includes_step_count(self):
        actae = _make_mock_actae()
        session = AgentSession(actae, "test-session")

        async def run():
            async with session:
                await session.step("s1", input=1, output=1)
                await session.step("s2", input=2, output=2)
            # Check that step_count was written to metadata
            step_count_found = any(
                c.kwargs.get("experiment_metadata", {}).get("step_count") == 2
                for c in actae.update_metadata.await_args_list
            )
            assert step_count_found, "step_count=2 not found in metadata updates"

        asyncio.run(run())


class TestSnapshotInterval:
    def test_snapshot_interval_min_one(self):
        actae = _make_mock_actae()
        session = AgentSession(actae, "test-session", snapshot_interval=0)
        assert session._snapshot_interval == 1

    def test_snapshot_interval_negative(self):
        actae = _make_mock_actae()
        session = AgentSession(actae, "test-session", snapshot_interval=-5)
        assert session._snapshot_interval == 1


class TestDeterministicStepOperationID:
    """AgentSession step operation ids are deterministic UUIDv5 ids over
    (scope, type, channel, step number, canonical content): re-driving the
    SAME step (same step_number, same payload/metadata) reproduces the SAME
    id so the server replays instead of duplicating; different content
    derives a distinct id."""

    def test_same_step_redrive_reuses_operation_id(self):
        from actae_client.session import AgentSession, _step_operation_id

        actae = _make_mock_actae()
        captured = []

        async def record_side(*args, **kwargs):
            if kwargs.get("event_type") == "inference":
                captured.append((kwargs.get("operation_id"), kwargs.get("metadata")))
            return Event(
                id="evt-step", channel_id="ch-1", event_type=kwargs.get("event_type", "step"),
                payload={}, actor="agent_session", cursor=1, channel_cursor=1,
                timestamp="2026-01-01T00:00:00Z",
            )

        actae.record = AsyncMock(side_effect=record_side)

        async def run():
            # First session: step 1 with fixed content.
            s1 = AgentSession(actae, "det-session")
            async with s1:
                await s1.step("inference", input="q", output="a", metadata={"m": 1})

            # Crash-recovery re-drive: a SECOND session on the same channel
            # re-runs the same step 1 with identical content.
            s2 = AgentSession(actae, "det-session")
            async with s2:
                await s2.step("inference", input="q", output="a", metadata={"m": 1})

            # Same step number + same content -> SAME operation id.
            assert len(captured) == 2, captured
            assert captured[0][0] == captured[1][0], (
                "re-drive must reuse the operation id so the server replays"
            )

            # A different output derives a DIFFERENT id.
            s3 = AgentSession(actae, "det-session")
            async with s3:
                await s3.step("inference", input="q", output="CHANGED", metadata={"m": 1})
            assert len(captured) == 3, captured
            assert captured[2][0] != captured[0][0], (
                "different content must derive a distinct operation id"
            )

        asyncio.run(run())

    def test_derivation_matches_documented_uuid5(self):
        from actae_client.session import _step_operation_id

        # Mirror the Go SDK's DeterministicOperationKey derivation exactly:
        # canonicalStepContent = json.Marshal({"payload":..., "metadata":...})
        # (payload FIRST — Go struct field order — with recursively sorted
        # keys, ASCII escapes and HTML escaping of < > &), NUL-joined with
        # the (scope, type, channel, step number) identity, 256-byte cap.
        import hashlib
        import json as _json

        payload = {"step_number": 1, "input": "p", "output": "r"}
        metadata = {"step_number": 1}

        def _sorted_recursive(obj):
            if isinstance(obj, dict):
                return {k: _sorted_recursive(v) for k, v in sorted(obj.items())}
            if isinstance(obj, list):
                return [_sorted_recursive(v) for v in obj]
            return obj

        canonical = _json.dumps(
            {"payload": _sorted_recursive(payload), "metadata": _sorted_recursive(metadata)},
            separators=(",", ":"), ensure_ascii=True,
        ).replace("<", "\\u003c").replace(">", "\\u003e").replace("&", "\\u0026")
        name = "\x00".join(
            ["agent-session", "s", "ch-det", "1", canonical]
        ).encode("utf-8")[:256]

        import uuid as _uuid
        ns = _uuid.UUID("3f74e5f1-9b2c-4a7e-8f1d-000000000001")
        d = hashlib.sha1(ns.bytes + name).digest()
        raw = bytearray(d[:16])
        raw[6] = (raw[6] & 0x0F) | 0x50
        raw[8] = (raw[8] & 0x3F) | 0x80
        expected = str(_uuid.UUID(bytes=bytes(raw)))

        got = _step_operation_id("ch-det", "s", 1, payload, metadata)
        assert got == expected, (got, expected)

    def test_go_parity_known_vector(self):
        from actae_client.session import _step_operation_id

        # Cross-SDK parity vector: this exact id was produced by the Go SDK
        # (DeterministicOperationKey with canonicalStepContent) for the same
        # inputs — see the parity harness in sdks/go docs. If this fails the
        # canonical serialization drifted from Go's json.Marshal output.
        got = _step_operation_id(
            "ch-det", "s", 1,
            {"step_number": 1, "input": "p", "output": "r"},
            {"step_number": 1},
        )
        assert got == "07ca53db-606e-5b76-95bc-697e73bc4c41", got

    def test_go_parity_escaping_vector(self):
        from actae_client.session import _step_operation_id

        # Hostile content: non-ASCII + HTML chars + nested dicts. The id
        # must match the Go SDK's json.Marshal-based derivation exactly
        # (raw UTF-8, < > & and U+2028/29 escaped, payload-first keys).
        got = _step_operation_id(
            "ch-x", "inference", 3,
            {"input": "a < b & c > d", "z": 1, "ctx": {"nested": "\u00e9<&>"}},
            {"note": "x&y"},
        )
        assert got == "f1f23da4-abd0-50f9-b62c-1f1d0edd2fbf", got

    def test_go_parity_float_vector(self):
        from actae_client.session import _canonical_step_content

        # Float formatting must byte-match Go's json.Marshal float encoder
        # (verified against live Go output): integer-valued floats collapse
        # to ints, -0.0 -> 0, 1e-7 keeps no exponent padding, 1e20/1e15 use
        # fixed notation, 2.5e-7 has no padding.
        got = _canonical_step_content(
            {"f1": 1.0, "f2": 1e-7, "f3": 1e20, "f4": 1e15, "f5": -0.0,
             "f6": 1.5, "f7": 3.14159},
            {"f8": 2.5e-7, "n": 3},
        )
        assert got == (
            '{"payload":{"f1":1,"f2":1e-7,"f3":100000000000000000000,'
            '"f4":1000000000000000,"f5":0,"f6":1.5,"f7":3.14159},'
            '"metadata":{"f8":2.5e-7,"n":3}}'
        ), got

    def test_nan_and_infinity_fail_loudly(self):
        from actae_client.session import _canonical_step_content

        # Go's json.Marshal rejects NaN/Infinity; the canonical must fail
        # the same way (never a silently divergent id).
        for bad in (float("nan"), float("inf"), float("-inf")):
            with pytest.raises(ValueError):
                _canonical_step_content({"x": bad}, {})
            with pytest.raises(ValueError):
                _canonical_step_content({}, {"x": bad})

    def test_numpy_scalars_serialize_go_exactly(self):
        np = pytest.importorskip("numpy")
        from actae_client.session import _canonical_step_content

        # numpy scalars (common in ML payloads) must format like their
        # builtin counterparts, Go-exactly.
        got = _canonical_step_content(
            {"f": np.float64(1.5), "i": np.int64(7), "w": np.float64(1.0)},
            {},
        )
        assert got == '{"payload":{"f":1.5,"i":7,"w":1},"metadata":{}}', got


class TestSessionFence:
    """Opt-in per-channel ownership fence (``fence_owner``)."""

    @staticmethod
    def _claim(status, token="tok-1"):
        from types import SimpleNamespace

        return SimpleNamespace(
            status=status, execution=SimpleNamespace(id="exec-1"), claim_token=token, result=None
        )

    def test_fence_claims_and_releases_the_channel_lease(self):
        actae = _make_mock_actae()
        actae.claim_execution = AsyncMock(return_value=self._claim("claimed"))
        actae.heartbeat_execution = AsyncMock()
        actae.cancel_execution = AsyncMock()
        session = AgentSession(actae, "fenced", fence_owner="A", fence_lease_seconds=5)

        async def run():
            async with session:
                await session.step("work")

        asyncio.run(run())
        args, kwargs = actae.claim_execution.await_args
        assert args[1] == "__session_lock__"
        assert kwargs.get("lease_seconds") == 5
        actae.heartbeat_execution.assert_awaited()
        actae.cancel_execution.assert_awaited()

    def test_second_live_owner_cannot_start(self):
        from actae_client.session import SessionLockedError

        actae = _make_mock_actae()
        actae.claim_execution = AsyncMock(return_value=self._claim("in_progress"))
        session = AgentSession(actae, "fenced", fence_owner="B")

        async def enter():
            async with session:
                pass

        with pytest.raises(SessionLockedError):
            asyncio.run(enter())

    def test_stale_owner_step_is_rejected(self):
        from actae_client.errors import ExecutionNotOwnedError
        from actae_client.session import SessionLockedError

        actae = _make_mock_actae()
        actae.claim_execution = AsyncMock(return_value=self._claim("claimed"))
        actae.heartbeat_execution = AsyncMock(side_effect=ExecutionNotOwnedError("taken"))
        actae.cancel_execution = AsyncMock()
        session = AgentSession(actae, "fenced", fence_owner="A")

        async def run():
            with pytest.raises(SessionLockedError):
                async with session:
                    await session.step("work")

        asyncio.run(run())

    def test_unfenced_session_never_touches_the_ledger(self):
        actae = _make_mock_actae()
        actae.claim_execution = AsyncMock()
        session = AgentSession(actae, "plain")

        async def run():
            async with session:
                await session.step("work")

        asyncio.run(run())
        actae.claim_execution.assert_not_awaited()
