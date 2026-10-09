from typing import Optional


class ActaeError(Exception):
    """Base exception for all Actae client errors."""


class AuthError(ActaeError):
    """Raised when authentication with the Actae server fails.

    Includes missing or invalid API keys and JWT token expiry during WebSocket
    auth.
    """

    def __init__(self, message: str = "Authentication failed") -> None:
        super().__init__(message)


class ActaeConnectionError(ActaeError):
    """Raised when a connection to Actae fails or drops.

    Named to avoid shadowing Python's built-in ``ConnectionError``.

    Covers WebSocket failures (handshake, transport timeout, subscription
    ack timeout, underlying socket error) **and** HTTP transport failures
    (server unreachable, DNS/TLS failure, request timeout, premature
    close). HTTP *status* errors (4xx/5xx) raise ``APIError`` and friends
    instead.
    """

    def __init__(self, message: str = "Connection failed") -> None:
        super().__init__(message)


class RateLimitError(ActaeError):
    """Raised when the server responds with HTTP 429 (Too Many Requests).

    Attributes:
        retry_after_seconds: Number of seconds to wait before retrying.
    """

    def __init__(self, retry_after_seconds: int, message: str = "") -> None:
        self.retry_after_seconds = retry_after_seconds
        super().__init__(f"Rate limited — retry after {retry_after_seconds}s" +
                         (f": {message}" if message else ""))


class APIError(ActaeError):
    """Raised when an HTTP API call returns a non-success status code.

    Attributes:
        status_code: The HTTP status code returned by the server.
    """

    def __init__(self, status_code: int, message: str = "") -> None:
        self.status_code = status_code
        super().__init__(f"HTTP {status_code}: {message}" if message else f"HTTP {status_code}")


class SnapshotBoundaryError(ActaeError):
    """Raised when a fork's ``at_cursor`` has no saved state boundary.

    The server responds ``409 snapshot_boundary_required`` — there is no
    saved snapshot at or before the requested cursor. Save state on the
    source channel first, or fork at ``at_cursor=0`` to use the latest state.
    """

    def __init__(self, message: str = "No saved state boundary at or before the requested cursor") -> None:
        super().__init__(message)


class NoRestorableCheckpointError(ActaeError):
    """Raised by ``AgentSession`` in strict (``exact``) boundary mode when a
    fork cannot restore a checkpoint at the requested step.

    The server reported ``snapshot_boundary_required`` (no saved snapshot at
    or before the step's cursor), and the session refused to silently fall
    forward to the latest state — doing so would contaminate the fork with
    state from *after* the requested boundary. To fork anyway:
    - save state on every step you care about (``snapshot_interval=1`` with a
      ``state_fn``), or
    - fork with ``boundary_mode="approximate"`` (inherit the nearest prior
      snapshot / latest state and inspect ``resolved_cursor`` for drift), or
    - fork with ``boundary_mode="lineage_only"`` (create the fork with no
      state copy — ``restorable=False``).
    """

    def __init__(self, message: str = "No restorable checkpoint at the requested fork boundary") -> None:
        super().__init__(message)


class IdempotencyConflictError(ActaeError):
    """Raised when a fork's ``operation_id`` was reused with a different request.

    The server responds ``409 idempotency_conflict`` — the same idempotency
    key was already used with different request parameters. Replay the
    original request, or use a fresh ``operation_id``.
    """

    def __init__(self, message: str = "Idempotency key already used with a different request") -> None:
        super().__init__(message)


class ChannelConflictError(ActaeError):
    """Raised when a fork's ``new_channel_id`` already exists under a different
    fork definition (different source or boundary).

    The server responds ``409 channel_conflict``. Use a fresh child channel id.
    """

    def __init__(self, message: str = "Channel already exists under a different fork definition") -> None:
        super().__init__(message)

class CounterfactualBlockedError(ActaeError):
    """A frozen boundary or BLOCK/REPLAY tool policy refused unsafe work."""
    pass


class ForkToolBlockedError(ActaeError):
    """A forked channel's ``tool_policies`` refused to execute a tool.

    The server responds ``409 fork_tool_blocked`` and persists a
    ``tool.blocked`` boundary event. This happens when the effective policy is
    ``block``, or ``replay`` with no exact source match (a *miss*/mismatch).
    The tool was **not** executed. Resolve the inherited result, change the
    policy to ``auto``/``live``, or continue the fork without that call.
    """

    def __init__(
        self,
        message: str = "Fork tool policy blocked the tool execution",
        *,
        boundary: Optional[str] = None,
    ) -> None:
        super().__init__(message)
        self.boundary = boundary


class VersionConflictError(ActaeError):
    """Raised when an optimistic-concurrency guard rejects a write.

    The server responds ``409 version_conflict`` — the channel's latest
    state version/cursor no longer matches ``expected_version`` /
    ``expected_cursor``. Reload the latest state and retry.
    """

    def __init__(self, message: str = "State version conflict — reload latest state and retry") -> None:
        super().__init__(message)


class ConsumerError(ActaeError):
    """Raised for consumer-group errors (missing group, expired lease)."""

    def __init__(self, message: str = "Consumer group error") -> None:
        super().__init__(message)


class IdempotencyKeyMismatchError(ActaeError):
    """Raised when an execution claim collides with a different request.

    The server responds ``409 idempotency_key_mismatch`` — the idempotency
    key ``(channel_id, key_name)`` was already used with different request
    parameters (the stored request hash differs). Use a fresh key, or resend
    the original request parameters.
    """

    def __init__(
        self,
        message: str = "Idempotency key already used with a different request",
    ) -> None:
        super().__init__(message)


class ExecutionNotOwnedError(ActaeError):
    """Raised when an execution operation supplies a stale claim token.

    The server responds ``409 execution_not_owned`` — the execution is owned
    by another attempt (its ``claim_token`` differs). Re-claim the execution
    to obtain a fresh token before completing/failing/heartbeating.
    """

    def __init__(
        self,
        message: str = "Execution is owned by another attempt",
    ) -> None:
        super().__init__(message)


class ExecutionNotFoundError(ActaeError):
    """Raised when an execution id does not exist (HTTP 404)."""

    def __init__(self, message: str = "Execution not found") -> None:
        super().__init__(message)


class ActaeLangGraphError(ActaeError):
    """Error raised by the Actae LangGraph checkpointer.

    Covers config problems (missing ``thread_id``) and checkpoint data
    corruption. Defined here (not in the adapter module) so it can be
    imported from ``actae_client`` without installing LangGraph.
    """

    def __init__(self, message: str = "LangGraph checkpointer error") -> None:
        super().__init__(message)


class ActaeLangGraphSyncError(ActaeLangGraphError):
    """Raised when a sync checkpointer call runs inside a running loop.

    ``ActaeCheckpointSaver`` sync protocol methods (``put``, ``get_tuple``,
    …) execute on a fresh event loop; calling them from within a running
    loop would deadlock, so they refuse loudly instead.
    """
