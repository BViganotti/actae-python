"""
Generic State Manager — framework-agnostic save/load/resume for any agent.

Usage:
    from actae_client.adapters.base import StateManager

    mgr = StateManager(actae_client, channel="my-agent")

    # Resume from last run
    state = await mgr.resume()
    if state is None:
        state = {"messages": [], "cursor": 0, "completed": []}

    # Save context every few steps
    await mgr.save(state)
"""

from typing import Any, Dict, List, Optional

from ..client import ActaeClient
from ..errors import SnapshotBoundaryError


class StateManager:
    """Manages versioned cursor-aligned state snapshots for any agent framework.

    Each ``save()`` creates a new immutable version. Use ``list_versions()``
    to browse history and ``get_version()`` to inspect a specific snapshot.
    """

    def __init__(self, actae: ActaeClient, channel: str):
        """Create a StateManager for the given Actae channel.

        Args:
            actae: Connected ActaeClient instance.
            channel: Channel ID to manage state for.
        """
        self.actae = actae
        self.channel = channel

    async def save(self, state: Dict[str, Any]) -> int:
        """Save a state snapshot. Returns the assigned version number."""
        cursor = await self.actae.latest_cursor(self.channel) or 0
        return await self.actae.save_state(self.channel, cursor, state)

    async def load(self) -> Optional[Dict[str, Any]]:
        """Load the latest state snapshot."""
        snapshot = await self.actae.latest_state(self.channel)
        if snapshot is None:
            return None
        return snapshot["state"]

    async def resume(self, default_state: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Load latest state or return default."""
        state = await self.load()
        if state is None:
            return default_state or {}
        return state

    async def fork(self, new_channel: str, *, reason: Optional[str] = None) -> "StateManager":
        """Fork this state into a new channel.

        Copies the latest state snapshot into ``new_channel`` (inherited by
        the fork) and returns a ``StateManager`` bound to it — the
        framework-agnostic "fork at the current point, refine from here"
        primitive. Works for any framework: the state is an opaque dict.

        Args:
            new_channel: Channel ID for the fork.
            reason: Optional human-readable reason recorded on the fork.

        Returns:
            A ``StateManager`` for the fork channel; ``load()`` on it returns
            the inherited state.

        Raises:
            ValueError: If this channel has no state to fork.
        """
        cursor = await self.actae.latest_cursor(self.channel) or 0
        # The server forks the latest state at-or-before `cursor`. If no
        # snapshot exists, fall back to at_cursor=0 (latest) so event-only
        # channels still fork; a channel with no state/events raises.
        try:
            await self.actae.fork(
                self.channel,
                new_channel,
                cursor,
                display_name=new_channel,
                reason=reason or f"Forked {self.channel} → {new_channel}",
            )
        except SnapshotBoundaryError:
            # No snapshot boundary: try latest state (at_cursor=0).
            await self.actae.fork(
                self.channel,
                new_channel,
                0,
                display_name=new_channel,
                reason=reason or f"Forked {self.channel} → {new_channel}",
            )
        return StateManager(self.actae, new_channel)

    async def list_versions(self) -> List[Dict[str, Any]]:
        """List all state version history for this channel."""
        return await self.actae.list_states(self.channel)

    async def get_version(self, version: int) -> Optional[Dict[str, Any]]:
        """Load a specific state snapshot version."""
        return await self.actae.get_state(self.channel, version)

    async def delete_version(self, version: int) -> None:
        """Delete a specific state snapshot version."""
        await self.actae.delete_state(self.channel, version)
