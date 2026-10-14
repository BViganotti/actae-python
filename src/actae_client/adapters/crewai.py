"""
CrewAI State Adapter for Actae.

Provides automatic save of completed tasks and task outputs as
cursor-aligned state snapshots. Includes a CrewAIResumer for
one-line restart with automatic skip of completed tasks.

Usage:
    from actae_client.adapters.crewai import ActaeCrewStateHook, CrewAIResumer
    from crewai import Crew

    hook = ActaeCrewStateHook(actae_client, channel="my-crew")
    crew = Crew(..., step_callback=hook.after_step)
    crew.kickoff()

    # On restart:
    resumer = CrewAIResumer(actae_client, channel="my-crew")
    remaining = await resumer.get_remaining_tasks(all_tasks)
    crew.kickoff(tasks=remaining)
"""

from typing import Any, Dict, List, Optional

from ..client import ActaeClient
from .contract import adapter_checkpoint_state, strip_checkpoint_metadata


class ActaeCrewStateHook:
    """Saves CrewAI state as cursor-aligned snapshots after each task.

    Captures: completed tasks, agent memory, task outputs, crew configuration.
    On restart, provides the list of tasks that still need execution.
    """

    def __init__(self, actae: ActaeClient, channel: str = "crewai"):
        """Create a CrewAI state hook.

        Args:
            actae: Connected ActaeClient instance.
            channel: Channel ID for state storage (default ``"crewai"``).
        """
        self.actae = actae
        self.channel = channel
        self._completed: List[str] = []
        self._outputs: List[Dict[str, Any]] = []

    async def after_step(self, task_output: Any, crew: Any = None) -> Dict[str, Any]:
        task_name = getattr(task_output, "description", "unknown")
        task_raw = getattr(task_output, "raw", str(task_output))
        self._completed.append(task_name)
        self._outputs.append({
            "task": task_name,
            "output": task_raw,
        })

        state: Dict[str, Any] = {
            "completed_tasks": self._completed,
            "task_outputs": self._outputs,
        }
        if crew is not None:
            state["crew_id"] = getattr(crew, "id", "unknown")

        cursor = await self.actae.latest_cursor(self.channel) or 0
        state = adapter_checkpoint_state(
            state,
            framework="crewai",
            channel_id=self.channel,
            portable_state={
                "completed_tasks": list(self._completed),
                "task_outputs": list(self._outputs),
            },
            native_checkpoint={"resume": "remaining_tasks"},
            event_cursor=cursor,
        )
        await self.actae.save_state(self.channel, cursor, state)
        return state

    async def load_state(self) -> Optional[Dict[str, Any]]:
        snapshot = await self.actae.latest_state(self.channel)
        if snapshot is None:
            return None
        return strip_checkpoint_metadata(snapshot["state"])

    async def save_state(self, state: Dict[str, Any]) -> None:
        cursor = await self.actae.latest_cursor(self.channel) or 0
        await self.actae.save_state(
            self.channel,
            cursor,
            adapter_checkpoint_state(
                state,
                framework="crewai",
                channel_id=self.channel,
                portable_state=state,
                native_checkpoint={"resume": "remaining_tasks"},
                event_cursor=cursor,
            ),
        )


class CrewAIResumer:
    """Resume a CrewAI crew from the last Actae state snapshot.

    On restart, loads the state, filters out already-completed tasks,
    and returns the remaining tasks for kickoff.

    Usage:
        resumer = CrewAIResumer(actae_client, channel="my-crew")
        remaining = await resumer.get_remaining_tasks(my_tasks)
        if remaining:
            crew.kickoff(tasks=remaining)
    """

    def __init__(self, actae: ActaeClient, channel: str = "crewai"):
        """Create a CrewAI resumer.

        Args:
            actae: Connected ActaeClient instance.
            channel: Channel ID for state storage (default ``"crewai"``).
        """
        self.actae = actae
        self.channel = channel
        self._hook = ActaeCrewStateHook(actae, channel)

    async def get_remaining_tasks(
        self,
        all_tasks: List[Any],
    ) -> List[Any]:
        """Return tasks not yet completed according to the last state snapshot.

        `all_tasks` can be CrewAI Task objects (matched by `.description`)
        or plain strings.
        """
        state = await self._hook.load_state()
        if state is None:
            return all_tasks

        completed = state.get("completed_tasks", [])
        remaining = []
        for task in all_tasks:
            if isinstance(task, str):
                name = task
            else:
                name = getattr(task, "description", str(task))
            if name not in completed:
                remaining.append(task)
        return remaining

    async def resume(
        self,
        crew: Any,
        all_tasks: List[Any],
    ) -> Any:
        """Full resume: load state, skip completed tasks, kickoff.

        Returns the crew's kickoff result or None if all tasks were already done.
        """
        remaining = await self.get_remaining_tasks(all_tasks)
        if not remaining:
            return None

        self._hook._completed = []
        self._hook._outputs = []
        return crew.kickoff(tasks=remaining)

    async def fork(
        self,
        new_channel: str,
        *,
        reason: Optional[str] = None,
    ) -> "CrewAIResumer":
        """Fork this crew's state into a new channel.

        Copies the latest state snapshot (completed tasks, outputs, context)
        into ``new_channel`` and returns a ``CrewAIResumer`` bound to it — the
        CrewAI-native counterpart of ``StateManager.fork``. The fork's
        ``get_remaining_tasks`` reflects the same completed tasks, so you can
        refine a later task against the inherited crew state without re-running
        the earlier ones.

        Args:
            new_channel: Channel ID for the fork.
            reason: Optional human-readable reason recorded on the fork.

        Returns:
            A ``CrewAIResumer`` for the fork channel.

        Raises:
            ValueError: If this channel has no state to fork.
        """
        cursor = await self.actae.latest_cursor(self.channel) or 0
        try:
            await self.actae.fork(
                self.channel,
                new_channel,
                cursor,
                display_name=new_channel,
                reason=reason or f"Forked {self.channel} → {new_channel}",
            )
        except Exception:
            await self.actae.fork(
                self.channel,
                new_channel,
                0,
                display_name=new_channel,
                reason=reason or f"Forked {self.channel} → {new_channel}",
            )
        return CrewAIResumer(self.actae, new_channel)
