"""High-level durable coordination API for execution groups."""

import asyncio
import time
import uuid
from typing import Any, AsyncIterator, Dict, Iterable, List, Optional, Tuple

from .types import GroupMessage, MemberLease


class GroupMemberSession:
    """One logical group member. No agent is run by Actae or this SDK."""

    def __init__(self, group: "GroupSession", member_id: str) -> None:
        self.group, self.member_id = group, member_id
        self._lease: Optional[MemberLease] = None

    async def claim(self, owner_id: Optional[str] = None, *, lease_seconds: int = 60) -> MemberLease:
        self._lease = await self.group.client.claim_member(self.group.group_id, self.member_id, owner_id or str(uuid.uuid4()), lease_seconds=lease_seconds)
        return self._lease

    async def heartbeat(self, *, lease_seconds: int = 60) -> MemberLease:
        if self._lease is None:
            raise RuntimeError("claim_member() must succeed before heartbeat()")
        self._lease = await self.group.client.heartbeat_member(self.group.group_id, self.member_id, self._lease.owner_id, self._lease.generation, lease_seconds=lease_seconds)
        return self._lease

    async def release(self) -> None:
        if self._lease is None:
            return
        lease, self._lease = self._lease, None
        await self.group.client.release_member(self.group.group_id, self.member_id, lease.owner_id, lease.generation)

    async def send(self, to: str, message_type: str, payload: Any, *, causal_context: Optional[Dict[str, Any]] = None, operation_id: Optional[str] = None) -> GroupMessage:
        return await self.group.client.send_group_message(self.group.group_id, self.member_id, to, message_type, payload, causal_context=causal_context, operation_id=operation_id)

    async def messages(self, *, after: Optional[str] = None, limit: int = 100) -> List[GroupMessage]:
        return await self.group.client.group_messages(self.group.group_id, self.member_id, after=after, limit=limit)

    async def _channel_id(self) -> str:
        members = await self.group.client.execution_group_members(self.group.group_id)
        for member in members:
            if member.member_id == self.member_id:
                return member.channel_id
        raise ValueError(f"unknown execution-group member {self.member_id!r}")

    async def subscribe_ws(self, *, cursor: Optional[int] = None) -> None:
        """Subscribe this member's channel on the client's live WebSocket."""
        await self.group.client.subscribe(await self._channel_id(), cursor=cursor)

    async def unsubscribe_ws(self) -> None:
        """Remove this member's channel from the live WebSocket."""
        await self.group.client.unsubscribe(await self._channel_id())

    async def stream_ws(self, *, cursor: Optional[int] = None) -> AsyncIterator[GroupMessage]:
        """Yield group deliveries from WebSocket replay followed by live events.

        The server's cursor replay makes this safe across a reconnect when the
        caller persists the last event cursor. The durable ``stream`` method
        remains available for message-identity polling semantics.
        """
        channel_id = await self._channel_id()
        async for event in self.group.client.stream(channel_id, cursor=cursor):
            if event.event_type != "message.received":
                continue
            message = GroupMessage.from_websocket_event(event)
            if message.group_id == self.group.group_id and message.to_member_id == self.member_id:
                yield message

    async def acknowledge(self, message: GroupMessage) -> GroupMessage:
        if self._lease is None:
            raise RuntimeError("claim_member() must succeed before acknowledge()")
        return await self.group.client.acknowledge_group_message(message.message_id, self._lease.owner_id, self._lease.generation)

    async def wait_for(self, message_type: str, *, timeout: Optional[float] = None, after: Optional[str] = None, poll_interval: float = 0.2) -> GroupMessage:
        """Durably wait across restarts: query history first, then poll.

        The cursor is a message identity rather than in-memory state, so a
        restarted caller sees deliveries that occurred while it was offline.
        """
        deadline = None if timeout is None else time.monotonic() + timeout
        cursor = after
        while True:
            batch = await self.messages(after=cursor, limit=100)
            for message in batch:
                cursor = message.message_id
                if message.message_type == message_type:
                    return message
            if deadline is not None and time.monotonic() >= deadline:
                raise TimeoutError(f"timed out waiting for {message_type!r}")
            await asyncio.sleep(poll_interval)

    async def stream(self, *, after: Optional[str] = None, poll_interval: float = 0.2) -> AsyncIterator[GroupMessage]:
        cursor = after
        while True:
            batch = await self.messages(after=cursor, limit=100)
            if not batch:
                await asyncio.sleep(poll_interval)
                continue
            for message in batch:
                cursor = message.message_id
                yield message


class GroupSession:
    def __init__(self, client: Any, group_id: str) -> None:
        self.client, self.group_id = client, group_id

    def member(self, member_id: str) -> GroupMemberSession:
        return GroupMemberSession(self, member_id)

    async def wait_for(self, member: str, event: str, *, timeout: Optional[float] = None, after: Optional[str] = None) -> GroupMessage:
        return await self.member(member).wait_for(event, timeout=timeout, after=after)

    def claim_member(self, member_id: str, owner_id: Optional[str] = None, *, lease_seconds: int = 60) -> "ClaimedMember":
        return ClaimedMember(self.member(member_id), owner_id, lease_seconds)

    async def promote(self, member_id: str) -> Dict[str, Any]:
        return await self.client.promote_execution_group_fork_member(self.group_id, member_id)

    async def wait_any(self, waits: Iterable[Tuple[str, str]], *, timeout: Optional[float] = None) -> Tuple[str, GroupMessage]:
        tasks = {asyncio.create_task(self.member(member).wait_for(event, timeout=timeout)): member for member, event in waits}
        if not tasks: raise ValueError("wait_any requires at least one waiter")
        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        for task in pending: task.cancel()
        result = next(iter(done)).result()
        return tasks[next(iter(done))], result

    async def wait_all(self, waits: Iterable[Tuple[str, str]], *, timeout: Optional[float] = None) -> Dict[str, GroupMessage]:
        pairs = list(waits)
        results = await asyncio.gather(*(self.member(member).wait_for(event, timeout=timeout) for member, event in pairs))
        return {member: message for (member, _), message in zip(pairs, results)}


class ClaimedMember:
    """Async context that renews a member lease and releases it on exit."""
    def __init__(self, member: GroupMemberSession, owner_id: Optional[str], lease_seconds: int) -> None:
        self.member, self.owner_id, self.lease_seconds = member, owner_id, lease_seconds
        self._task: Optional[asyncio.Task[None]] = None

    async def __aenter__(self) -> GroupMemberSession:
        await self.member.claim(self.owner_id, lease_seconds=self.lease_seconds)
        self._task = asyncio.create_task(self._renew())
        return self.member

    async def _renew(self) -> None:
        try:
            while True:
                await asyncio.sleep(max(1.0, self.lease_seconds / 3))
                await self.member.heartbeat(lease_seconds=self.lease_seconds)
        except asyncio.CancelledError:
            pass

    async def __aexit__(self, *_: object) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        await self.member.release()
