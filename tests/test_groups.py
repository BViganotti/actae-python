import asyncio
from actae_client.groups import GroupSession
from actae_client.types import Event, ExecutionGroupMember, GroupMessage, MemberLease


def message(identifier: str, member: str, kind: str) -> GroupMessage:
    return GroupMessage(identifier, "group", "router", member, kind, {}, None, "source", "delivery", "delivered", None)


class FakeClient:
    def __init__(self):
        self.queues = {"a": [[message("1", "a", "done")]], "b": [[], [message("2", "b", "candidate")]]}
        self.released = False

    async def group_messages(self, _group, member, **_kwargs):
        return self.queues[member].pop(0) if self.queues[member] else []

    async def claim_member(self, group, member, owner, **_kwargs):
        return MemberLease(group, member, owner, 3, "2099-01-01T00:00:00Z")

    async def heartbeat_member(self, group, member, owner, generation, **_kwargs):
        return MemberLease(group, member, owner, generation, "2099-01-01T00:00:00Z")

    async def release_member(self, *_args): self.released = True

    async def promote_execution_group_fork_member(self, group, member): return {"fork_group_id": group, "member_policies": {member: "reactive"}}


def test_durable_wait_any_all_and_claim_context():
    async def run():
        client = FakeClient(); group = GroupSession(client, "group")
        assert (await group.wait_for("a", "done")).message_id == "1"
        winner, found = await group.wait_any([("b", "candidate")], timeout=1)
        assert winner == "b" and found.message_id == "2"
        client.queues["a"] = [[message("3", "a", "done")]]
        client.queues["b"] = [[message("4", "b", "done")]]
        assert set(await group.wait_all([("a", "done"), ("b", "done")])) == {"a", "b"}
        async with group.claim_member("a", "worker", lease_seconds=30) as claimed:
            assert claimed._lease.generation == 3
        assert client.released
        assert (await group.promote("a"))["member_policies"]["a"] == "reactive"
    asyncio.run(run())


def test_websocket_group_stream_filters_delivery_events():
    async def run():
        class LiveClient(FakeClient):
            async def execution_group_members(self, _group):
                return [ExecutionGroupMember("group", "a", "member-a", None, {}, None, 0, None)]

            async def stream(self, topic, **_kwargs):
                assert topic == "member-a"
                yield Event("noise", topic, "broadcast", {}, "", 1, None, "", None, None)
                yield Event("delivery", topic, "message.received", {
                    "message_id": "m1", "group_id": "group",
                    "from": {"member": "router"}, "to": {"member": "a"},
                    "type": "task.done", "payload": {"ok": True},
                }, "", 2, None, "", None, None)

        values = [value async for value in GroupSession(LiveClient(), "group").member("a").stream_ws()]
        assert len(values) == 1 and values[0].message_id == "m1" and values[0].delivery_event_id == "delivery"
    asyncio.run(run())
