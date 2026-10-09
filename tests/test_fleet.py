import pytest

from actae_client.fleet import FleetClient


@pytest.mark.asyncio
async def test_fleet_instances_uses_organization_gateway_route(monkeypatch):
    client = FleetClient("org-secret", "https://fleet.example", "org-1")
    calls = []

    async def request(*args, **kwargs):
        calls.append((args, kwargs))
        return {"organization_id": "org-1", "instances": []}

    monkeypatch.setattr(client, "_request", request)
    await client.instances()
    assert calls[0][0][:2] == ("GET", "/v1/organizations/org-1/instances")


@pytest.mark.asyncio
async def test_token_exchange_sends_required_body(monkeypatch):
    """PR-006: the fleet token exchange must send the canonical body
    {purpose, organization_id, scopes} — the real SaaS returns 422 without
    it."""
    client = FleetClient("org-secret", "https://fleet.example", "org-1")
    captured = {}

    async def request(method, path, **kwargs):
        captured.update(method=method, path=path, **kwargs)
        return {"token": "fleet-tok", "expires_in": 300}

    monkeypatch.setattr(client, "_request", request)
    token = await client._token()
    assert token == "fleet-tok"
    assert captured["method"] == "POST"
    assert captured["path"] == "/api/v1/fleet/token"
    assert captured["body"] == {
        "purpose": "fleet_access",
        "organization_id": "org-1",
        "scopes": client.requested_scopes,
    }
    assert captured["headers"] == {"Authorization": "Bearer org-secret"}
    assert captured["auth"] is False


@pytest.mark.asyncio
async def test_token_exchange_respects_requested_scopes(monkeypatch):
    """PR-006: explicitly configured scopes are sent; the default is a
    least-privilege data surface."""
    client = FleetClient("org-secret", "https://fleet.example", "org-1",
                         requested_scopes=["events:read"])
    captured = {}

    async def request(method, path, **kwargs):
        captured.update(**kwargs)
        return {"token": "t", "expires_in": 300}

    monkeypatch.setattr(client, "_request", request)
    await client._token()
    assert captured["body"]["scopes"] == ["events:read"]
    assert client.requested_scopes == ["events:read"]


@pytest.mark.asyncio
async def test_fleet_uses_canonical_event_and_state_routes(monkeypatch):
    client = FleetClient("org-secret", "https://fleet.example", "org-1")
    calls = []

    async def request(method, path, **kwargs):
        calls.append((method, path, kwargs))
        return {"ok": True}

    monkeypatch.setattr(client, "_request", request)
    await client.query("inst-1", channel_ids=["run"], limit=10)
    await client.replay("inst-1", "run", cursor=3)
    await client.save_state("inst-1", "run", 7, {"x": 1})
    await client.latest_state("inst-1", "run")

    assert [(method, path) for method, path, _ in calls] == [
        ("POST", "/v1/organizations/org-1/events/query"),
        ("POST", "/v1/organizations/org-1/instances/inst-1/ops/actae.api.v1.events.replay"),
        ("PUT", "/v1/organizations/org-1/instances/inst-1/state/run"),
        ("POST", "/v1/organizations/org-1/instances/inst-1/ops/actae.api.v1.state.load"),
    ]
    assert calls[0][2]["body"] == {"instances": ["inst-1"], "query": {"channel_ids": ["run"], "limit": 10}}
    assert calls[2][2]["body"] == {"channel_id": "run", "cursor": 7, "state": {"x": 1}}


@pytest.mark.asyncio
async def test_fleet_call_passes_idempotency_and_confirmation(monkeypatch):
    client = FleetClient("org-secret", "https://fleet.example", "org-1")
    captured = {}

    async def request(method, path, **kwargs):
        captured.update(method=method, path=path, **kwargs)
        return {"ok": True}

    monkeypatch.setattr(client, "_request", request)
    await client.call("DELETE", "/api/v1/channels/run", instance_id="inst-1",
                      operation_id="op-1", confirm_target="run")
    assert captured["headers"] == {
        "Idempotency-Key": "op-1", "X-Actae-Confirm-Target": "run"
    }


@pytest.mark.asyncio
async def test_fleet_refuses_unconfirmed_delete():
    client = FleetClient("org-secret", "https://fleet.example", "org-1")
    with pytest.raises(ValueError, match="confirm_target"):
        await client.call("DELETE", "/api/v1/channels/run", instance_id="inst-1")


@pytest.mark.asyncio
async def test_fleet_partial_result_requires_explicit_completion(monkeypatch):
    client = FleetClient("org-secret", "https://fleet.example", "org-1")
    calls = []

    async def request(*args, **kwargs):
        calls.append((args, kwargs))
        return {"events": [{"id": "ok"}], "errors": [{"instance_id": "down", "code": "unavailable", "retryable": True}]}

    monkeypatch.setattr(client, "_request", request)
    result = await client.query_many(instance_ids=["ok", "down"])
    assert result.items == [{"id": "ok"}]
    assert result.errors[0].retryable is True
    with pytest.raises(Exception, match="partial fleet result"):
        result.require_complete()
    assert calls[0][0][:2] == ("POST", "/v1/organizations/org-1/events/query")


@pytest.mark.asyncio
async def test_query_many_uses_top_level_page_tokens(monkeypatch):
    """PR-008: selectors are `instances`, filters are `query`, and the
    per-instance continuation map is the top-level `page_tokens` object. The
    returned page token is the full map, never a single-instance collapse."""
    client = FleetClient("org-secret", "https://fleet.example", "org-1")
    calls = []

    async def request(*args, **kwargs):
        calls.append((args, kwargs))
        return {
            "events": [{"id": "e1"}],
            "page": {"next_page_tokens": {"ins-a": "tok-a", "ins-b": "tok-b"}, "has_more": True},
        }

    monkeypatch.setattr(client, "_request", request)
    result = await client.query_many(
        instance_ids=["ins-a", "ins-b"],
        page_tokens={"ins-a": "prev-a", "ins-b": "prev-b"},
        limit=25,
        channel_ids=["run"],
    )
    body = calls[0][1]["body"]
    assert body["instances"] == ["ins-a", "ins-b"]
    assert body["query"]["limit"] == 25
    assert body["query"]["channel_ids"] == ["run"]
    # Continuation is top-level, not inside query.
    assert body["page_tokens"] == {"ins-a": "prev-a", "ins-b": "prev-b"}
    assert "page_token" not in body["query"]
    # The full per-instance map is preserved.
    assert result.page_tokens == {"ins-a": "tok-a", "ins-b": "tok-b"}


@pytest.mark.asyncio
async def test_query_page_sends_top_level_single_token(monkeypatch):
    """PR-008: single-instance page tokens are sent as `page_tokens:
    {instance_id: token}`, never inside `query`."""
    client = FleetClient("org-secret", "https://fleet.example", "org-1")
    calls = []

    async def request(*args, **kwargs):
        calls.append((args, kwargs))
        return {"events": [{"id": "e1"}], "page": {"next_page_tokens": {"ins-1": "next-tok"}, "has_more": True}}

    monkeypatch.setattr(client, "_request", request)
    page = await client.query_page("ins-1", page_token="prev-tok", limit=5)
    body = calls[0][1]["body"]
    assert body["instances"] == ["ins-1"]
    assert body["query"]["limit"] == 5
    assert body["page_tokens"] == {"ins-1": "prev-tok"}
    assert "page_token" not in body["query"]
    assert page.next_page_token == "next-tok"


@pytest.mark.asyncio
async def test_instance_transport_preserves_session_record_shape(monkeypatch):
    client = FleetClient("org-secret", "https://fleet.example", "org-1")

    async def request(method, path, **kwargs):
        assert (method, path) == ("POST", "/v1/organizations/org-1/instances/inst-1/ops/actae.api.v1.events.record")
        return {"instance_id": "inst-1", "data": {"event": {"id": "e1", "channel_id": "run", "type": "agent.step", "payload": {}, "cursor": 1, "actor": "agent", "timestamp": "2026-01-01T00:00:00Z"}}}

    monkeypatch.setattr(client, "_request", request)
    event = await client.for_instance("inst-1").record("run", "agent.step", {}, actor="agent", step_number=1)
    assert event.id == "e1"


@pytest.mark.asyncio
async def test_ticketed_stream_requires_rotation_source():
    client = FleetClient("org-secret", "https://fleet.example", "org-1")
    stream = client.stream(reconnect=True)
    with pytest.raises(ValueError, match="ticket"):
        await stream.__anext__()
    await client.close()
