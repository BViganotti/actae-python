"""Organization/fleet client for the canonical Actae API.

The fleet gateway is deliberately thin: this class only adds instance routing
and token exchange; event, state and fork semantics remain the same as the
direct API.  Tokens are process-memory only.
"""
from __future__ import annotations

import asyncio
import time
import uuid
from dataclasses import dataclass, field
import os
from typing import Any, AsyncIterator, Dict, Optional

import aiohttp

from .errors import APIError, AuthError, ActaeConnectionError
from .types import ChannelMetadata, Event, ForkReceipt, _unwrap_sonic
from .manifest import ManifestOperation, MANIFEST_OPERATIONS, manifest_operation


@dataclass(frozen=True)
class InstanceError:
    """A safe per-instance failure returned by a multi-instance operation."""
    instance_id: str
    code: str
    message: str
    retryable: bool = False


@dataclass(frozen=True)
class Page:
    """Opaque keyset page. ``next_page_token`` must be passed through unchanged."""
    items: list[Any]
    next_page_token: Optional[str] = None


@dataclass(frozen=True)
class PartialResult:
    """Fleet result that never hides which selected instances failed."""
    items: list[Any]
    errors: list[InstanceError] = field(default_factory=list)
    next_page_token: Optional[str] = None
    # PR-008: the full per-instance continuation map (instance_id -> opaque
    # token) for multi-instance reads. Preserved unchanged; never collapsed to
    # a single token.
    page_tokens: Optional[Dict[str, str]] = None

    def require_complete(self) -> list[Any]:
        if self.errors:
            detail = ", ".join(f"{e.instance_id}: {e.code}" for e in self.errors)
            raise APIError(502, f"partial fleet result: {detail}")
        return self.items


class UnsupportedOperationError(APIError):
    """The target instance/gateway does not advertise a requested operation."""
    def __init__(self, operation_id: str) -> None:
        self.operation_id = operation_id
        super().__init__(426, f"unsupported API operation: {operation_id}")


class FleetClient:
    """Async client for fleet endpoints (server-side use only)."""

    def __init__(self, organization_token: str, endpoint: Optional[str] = None,
                 organization_id: Optional[str] = None,
                 *, token_endpoint: Optional[str] = None, timeout: float = 30.0,
                 requested_scopes: Optional[list[str]] = None) -> None:
        if not organization_token:
            raise ValueError("organization_token is required")
        self.endpoint = (endpoint or os.getenv("ACTAE_FLEET_URL", "")).rstrip("/")
        self.token_endpoint = (token_endpoint or os.getenv("ACTAE_FLEET_TOKEN_URL", self.endpoint)).rstrip("/")
        if not self.endpoint:
            raise ValueError("endpoint or ACTAE_FLEET_URL is required")
        self.organization_token = organization_token
        self.organization_id = organization_id or os.getenv("ACTAE_ORG_ID", "")
        if not self.organization_id:
            raise ValueError("organization_id or ACTAE_ORG_ID is required")
        # PR-006: requested scopes are explicit in configuration. The default
        # is a least-privilege read+write data surface; callers must
        # intentionally broaden. These are intersected with the token's own
        # grants at exchange time.
        self.requested_scopes = requested_scopes or os.getenv(
            "ACTAE_FLEET_SCOPES", ""
        ).split(",") or [
            "events:read", "events:write",
            "state:read", "state:write",
            "channels:read", "channels:write",
            "forks:create",
        ]
        self.timeout = timeout
        self._fleet_token: Optional[str] = None
        self._fleet_token_expires_at = 0.0
        self._token_lock = asyncio.Lock()
        self._session: Optional[aiohttp.ClientSession] = None
        self._capabilities: Dict[Optional[str], Any] = {}

    @classmethod
    def from_env(cls, *, endpoint: Optional[str] = None, timeout: float = 30.0) -> "FleetClient":
        """Create a backend-only fleet client from ACTAE_ORG_TOKEN/FLEET_URL."""
        token = os.getenv("ACTAE_ORG_TOKEN", "")
        return cls(token, endpoint or os.getenv("ACTAE_FLEET_URL"), os.getenv("ACTAE_ORG_ID"), token_endpoint=os.getenv("ACTAE_FLEET_TOKEN_URL"), timeout=timeout)

    def _gateway_path(self, suffix: str) -> str:
        return f"/v1/organizations/{self.organization_id}{suffix}"

    @staticmethod
    def _unwrap_gateway(data: Any) -> Any:
        return data.get("data", data) if isinstance(data, dict) and isinstance(data.get("data"), dict) else data

    async def __aenter__(self) -> "FleetClient":
        self._session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, *_: Any) -> None:
        await self.close()

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()
        self._session = None

    async def _token(self) -> str:
        if self._fleet_token and time.monotonic() < self._fleet_token_expires_at:
            return self._fleet_token
        async with self._token_lock:
            if self._fleet_token and time.monotonic() < self._fleet_token_expires_at:
                return self._fleet_token
            # PR-006: the canonical exchange body requires purpose, org id and
            # the requested scope set. Without it the real SaaS returns 422.
            body = {
                "purpose": "fleet_access",
                "organization_id": self.organization_id,
                "scopes": self.requested_scopes,
            }
            result = await self._request("POST", "/api/v1/fleet/token",
                                         body=body,
                                         headers={"Authorization": f"Bearer {self.organization_token}"},
                                         auth=False, _base_url=self.token_endpoint)
            token = result.get("token") or result.get("access_token")
            if not token:
                raise AuthError("fleet token exchange returned no access token")
            self._fleet_token = str(token)
            # Canonical `expires_in` (seconds). Refresh 15s early.
            expires_in = result.get("expires_in", 300)
            try:
                self._fleet_token_expires_at = time.monotonic() + max(0.0, float(expires_in) - 15.0)
            except (TypeError, ValueError):
                self._fleet_token_expires_at = time.monotonic() + 285.0
            return self._fleet_token

    async def _request(self, method: str, path: str, *, instance_id: Optional[str] = None,
                       body: Any = None, headers: Optional[Dict[str, str]] = None,
                       auth: bool = True, _retried: bool = False, _base_url: Optional[str] = None) -> Any:
        if self._session is None:
            self._session = aiohttp.ClientSession()
        hdr = dict(headers or {})
        if auth:
            hdr["Authorization"] = f"Bearer {await self._token()}"
        if instance_id:
            hdr["X-Actae-Instance-Id"] = instance_id
        try:
            async with self._session.request(method, (_base_url or self.endpoint) + path, json=body,
                                              headers=hdr,
                                              timeout=aiohttp.ClientTimeout(total=self.timeout)) as resp:
                data = await resp.json(content_type=None)
                if resp.status == 401:
                    self._fleet_token = None
                    self._fleet_token_expires_at = 0.0
                    if auth and not _retried:
                        return await self._request(method, path, instance_id=instance_id, body=body,
                                                   headers=headers, auth=True, _retried=True, _base_url=_base_url)
                    raise AuthError(str(data.get("message", "Authentication failed")))
                if resp.status >= 400:
                    raise APIError(resp.status, str(data.get("message", data)))
                return data
        except aiohttp.ClientError as exc:
            raise ActaeConnectionError(str(exc)) from exc

    async def instances(self) -> Any:
        return await self._request("GET", self._gateway_path("/instances"))

    async def capabilities(self, instance_id: Optional[str] = None) -> Any:
        """Fetch and cache the advertised API capabilities for a target.

        PR-017: uses the gateway's typed capability endpoint
        (`/v1/organizations/{org}/instances/{instance}/capabilities`) which
        returns the gateway protocol version, the target instance's
        API/engine versions, and the supported fleet operation ids. The cache
        is keyed by instance and invalidated on a 426 (upgrade required).
        """
        if instance_id not in self._capabilities:
            try:
                if not instance_id:
                    raise ValueError("instance_id is required for fleet capabilities")
                self._capabilities[instance_id] = await self._request(
                    "GET", self._gateway_path(f"/instances/{instance_id}/capabilities")
                )
            except APIError as exc:
                if exc.status_code == 426:
                    self._capabilities.pop(instance_id, None)
                raise
        return self._capabilities[instance_id]

    async def ensure_operation(self, operation_id: str, *, instance_id: Optional[str] = None) -> None:
        caps = await self.capabilities(instance_id)
        # PR-017: the gateway's capability endpoint advertises supported fleet
        # operations under `supported_fleet_operations`; also accept the
        # canonical instance's own `operations` list (wrapped under
        # `capabilities`).
        advertised = caps.get("supported_fleet_operations")
        if advertised is None and isinstance(caps.get("capabilities"), dict):
            advertised = caps["capabilities"].get("operations") or caps["capabilities"].get("operation_ids")
        if advertised is None:
            advertised = caps.get("operations") or caps.get("operation_ids") or caps.get("supported_operations")
        if advertised is None:
            return  # older servers do not expose the optional operation list
        if isinstance(advertised, dict): advertised = advertised.keys()
        if operation_id not in advertised:
            raise UnsupportedOperationError(operation_id)

    def for_instance(self, instance_id: str) -> "FleetSessionTransport":
        """Return the transport-neutral session backend for one instance."""
        if not instance_id:
            raise ValueError("instance_id is required")
        return FleetSessionTransport(self, instance_id)

    async def call(self, method: str, path: str, *, instance_id: Optional[str] = None,
                   body: Any = None, query: Optional[Dict[str, Any]] = None,
                   operation_id: Optional[str] = None,
                   confirm_target: Optional[str] = None, operation_manifest_id: Optional[str] = None) -> Any:
        """Invoke any operation from the checked-in parity manifest.

        This is intentionally a typed transport escape hatch for newly added
        manifest operations; resource helpers above remain the preferred API.
        """
        from urllib.parse import urlencode
        if query:
            path += "?" + urlencode(query, doseq=True)
        destructive = method.upper() == "DELETE" or path.rstrip("/").endswith("/cancel")
        if destructive and not confirm_target:
            raise ValueError("confirm_target is required for destructive fleet operations")
        if operation_manifest_id:
            await self.ensure_operation(operation_manifest_id, instance_id=instance_id)
        headers: Dict[str, str] = {}
        if operation_id: headers["Idempotency-Key"] = operation_id
        if confirm_target: headers["X-Actae-Confirm-Target"] = confirm_target
        return await self._request(method, path, instance_id=instance_id, body=body, headers=headers)

    async def call_operation(self, operation_id: str, *, instance_id: Optional[str] = None,
                             path_params: Optional[Dict[str, Any]] = None, body: Any = None,
                             query: Optional[Dict[str, Any]] = None,
                             operation_idempotency_key: Optional[str] = None,
                             confirm_target: Optional[str] = None) -> Any:
        """Invoke a manifest operation using its checked-in method/path metadata.

        This is the typed escape hatch for operations added after a named SDK
        helper.  ``path_params`` is required for every ``{placeholder}`` in
        the manifest path; unknown/missing parameters fail before network I/O.
        """
        op: ManifestOperation = manifest_operation(operation_id)
        path = op.path
        values = path_params or {}
        import re
        names = set(re.findall(r"\{([^}]+)\}", path))
        missing = names - set(values)
        if missing:
            raise ValueError(f"missing path parameters for {operation_id}: {sorted(missing)}")
        if set(values) - names:
            raise ValueError(f"unknown path parameters for {operation_id}: {sorted(set(values)-names)}")
        for name, value in values.items():
            from urllib.parse import quote
            path = path.replace("{" + name + "}", quote(str(value), safe=""))
        if not instance_id:
            raise ValueError(f"instance_id is required for fleet operation {operation_id}")
        # PR-016: auth/session/dashboard operations are NOT fleet commands.
        # Fail locally with UnsupportedOperationError before any network I/O.
        if not op.fleet_supported:
            raise UnsupportedOperationError(operation_id)
        # Confirmation is manifest-driven (not hardcoded to DELETE).
        if op.confirmation and not confirm_target:
            raise ValueError(f"confirm_target is required for {operation_id}")
        # The gateway's single typed dispatch route is POST-only; it resolves
        # the canonical method/path from the manifest and forwards this
        # envelope to the instance. This is what keeps all 75 operations on
        # one contract without exposing a generic customer-controlled proxy.
        # Keep path parameters and query parameters distinct on the wire. The
        # gateway still accepts the legacy combined `params` field, but the
        # split form prevents a POST request's filters from being mistaken for
        # path values and makes URL/body rendering deterministic.
        envelope = {"path_params": values, "query": query or {}, "body": body if body is not None else {}}
        headers: Dict[str, str] = {}
        if op.action != "read" and not operation_idempotency_key: operation_idempotency_key = str(uuid.uuid4())
        if operation_idempotency_key: headers["Idempotency-Key"] = operation_idempotency_key
        if confirm_target: headers["X-Actae-Confirm-Target"] = confirm_target
        return await self._request("POST", self._gateway_path(f"/instances/{instance_id}/ops/{operation_id}"), body=envelope, instance_id=None, headers=headers)

    async def record(self, instance_id: str, channel_id: str, event_type: str,
                     payload: Any, *, operation_id: Optional[str] = None) -> Any:
        body = {"channel_id": channel_id, "event_type": event_type, "payload": payload}
        if operation_id:
            body["operation_id"] = operation_id
        return await self._request("POST", self._gateway_path(f"/instances/{instance_id}/events"), body=body)

    async def query(self, instance_id: str, **params: Any) -> Any:
        """Run the canonical POST query operation on one fleet instance."""
        return await self._request("POST", self._gateway_path("/events/query"),
                                   body={"instances": [instance_id], "query": {k: v for k, v in params.items() if v is not None}})

    async def query_page(self, instance_id: str, *, page_token: Optional[str] = None,
                         limit: int = 100, **params: Any) -> Page:
        """Query one instance using the canonical opaque keyset token.

        PR-008: continuation tokens are sent as the top-level `page_tokens`
        map (instance_id -> opaque token) — never inside the `query` object.
        """
        body: Dict[str, Any] = {"instances": [instance_id],
                                "query": {k: v for k, v in params.items() if v is not None and k != "limit"}}
        # PR-007: keyset pagination must be explicit so even a tokenless FIRST
        # page returns a continuation token (legacy offset mode cannot start).
        body["query"].setdefault("pagination", "keyset")
        if limit is not None:
            body["query"]["limit"] = limit
        if page_token is not None:
            body["page_tokens"] = {instance_id: page_token}
        result = await self._request("POST", self._gateway_path("/events/query"), body=body)
        page = result.get("page") or {}
        tokens = page.get("next_page_tokens") or {}
        token = tokens.get(instance_id) if isinstance(tokens, dict) else None
        if token is None:
            token = result.get("next_page_token")
        return Page(list(result.get("events", result.get("items", []))), token)

    async def iter_query(self, instance_id: str, *, limit: int = 100, **params: Any) -> AsyncIterator[Any]:
        """Iterate every event page while preserving opaque server tokens."""
        token: Optional[str] = None
        while True:
            page = await self.query_page(instance_id, page_token=token, limit=limit, **params)
            for item in page.items:
                yield item
            if not page.next_page_token: return
            token = page.next_page_token

    async def query_many(self, *, instance_ids: list[str], page_tokens: Optional[Dict[str, str]] = None,
                         limit: int = 100, **params: Any) -> PartialResult:
        """Ask the gateway for a partial-aware multi-instance event page.

        PR-008: selectors are `instances`, filters are `query`, and the
        per-instance continuation map is the top-level `page_tokens` object.
        The returned page token is the FULL `next_page_tokens` map — never a
        single-instance collapse.
        """
        body: Dict[str, Any] = {
            "instances": instance_ids,
            "query": {k: v for k, v in params.items() if v is not None and k != "limit"},
        }
        if limit is not None:
            body["query"]["limit"] = limit
        if page_tokens:
            body["page_tokens"] = page_tokens
        result = await self._request("POST", self._gateway_path("/events/query"), body=body)
        errors = [InstanceError(e.get("instance_id", ""), e.get("code", e.get("error", "unknown")),
                                e.get("message", e.get("error", "")), bool(e.get("retryable", True)))
                  for e in result.get("errors", [])]
        page = result.get("page") or {}
        tokens = page.get("next_page_tokens") or {}
        return PartialResult(list(result.get("events", result.get("items", []))), errors,
                             page_tokens=dict(tokens) if isinstance(tokens, dict) and tokens else None)

    async def replay(self, instance_id: str, channel_id: str, **params: Any) -> Any:
        return await self.call_operation("actae.api.v1.events.replay", instance_id=instance_id,
                                         path_params={"channel_id": channel_id},
                                         query={k: v for k, v in params.items() if v is not None})

    async def fork(self, instance_id: str, source_channel_id: str, new_channel_id: str,
                   at_cursor: int, *, operation_id: Optional[str] = None, **extra: Any) -> Any:
        body = {"source_channel_id": source_channel_id, "new_channel_id": new_channel_id,
                "at_cursor": at_cursor, **extra}
        if operation_id:
            body["operation_id"] = operation_id
        headers = {"Idempotency-Key": operation_id or str(uuid.uuid4())}
        return await self._request("POST", self._gateway_path(f"/instances/{instance_id}/forks"), body=body, headers=headers)

    async def save_state(self, instance_id: str, channel_id: str, cursor: int, state: Any, **guards: Any) -> Any:
        headers = {"Idempotency-Key": str(guards.pop("operation_id", uuid.uuid4()))}
        return await self._request("PUT", self._gateway_path(f"/instances/{instance_id}/state/{channel_id}"),
                                   body={"channel_id": channel_id, "cursor": cursor, "state": state, **guards}, headers=headers)

    async def latest_state(self, instance_id: str, channel_id: str) -> Any:
        return await self.call_operation("actae.api.v1.state.load", instance_id=instance_id,
                                         path_params={"channel_id": channel_id})

    async def stream(self, *, instance_id: Optional[str] = None,
                     ticket: Optional[str] = None,
                     ticket_provider: Optional[Any] = None,
                     resume_cursor: Optional[int] = None,
                     reconnect: bool = False,
                     max_reconnect_attempts: int = 5,
                     reconnect_delay: float = 0.25,
                     **params: Any) -> AsyncIterator[Any]:
        """Yield fleet events with bounded deduplication and resumable reconnect.

        Hosted gateway streams use a one-use, Origin-bound ``ticket``. A
        reconnect therefore *must* obtain a fresh ticket through
        ``ticket_provider``; the last observed cursor is sent as
        ``resume_cursor`` so canonical subscribe-with-cursor closes the gap.
        The legacy NDJSON endpoint remains available when no ticket is given.
        """
        from urllib.parse import urlencode
        if reconnect and ticket is None and ticket_provider is None:
            raise ValueError("ticket or ticket_provider is required for gateway streams")
        if ticket is not None or ticket_provider is not None:
            attempts = 0
            seen: set[tuple[Any, Any]] = set()
            cursor = resume_cursor
            while True:
                if ticket is None:
                    if ticket_provider is None:
                        raise ValueError("ticket or ticket_provider is required for gateway streams")
                    # Pass the latest durable cursor so the control plane can
                    # mint a ticket scoped to a precise resume boundary. A
                    # zero-argument provider remains accepted for backwards
                    # compatibility with pre-resume clients.
                    try:
                        import inspect
                        accepts_cursor = len([
                            p for p in inspect.signature(ticket_provider).parameters.values()
                            if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD)
                        ]) > 0
                    except (TypeError, ValueError):
                        accepts_cursor = True
                    supplied = ticket_provider(cursor) if accepts_cursor else ticket_provider()
                    ticket = await supplied if hasattr(supplied, "__await__") else supplied
                qargs = {k: v for k, v in params.items() if v is not None}
                qargs["ticket"] = ticket
                if cursor is not None: qargs["resume_cursor"] = cursor
                url = self.endpoint + self._gateway_path("/events/stream") + "?" + urlencode(qargs, doseq=True)
                if self._session is None: self._session = aiohttp.ClientSession()
                try:
                    async with self._session.ws_connect(url, timeout=self.timeout) as ws:
                        ticket = None
                        attempts = 0
                        async for message in ws:
                            if message.type != aiohttp.WSMsgType.TEXT: continue
                            import json
                            item = json.loads(message.data)
                            if isinstance(item, dict):
                                event = item.get("event") if isinstance(item.get("event"), dict) else item
                                broadcast = item.get("Broadcast") or item.get("broadcast")
                                if isinstance(broadcast, dict) and isinstance(broadcast.get("payload"), dict) and isinstance(broadcast["payload"].get("event"), dict):
                                    event = broadcast["payload"]["event"]
                                if isinstance(event, dict):
                                    if isinstance(event.get("cursor"), int): cursor = event["cursor"]
                                    event_id = event.get("event_id", event.get("id"))
                                    source = event.get("instance_id", instance_id)
                                    if event_id is not None:
                                        key = (source, event_id)
                                        if key in seen: continue
                                        seen.add(key)
                                        if len(seen) > 10000: seen.clear()
                            yield item
                except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                    if not reconnect or attempts >= max_reconnect_attempts: raise ActaeConnectionError(str(exc)) from exc
                    attempts += 1
                    await asyncio.sleep(min(30.0, reconnect_delay * (2 ** (attempts - 1))))
                    continue
                if not reconnect: return
                if attempts >= max_reconnect_attempts: return
                attempts += 1
                await asyncio.sleep(min(30.0, reconnect_delay * (2 ** (attempts - 1))))
            return

        token = await self._token()
        q = urlencode({k: v for k, v in params.items() if v is not None}, doseq=True)
        url = self.endpoint + "/api/v1/fleet/stream" + ("?" + q if q else "")
        hdr = {"Authorization": f"Bearer {token}"}
        if instance_id: hdr["X-Actae-Instance-Id"] = instance_id
        if self._session is None:
            self._session = aiohttp.ClientSession()
        seen: set[tuple[Any, Any]] = set()
        async with self._session.get(url, headers=hdr, timeout=None) as resp:
            if resp.status >= 400:
                raise APIError(resp.status, await resp.text())
            async for line in resp.content:
                if line.strip():
                    import json
                    item = json.loads(line)
                    if isinstance(item, dict):
                        event_id = item.get("event_id", item.get("id"))
                        source = item.get("instance_id", instance_id)
                        if event_id is not None:
                            key = (source, event_id)
                            if key in seen:
                                continue
                            seen.add(key)
                            if len(seen) > 10000:
                                seen.clear()
                    yield item


class FleetSessionTransport:
    """Direct-client shaped adapter used by ``AgentSession`` over fleet routing.

    It contains no session lifecycle logic: record/fork/resume decisions stay
    in ``session.py`` and therefore behave identically for direct and fleet
    execution.
    """
    def __init__(self, fleet: FleetClient, instance_id: str) -> None:
        self._fleet, self.instance_id = fleet, instance_id

    async def record(self, channel_id: str, event_type: str, payload: Any, *, actor: str,
                     agent_id: Optional[str] = None, user_id: Optional[str] = None,
                     metadata: Optional[Dict[str, Any]] = None, operation_id: Optional[str] = None,
                     step_number: Optional[int] = None, dependencies: Optional[list[Dict[str, str]]] = None) -> Event:
        body: Dict[str, Any] = {"channel_id": channel_id, "event_type": event_type, "payload": payload,
                                "metadata": {"actor": actor, "agent_id": agent_id, "user_id": user_id, "metadata": metadata}}
        if operation_id is not None: body["operation_id"] = operation_id
        if step_number is not None: body["step_number"] = step_number
        if dependencies: body["dependencies"] = dependencies
        data = await self._fleet.call_operation("actae.api.v1.events.record", instance_id=self.instance_id,
                                                body=body, operation_idempotency_key=operation_id or str(uuid.uuid4()))
        data = self._fleet._unwrap_gateway(data)
        event = Event.from_record(data["event"])
        event.metadata, event.agent_id, event.user_id = metadata, agent_id, user_id
        return event

    async def replay(self, channel_id: str, *, cursor: Optional[int] = None, limit: int = 100,
                     event_type: Optional[str] = None) -> list[Event]:
        result = await self._fleet.call_operation("actae.api.v1.events.replay", instance_id=self.instance_id,
                                                  path_params={"channel_id": channel_id},
                                                  query={"cursor": cursor, "limit": limit, "event_type": event_type})
        result = self._fleet._unwrap_gateway(result)
        return [Event.from_replay(item) for item in result.get("events", [])]

    async def save_state(self, channel_id: str, cursor: int, state: Any, **guards: Any) -> int:
        result = await self._fleet.save_state(self.instance_id, channel_id, cursor, state, **guards)
        result = self._fleet._unwrap_gateway(result)
        return int(result["version"])

    async def latest_state(self, channel_id: str) -> Optional[Dict[str, Any]]:
        result = await self._fleet.call_operation("actae.api.v1.state.load", instance_id=self.instance_id,
                                                  path_params={"channel_id": channel_id})
        result = self._fleet._unwrap_gateway(result)
        if result.get("cursor") is None: return None
        return {"cursor": result["cursor"], "state": _unwrap_sonic(result["state"])}

    async def fork(self, source_channel_id: str, new_channel_id: str, at_cursor: int = 0, **opts: Any) -> ForkReceipt:
        result = await self._fleet.fork(self.instance_id, source_channel_id, new_channel_id, at_cursor, **opts)
        return ForkReceipt.from_dict(self._fleet._unwrap_gateway(result))

    async def get_channel_metadata(self, channel_id: str) -> Optional[ChannelMetadata]:
        try:
            data = self._fleet._unwrap_gateway(await self._fleet.call_operation("actae.api.v1.channels.metadata.get", instance_id=self.instance_id, path_params={"channel_id": channel_id}))
            return ChannelMetadata.from_dict(data)
        except APIError as exc:
            if exc.status_code == 404: return None
            raise

    async def update_metadata(self, channel_id: str, **opts: Any) -> ChannelMetadata:
        data = self._fleet._unwrap_gateway(await self._fleet.call_operation("actae.api.v1.channels.metadata.put", instance_id=self.instance_id,
                                                path_params={"channel_id": channel_id}, body={"channel_id": channel_id, **{k: v for k, v in opts.items() if v is not None}}))
        return ChannelMetadata.from_dict(data)

    async def resolve_step(self, channel_id: str, step_number: int) -> Optional[tuple[str, int, Optional[str]]]:
        try:
            data = self._fleet._unwrap_gateway(await self._fleet.call_operation("actae.api.v1.channels.steps.resolve", instance_id=self.instance_id,
                                                    path_params={"channel_id": channel_id, "step_number": step_number}))
            return (data["channel_id"], data["cursor"], data.get("event_id"))
        except APIError as exc:
            if exc.status_code == 404: return None
            raise

    async def latest_step_number(self, channel_id: str) -> Optional[int]:
        try:
            data = self._fleet._unwrap_gateway(await self._fleet.call_operation(
                "actae.api.v1.channels.steps.latest", instance_id=self.instance_id,
                path_params={"channel_id": channel_id}))
            return int(data.get("last_step_number", 0))
        except APIError as exc:
            if exc.status_code == 404:
                return None
            raise

    async def set_outcome(self, channel_id: str, outcome: str, *, score: Optional[float] = None) -> Any:
        body: Dict[str, Any] = {"outcome": outcome}
        if score is not None: body["score"] = score
        return await self._fleet.call_operation("actae.api.v1.channels.outcome.set", instance_id=self.instance_id,
                                                path_params={"channel_id": channel_id}, body=body)

    async def promote_channel(self, channel_id: str) -> Any:
        return await self._fleet.call_operation("actae.api.v1.forks.promote", instance_id=self.instance_id,
                                                path_params={"channel_id": channel_id})

    async def create_experiment(self, name: str, *, description: Optional[str] = None,
                                baseline_channel_id: Optional[str] = None) -> Any:
        body: Dict[str, Any] = {"name": name}
        if description: body["description"] = description
        if baseline_channel_id: body["baseline_channel_id"] = baseline_channel_id
        return await self._fleet.call_operation("actae.api.v1.experiments.create", instance_id=self.instance_id, body=body)

    async def add_experiment_member(self, group_id: str, channel_id: str, *, role: str = "variant",
                                    declared_delta: Optional[Dict[str, Any]] = None) -> Any:
        body: Dict[str, Any] = {"channel_id": channel_id, "role": role}
        if declared_delta: body["declared_delta"] = declared_delta
        return await self._fleet.call_operation("actae.api.v1.experiments.members.add", instance_id=self.instance_id,
                                                path_params={"group_id": group_id}, body=body)
