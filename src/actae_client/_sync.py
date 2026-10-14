"""Synchronous facade support — call async ActaeClient methods from sync code.

Each calling thread gets its own daemon background event loop; coroutines are
submitted to it and the caller blocks until the result is ready. Sessions and
connectors are created lazily inside that loop (see ``client._request``'s
per-loop session cache), so sync and async use of the same client never
share loop-bound resources.

Usage (via ``ActaeClient.record_sync`` and friends):

    client = ActaeClient(endpoint="http://localhost:8002", api_key="sk-...")
    event = client.record_sync("my-channel", "agent.step", {...}, actor="me")
    events = client.replay_sync("my-channel")
"""

import asyncio
import threading
from typing import Any, Optional


class _SyncLoop:
    """Per-thread background event loop."""

    _local = threading.local()

    @classmethod
    def get(cls) -> asyncio.AbstractEventLoop:
        loop: Optional[asyncio.AbstractEventLoop] = getattr(cls._local, "loop", None)
        if loop is None or loop.is_closed():
            loop = asyncio.new_event_loop()
            threading.Thread(
                target=loop.run_forever,
                name="actae-sync-loop",
                daemon=True,
            ).start()
            cls._local.loop = loop
        return loop


def run_sync(awaitable: Any) -> Any:
    """Run *awaitable* on the caller's background loop and block for its result."""
    loop = _SyncLoop.get()
    future = asyncio.run_coroutine_threadsafe(awaitable, loop)
    return future.result()
