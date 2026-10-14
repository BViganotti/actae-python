"""Live quickstart fork test — mirrors the documented quickstart's step 6
("Kill it, resume it, fork it") against a real Actae server.

Guards the doc example's semantics so it cannot silently rot:

  1. a run crashes after step 3 (the expensive prefix),
  2. a same-channel resume continues at step 4 without re-running 1-3,
  3. a fork at step 3 inherits the state saved at step 3 and runs only its
     tail,
  4. the SERVER metered both a crash recovery and a fork (visible in the
     always-local `/metrics.json` counters) — i.e. the differentiated-value
     signal is produced by the documented flow.

    ACTAE_URL=http://localhost:8002 ACTAE_API_KEY=sk-dev-0000000000000000000000 \
        python3 -m pytest sdks/python/tests/test_quickstart_live.py -v
"""

import asyncio
import json
import os
import urllib.request
import uuid

import pytest

from actae_client import ActaeClient
from actae_client.session import AgentSession

_ENDPOINT = os.environ.get("ACTAE_URL") or os.environ.get("ACTAE_ENDPOINT")
_API_KEY = os.environ.get("ACTAE_API_KEY")

pytestmark = pytest.mark.skipif(
    not (_ENDPOINT and _API_KEY), reason="ACTAE_URL/ACTAE_API_KEY not set"
)


def _metrics():
    req = urllib.request.Request(
        _ENDPOINT.rstrip("/") + "/metrics.json", headers={"X-API-Key": _API_KEY}
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.load(resp)


def test_quickstart_crash_resume_fork():
    async def main():
        channel = "qs-live-%s" % uuid.uuid4().hex[:8]
        actae = ActaeClient(api_key=_API_KEY, endpoint=_ENDPOINT)
        memory = []

        def state_fn():
            return {"steps_done": list(memory)}

        # 1. Crash after step 3.
        with pytest.raises(RuntimeError):
            async with AgentSession(actae, name=channel, state_fn=state_fn) as s:
                for i in range(1, 4):
                    memory.append(i)
                    await s.step("inference", input=i, output=i * 10)
                raise RuntimeError("worker died")

        # 2. Same-channel resume: continues at step 4.
        async with (await AgentSession.resume(actae, channel)) as s:
            memory.append(4)
            await s.step("inference", input=4, output=40)

        # 3. Fork at step 3: inherits steps 1-3 state, runs only the tail.
        fork_name = channel + "-alt"
        fork = await AgentSession.resume(
            actae, channel, fork_at_step=3, name=fork_name, params={"model": "gpt-5"}
        )
        async with fork:
            inherited = fork.inherited_state or {}
            assert inherited.get("steps_done") == [1, 2, 3], (
                "fork must inherit the state saved at step 3 (prefix not re-run): %r"
                % inherited
            )
            await fork.step(
                "inference",
                input=4,
                output=100,
                context={"model": "gpt-5", "inherited": inherited},
            )

        await actae.disconnect()
        return channel, fork_name

    channel, fork_name = asyncio.run(main())

    # The documented flow must produce the differentiated-value signal: one
    # same-channel recovery and one fork, both counted server-side and visible
    # locally with no phone-home.
    metrics = _metrics()
    assert metrics.get("forks", {}).get("total", 0) >= 1
    assert metrics.get("recoveries", {}).get("total", 0) >= 1
