"""
LangChain State Adapter for Actae.

Automatically saves agent context (messages, tool outputs, chain metadata)
as cursor-aligned state snapshots. Includes a ChainResumer for one-line
restart with full context recovery.

Usage:
    from actae_client.adapters.langchain import ActaeContextSaver, ChainResumer

    saver = ActaeContextSaver(actae_client, channel="my-chain")
    chain.invoke(input, config={"callbacks": [saver]})

    # On restart:
    resumer = ChainResumer(actae_client, channel="my-chain")
    response = await resumer.resume(chain, default_input)
"""

from typing import Any, Dict, Optional

from .._utils import serialize
from ..client import ActaeClient
from .contract import adapter_checkpoint_state

try:
    from langchain_core.callbacks.base import BaseCallbackHandler as _LCBaseCallbackHandler
    _HAS_LANGCHAIN = True
except ImportError:
    _LCBaseCallbackHandler = object
    _HAS_LANGCHAIN = False


class ActaeContextSaver(_LCBaseCallbackHandler):
    """Saves full agent context as cursor-aligned state snapshots.

    Captures messages, tool outputs, chain metadata, and LLM responses
    at each step so the agent can resume with full context.
    """

    def __init__(self, actae: ActaeClient, channel: str = "langchain", save_every_n: int = 3):
        """Create a LangChain context saver callback handler.

        Args:
            actae: Connected ActaeClient instance.
            channel: Channel ID for context storage (default ``"langchain"``).
            save_every_n: Save context snapshot every N callbacks (default 3).
        """
        super().__init__()
        self.actae = actae
        self.channel = channel
        self.save_every_n = save_every_n
        self._context: Dict[str, Any] = {
            "messages": [],
            "tool_outputs": [],
            "chain_steps": [],
        }
        self._step_count = 0

    def on_llm_end(
        self,
        response: Any,
        *,
        run_id: Any = None,
        parent_run_id: Any = None,
        **kwargs: Any,
    ) -> None:
        self._step_count += 1
        response_text = ""
        try:
            if hasattr(response, "generations") and response.generations:
                for gen_list in response.generations:
                    for gen in gen_list:
                        text = getattr(gen, "text", None)
                        if isinstance(text, str) and text:
                            response_text += text
                        else:
                            message = getattr(gen, "message", None)
                            if message is not None:
                                response_text += str(message)
        except Exception:
            response_text = str(response)[:500]

        self._context["messages"].append({
            "role": "assistant",
            "content": response_text,
            "run_id": str(run_id) if run_id else None,
        })
        self._maybe_save()

    def on_tool_end(
        self,
        output: Any,
        *,
        run_id: Any = None,
        parent_run_id: Any = None,
        **kwargs: Any,
    ) -> None:
        self._step_count += 1
        self._context["tool_outputs"].append({
            "output": serialize(output),
            "run_id": str(run_id) if run_id else None,
        })
        self._maybe_save()

    def on_chain_end(
        self,
        outputs: Any,
        *,
        run_id: Any = None,
        parent_run_id: Any = None,
        **kwargs: Any,
    ) -> None:
        self._context["chain_steps"].append({
            "outputs": serialize(outputs),
            "run_id": str(run_id) if run_id else None,
        })
        self._save_now()

    async def load_context(self) -> Optional[Dict[str, Any]]:
        snapshot = await self.actae.latest_state(self.channel)
        if snapshot is None:
            return None
        return snapshot["state"].get("context", {})

    def _maybe_save(self) -> None:
        if self._step_count % self.save_every_n == 0:
            self._save_now()

    def _save_now(self) -> None:
        if not self._actae_available():
            return
        try:
            import asyncio
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        loop.create_task(self._do_save())

    async def _do_save(self) -> None:
        cursor = await self.actae.latest_cursor(self.channel) or 0
        state = {"context": self._context}
        await self.actae.save_state(
            self.channel,
            cursor,
            adapter_checkpoint_state(
                state,
                framework="langchain",
                channel_id=self.channel,
                portable_state={
                    "message_count": len(self._context.get("messages", [])),
                    "tool_output_count": len(self._context.get("tool_outputs", [])),
                    "chain_step_count": len(self._context.get("chain_steps", [])),
                },
                native_checkpoint={"resume": "context_seeded"},
                event_cursor=cursor,
            ),
        )

    def _actae_available(self) -> bool:
        return self.actae is not None


class ChainResumer:
    """Resume a LangChain chain from the last Actae state snapshot.

    Usage:
        resumer = ChainResumer(actae_client, channel="my-chain")
        response = await resumer.resume(chain, default_input)
    """

    def __init__(self, actae: ActaeClient, channel: str = "langchain"):
        """Create a LangChain chain resumer.

        Args:
            actae: Connected ActaeClient instance.
            channel: Channel ID for context storage (default ``"langchain"``).
        """
        self.actae = actae
        self.channel = channel
        self._saver = ActaeContextSaver(actae, channel)

    async def resume(
        self,
        chain: Any,
        default_input: Any = None,
    ) -> Any:
        """Run chain, seeded from the last saved context if available.

        If a previous context exists, the last assistant message is used as
        input. Otherwise, `default_input` is used. Returns the chain output.
        """
        context = await self._saver.load_context()
        if context is None or not context.get("messages"):
            return chain.invoke(default_input, config={"callbacks": [self._saver]})

        last_msg = context["messages"][-1]["content"] if context["messages"] else None
        input_val = last_msg or default_input
        return chain.invoke(input_val, config={"callbacks": [self._saver]})

    async def fork(
        self,
        new_channel: str,
        *,
        reason: Optional[str] = None,
    ) -> "ChainResumer":
        """Fork this chain's context into a new channel.

        Copies the latest context snapshot (messages, tool outputs, chain
        steps) into ``new_channel`` and returns a ``ChainResumer`` bound to
        it — the LangChain-native counterpart of ``StateManager.fork``. The
        fork's ``resume`` seeds from the inherited context, so you can refine
        a later step against the full prior conversation without re-running
        the earlier ones.

        Args:
            new_channel: Channel ID for the fork.
            reason: Optional human-readable reason recorded on the fork.

        Returns:
            A ``ChainResumer`` for the fork channel.

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
        return ChainResumer(self.actae, new_channel)
