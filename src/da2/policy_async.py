"""Async Policy -- async variant of Policy.

Supports both ``async def _on_*`` and plain ``def _on_*`` handlers.

Example::

    from da2.policy_async import PolicyAsync

    class AsyncWelcomePolicy(PolicyAsync):
        async def _on_UserRegistered(self, event):
            await validate_email(event.email)
            self._dispatch(SendWelcomeEmail(email=event.email))
"""

from __future__ import annotations

import asyncio

from .command import Command
from .event import Event


class PolicyAsync:
    """Async stateless event-to-command reactor.

    Same convention as :class:`Policy`: define ``_on_<EventClassName>``
    handlers (async or sync) and call ``_dispatch(cmd)``.
    """

    def __init__(self) -> None:
        self._pending_commands: list[Command] = []

    async def handle(self, event: Event) -> list[Command]:
        """Route *event* to handler (async or sync) and return queued commands."""
        handler = getattr(self, f"_on_{type(event).__name__}", None)
        if handler is not None:
            if asyncio.iscoroutinefunction(handler):
                await handler(event)
            else:
                handler(event)
        cmds = list(self._pending_commands)
        self._pending_commands.clear()
        return cmds

    def _dispatch(self, command: Command) -> None:
        self._pending_commands.append(command)
