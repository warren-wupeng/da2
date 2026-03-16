"""Async Process Manager -- async variant of ProcessManager.

Supports both ``async def _on_*`` and plain ``def _on_*`` handlers.

Example::

    from da2.process_manager_async import ProcessManagerAsync

    class AsyncFulfillment(ProcessManagerAsync):
        async def _on_OrderPlaced(self, event):
            await some_async_check(event)
            self._dispatch(ReserveStock(order_id=event.order_id))
"""

from __future__ import annotations

import abc
import asyncio

from .command import Command
from .event import Event


class ProcessManagerAsync(abc.ABC):
    """Async base class for process managers.

    Same convention as :class:`ProcessManager`: define ``_on_<EventClassName>``
    handlers (async or sync) and call ``_dispatch`` / ``_mark_completed``.
    """

    def __init__(self, process_id: str) -> None:
        self._process_id = process_id
        self._pending_commands: list[Command] = []
        self._completed: bool = False

    @property
    def process_id(self) -> str:
        return self._process_id

    @property
    def pending_commands(self) -> list[Command]:
        return list(self._pending_commands)

    @property
    def completed(self) -> bool:
        return self._completed

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

    def _mark_completed(self) -> None:
        self._completed = True
