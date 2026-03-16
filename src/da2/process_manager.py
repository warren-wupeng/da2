"""Process Manager -- orchestrates multi-aggregate workflows.

A Process Manager listens to domain events, maintains internal state, and
issues commands to drive a long-running business process forward.

Convention: define ``_on_<EventClassName>`` methods to handle events.
Call ``_dispatch(command)`` inside handlers to queue commands for execution.
Call ``_mark_completed()`` when the process reaches its terminal state.

Example::

    from da2 import Command, Event
    from da2.process_manager import ProcessManager

    class ReserveStock(Command):
        def __init__(self, order_id: str): self.order_id = order_id

    class ChargePayment(Command):
        def __init__(self, order_id: str, amount: float):
            self.order_id = order_id
            self.amount = amount

    class OrderPlaced(Event):
        def __init__(self, order_id: str, amount: float):
            self.order_id = order_id
            self.amount = amount

    class StockReserved(Event):
        def __init__(self, order_id: str): self.order_id = order_id

    class OrderFulfillment(ProcessManager):
        def __init__(self, process_id: str):
            super().__init__(process_id)
            self.amount = 0.0

        def _on_OrderPlaced(self, event: OrderPlaced):
            self.amount = event.amount
            self._dispatch(ReserveStock(order_id=event.order_id))

        def _on_StockReserved(self, event: StockReserved):
            self._dispatch(ChargePayment(order_id=event.order_id, amount=self.amount))
            self._mark_completed()

    pm = OrderFulfillment("order-1")
    cmds = pm.handle(OrderPlaced(order_id="order-1", amount=99.0))
    # cmds == [ReserveStock(order_id="order-1")]
"""

from __future__ import annotations

import abc

from .command import Command
from .event import Event


class ProcessManager(abc.ABC):
    """Base class for synchronous process managers.

    Subclasses define ``_on_<EventClassName>`` methods to react to events.
    Inside each handler, call ``self._dispatch(cmd)`` to queue commands
    and ``self._mark_completed()`` to signal the process is done.

    ``handle(event)`` routes the event, then returns and clears the
    pending command queue.
    """

    def __init__(self, process_id: str) -> None:
        self._process_id = process_id
        self._pending_commands: list[Command] = []
        self._completed: bool = False

    @property
    def process_id(self) -> str:
        """Correlation identifier for this process instance."""
        return self._process_id

    @property
    def pending_commands(self) -> list[Command]:
        """Snapshot of commands queued but not yet drained."""
        return list(self._pending_commands)

    @property
    def completed(self) -> bool:
        """Whether the process has reached its terminal state."""
        return self._completed

    def handle(self, event: Event) -> list[Command]:
        """Route *event* to the matching handler and return queued commands.

        Returns the list of commands dispatched during handling,
        then clears the internal queue.
        """
        handler = getattr(self, f"_on_{type(event).__name__}", None)
        if handler is not None:
            handler(event)
        cmds = list(self._pending_commands)
        self._pending_commands.clear()
        return cmds

    def _dispatch(self, command: Command) -> None:
        """Queue a command to be issued after the current event is processed."""
        self._pending_commands.append(command)

    def _mark_completed(self) -> None:
        """Mark this process as finished."""
        self._completed = True
