"""Tests for ProcessManager."""

from da2 import Command, Event
from da2.process_manager import ProcessManager


# ── Fixtures ──────────────────────────────────────────────────────

class ReserveStock(Command):
    def __init__(self, order_id: str):
        self.order_id = order_id


class ChargePayment(Command):
    def __init__(self, order_id: str, amount: float):
        self.order_id = order_id
        self.amount = amount


class RefundPayment(Command):
    def __init__(self, order_id: str):
        self.order_id = order_id


class OrderPlaced(Event):
    def __init__(self, order_id: str, amount: float):
        self.order_id = order_id
        self.amount = amount


class StockReserved(Event):
    def __init__(self, order_id: str):
        self.order_id = order_id


class PaymentCharged(Event):
    def __init__(self, order_id: str):
        self.order_id = order_id


class StockUnavailable(Event):
    def __init__(self, order_id: str):
        self.order_id = order_id


class UnknownEvent(Event):
    pass


class OrderFulfillment(ProcessManager):
    """Multi-step order fulfillment process."""

    def __init__(self, process_id: str):
        super().__init__(process_id)
        self.amount = 0.0
        self.stock_reserved = False

    def _on_OrderPlaced(self, event: OrderPlaced):
        self.amount = event.amount
        self._dispatch(ReserveStock(order_id=event.order_id))

    def _on_StockReserved(self, event: StockReserved):
        self.stock_reserved = True
        self._dispatch(ChargePayment(order_id=event.order_id, amount=self.amount))

    def _on_PaymentCharged(self, event: PaymentCharged):
        self._mark_completed()

    def _on_StockUnavailable(self, event: StockUnavailable):
        self._dispatch(RefundPayment(order_id=event.order_id))
        self._mark_completed()


# ── Tests ─────────────────────────────────────────────────────────


class TestProcessManagerHandle:

    def test_handle_routes_event_and_returns_commands(self):
        pm = OrderFulfillment("p-1")
        cmds = pm.handle(OrderPlaced(order_id="o-1", amount=50.0))
        assert len(cmds) == 1
        assert isinstance(cmds[0], ReserveStock)
        assert cmds[0].order_id == "o-1"

    def test_handle_unknown_event_returns_empty(self):
        pm = OrderFulfillment("p-1")
        cmds = pm.handle(UnknownEvent())
        assert cmds == []

    def test_handle_clears_pending_after_return(self):
        pm = OrderFulfillment("p-1")
        pm.handle(OrderPlaced(order_id="o-1", amount=10.0))
        assert pm.pending_commands == []

    def test_state_is_maintained_across_events(self):
        pm = OrderFulfillment("p-1")
        pm.handle(OrderPlaced(order_id="o-1", amount=75.0))
        assert pm.amount == 75.0
        cmds = pm.handle(StockReserved(order_id="o-1"))
        assert len(cmds) == 1
        assert isinstance(cmds[0], ChargePayment)
        assert cmds[0].amount == 75.0


class TestProcessManagerLifecycle:

    def test_full_happy_path(self):
        pm = OrderFulfillment("p-1")
        assert not pm.completed

        cmds = pm.handle(OrderPlaced(order_id="o-1", amount=100.0))
        assert not pm.completed
        assert len(cmds) == 1

        cmds = pm.handle(StockReserved(order_id="o-1"))
        assert not pm.completed
        assert len(cmds) == 1

        cmds = pm.handle(PaymentCharged(order_id="o-1"))
        assert pm.completed
        assert cmds == []

    def test_compensation_path(self):
        pm = OrderFulfillment("p-1")
        pm.handle(OrderPlaced(order_id="o-1", amount=100.0))
        cmds = pm.handle(StockUnavailable(order_id="o-1"))
        assert pm.completed
        assert len(cmds) == 1
        assert isinstance(cmds[0], RefundPayment)

    def test_process_id(self):
        pm = OrderFulfillment("my-process")
        assert pm.process_id == "my-process"

    def test_multiple_dispatches_in_single_handler(self):
        """A handler can dispatch multiple commands at once."""

        class MultiDispatch(ProcessManager):
            def _on_OrderPlaced(self, event: OrderPlaced):
                self._dispatch(ReserveStock(order_id=event.order_id))
                self._dispatch(ChargePayment(order_id=event.order_id, amount=event.amount))

        pm = MultiDispatch("p-1")
        cmds = pm.handle(OrderPlaced(order_id="o-1", amount=42.0))
        assert len(cmds) == 2
        assert isinstance(cmds[0], ReserveStock)
        assert isinstance(cmds[1], ChargePayment)
