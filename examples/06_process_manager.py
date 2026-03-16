"""Example 06 -- Process Manager (Saga).

A Process Manager orchestrates a multi-step workflow across aggregates
by reacting to events and issuing commands.

This example shows an order fulfillment process:
  OrderPlaced -> ReserveStock -> StockReserved -> ChargePayment -> done
  OrderPlaced -> ReserveStock -> StockUnavailable -> RefundPayment -> done
"""

from __future__ import annotations

from da2 import Command, Event
from da2.process_manager import ProcessManager


# ── Commands ──────────────────────────────────────────────────────

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


class ShipOrder(Command):
    def __init__(self, order_id: str):
        self.order_id = order_id


# ── Events ────────────────────────────────────────────────────────

class OrderPlaced(Event):
    def __init__(self, order_id: str, amount: float):
        self.order_id = order_id
        self.amount = amount


class StockReserved(Event):
    def __init__(self, order_id: str):
        self.order_id = order_id


class StockUnavailable(Event):
    def __init__(self, order_id: str):
        self.order_id = order_id


class PaymentCharged(Event):
    def __init__(self, order_id: str):
        self.order_id = order_id


# ── Process Manager ───────────────────────────────────────────────

class OrderFulfillment(ProcessManager):
    """Orchestrates: place -> reserve stock -> charge -> ship."""

    def __init__(self, process_id: str):
        super().__init__(process_id)
        self.amount = 0.0

    def _on_OrderPlaced(self, event: OrderPlaced):
        self.amount = event.amount
        self._dispatch(ReserveStock(order_id=event.order_id))

    def _on_StockReserved(self, event: StockReserved):
        self._dispatch(ChargePayment(order_id=event.order_id, amount=self.amount))

    def _on_StockUnavailable(self, event: StockUnavailable):
        self._dispatch(RefundPayment(order_id=event.order_id))
        self._mark_completed()

    def _on_PaymentCharged(self, event: PaymentCharged):
        self._dispatch(ShipOrder(order_id=event.order_id))
        self._mark_completed()


# ── Demo ──────────────────────────────────────────────────────────

def main():
    # Happy path
    print("=== Happy path ===")
    pm = OrderFulfillment("order-1")

    cmds = pm.handle(OrderPlaced(order_id="order-1", amount=99.0))
    print(f"Step 1: {[type(c).__name__ for c in cmds]}")
    # -> ['ReserveStock']

    cmds = pm.handle(StockReserved(order_id="order-1"))
    print(f"Step 2: {[type(c).__name__ for c in cmds]}")
    # -> ['ChargePayment']

    cmds = pm.handle(PaymentCharged(order_id="order-1"))
    print(f"Step 3: {[type(c).__name__ for c in cmds]}")
    print(f"Completed: {pm.completed}")
    # -> ['ShipOrder'], Completed: True

    # Compensation path
    print("\n=== Compensation path ===")
    pm2 = OrderFulfillment("order-2")

    cmds = pm2.handle(OrderPlaced(order_id="order-2", amount=50.0))
    print(f"Step 1: {[type(c).__name__ for c in cmds]}")

    cmds = pm2.handle(StockUnavailable(order_id="order-2"))
    print(f"Compensate: {[type(c).__name__ for c in cmds]}")
    print(f"Completed: {pm2.completed}")
    # -> ['RefundPayment'], Completed: True

    print("\nDone.")


if __name__ == "__main__":
    main()
