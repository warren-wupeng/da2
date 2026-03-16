"""Example 08 -- Full Integration.

Wires together all da2 building blocks in an e-commerce scenario:
  - EventSourcedEntity (Order aggregate)
  - EventStore + EventSourcedRepository
  - Projection (OrderDashboard read model)
  - ProcessManager (OrderFulfillment saga)
  - Policy (NotificationPolicy -- stateless reactor)
  - MessageBus + Bootstrap (command/event dispatch with DI)

Flow:
  1. PlaceOrder command -> Order aggregate emits OrderPlaced event
  2. OrderPlaced -> ProcessManager dispatches ReserveStock
  3. ReserveStock -> (simulated) emits StockReserved
  4. StockReserved -> ProcessManager dispatches ChargePayment
  5. ChargePayment -> (simulated) emits PaymentCharged
  6. PaymentCharged -> ProcessManager marks completed
  7. Policy sends notifications at each step
  8. Projection builds a read model throughout
"""

from __future__ import annotations

from da2 import (
    Command,
    Event,
    UnitOfWork,
    Bootstrap,
    StoredEvent,
)
from da2.event_sourced import EventSourcedEntity
from da2.event_store import InMemoryEventStore
from da2.event_sourced_repository import EventSourcedRepository
from da2.projection import Projection
from da2.process_manager import ProcessManager
from da2.policy import Policy


# ═══════════════════════════════════════════════════════════════════
# Domain Events
# ═══════════════════════════════════════════════════════════════════

class OrderPlaced(Event):
    def __init__(self, order_id: str, product: str, amount: float):
        self.order_id = order_id
        self.product = product
        self.amount = amount


class StockReserved(Event):
    def __init__(self, order_id: str):
        self.order_id = order_id


class PaymentCharged(Event):
    def __init__(self, order_id: str, amount: float):
        self.order_id = order_id
        self.amount = amount


# ═══════════════════════════════════════════════════════════════════
# Commands
# ═══════════════════════════════════════════════════════════════════

class PlaceOrder(Command):
    def __init__(self, order_id: str, product: str, amount: float):
        self.order_id = order_id
        self.product = product
        self.amount = amount


class ReserveStock(Command):
    def __init__(self, order_id: str):
        self.order_id = order_id


class ChargePayment(Command):
    def __init__(self, order_id: str, amount: float):
        self.order_id = order_id
        self.amount = amount


# ═══════════════════════════════════════════════════════════════════
# Event-Sourced Aggregate
# ═══════════════════════════════════════════════════════════════════

class Order(EventSourcedEntity[str]):
    def __init__(self, identity: str):
        super().__init__(identity)
        self.product = ""
        self.amount = 0.0
        self.status = "new"

    @classmethod
    def place(cls, order_id: str, product: str, amount: float) -> Order:
        order = cls(order_id)
        order._apply_and_record(OrderPlaced(order_id=order_id, product=product, amount=amount))
        return order

    def _when_OrderPlaced(self, event: OrderPlaced):
        self.product = event.product
        self.amount = event.amount
        self.status = "placed"

    def _when_StockReserved(self, event: StockReserved):
        self.status = "stock_reserved"

    def _when_PaymentCharged(self, event: PaymentCharged):
        self.status = "paid"


EVENT_REGISTRY = {
    "OrderPlaced": OrderPlaced,
    "StockReserved": StockReserved,
    "PaymentCharged": PaymentCharged,
}


# ═══════════════════════════════════════════════════════════════════
# Projection (CQRS Read Model)
# ═══════════════════════════════════════════════════════════════════

class OrderDashboard(Projection):
    """Read model tracking order count, total revenue, and statuses."""

    def __init__(self):
        super().__init__()
        self.order_count = 0
        self.total_revenue = 0.0
        self.statuses: dict[str, str] = {}

    def _on_OrderPlaced(self, event: OrderPlaced):
        self.order_count += 1
        self.statuses[event.order_id] = "placed"

    def _on_StockReserved(self, event: StockReserved):
        self.statuses[event.order_id] = "stock_reserved"

    def _on_PaymentCharged(self, event: PaymentCharged):
        self.total_revenue += event.amount
        self.statuses[event.order_id] = "paid"


# ═══════════════════════════════════════════════════════════════════
# Process Manager (Saga)
# ═══════════════════════════════════════════════════════════════════

class OrderFulfillment(ProcessManager):
    """Orchestrates: place -> reserve stock -> charge payment."""

    def __init__(self, process_id: str):
        super().__init__(process_id)
        self.amount = 0.0

    def _on_OrderPlaced(self, event: OrderPlaced):
        self.amount = event.amount
        self._dispatch(ReserveStock(order_id=event.order_id))

    def _on_StockReserved(self, event: StockReserved):
        self._dispatch(ChargePayment(order_id=event.order_id, amount=self.amount))

    def _on_PaymentCharged(self, event: PaymentCharged):
        self._mark_completed()


# ═══════════════════════════════════════════════════════════════════
# Policy (Stateless Reactor)
# ═══════════════════════════════════════════════════════════════════

class NotificationPolicy(Policy):
    """Sends notifications (simulated via print) for key events."""

    def _on_OrderPlaced(self, event: OrderPlaced):
        print(f"  [Notification] Order {event.order_id} placed: {event.product} ${event.amount}")

    def _on_PaymentCharged(self, event: PaymentCharged):
        print(f"  [Notification] Payment received for {event.order_id}: ${event.amount}")


# ═══════════════════════════════════════════════════════════════════
# Unit of Work + Command Handlers
# ═══════════════════════════════════════════════════════════════════

class AppUoW(UnitOfWork):
    def __init__(self):
        self.store = InMemoryEventStore()
        self.orders = EventSourcedRepository(
            event_store=self.store,
            entity_cls=Order,
            event_registry=EVENT_REGISTRY,
        )

    def _enter(self):
        pass

    def _commit(self):
        pass

    def rollback(self):
        pass


# Shared instances (in a real app these would be injected)
dashboard = OrderDashboard()
fulfillment = OrderFulfillment("saga-1")
notifications = NotificationPolicy()


def handle_place_order(cmd: PlaceOrder, uow: AppUoW):
    order = Order.place(cmd.order_id, cmd.product, cmd.amount)
    uow.orders.save(order)
    # Feed events to projection, process manager, and policy
    event = OrderPlaced(order_id=cmd.order_id, product=cmd.product, amount=cmd.amount)
    dashboard.apply(event)
    notifications.handle(event)
    saga_cmds = fulfillment.handle(event)
    # Execute commands from process manager
    for c in saga_cmds:
        bus.handle(c)


def handle_reserve_stock(cmd: ReserveStock, uow: AppUoW):
    print(f"  [Stock] Reserving stock for order {cmd.order_id}")
    # Simulate stock reservation success
    event = StockReserved(order_id=cmd.order_id)
    dashboard.apply(event)
    notifications.handle(event)
    saga_cmds = fulfillment.handle(event)
    for c in saga_cmds:
        bus.handle(c)


def handle_charge_payment(cmd: ChargePayment, uow: AppUoW):
    print(f"  [Payment] Charging ${cmd.amount} for order {cmd.order_id}")
    # Simulate payment success
    event = PaymentCharged(order_id=cmd.order_id, amount=cmd.amount)
    dashboard.apply(event)
    notifications.handle(event)
    saga_cmds = fulfillment.handle(event)
    for c in saga_cmds:
        bus.handle(c)


# ═══════════════════════════════════════════════════════════════════
# Bootstrap
# ═══════════════════════════════════════════════════════════════════

app_uow = AppUoW()
bootstrap_instance = Bootstrap(
    uow=app_uow,
    command_handlers={
        PlaceOrder: handle_place_order,
        ReserveStock: handle_reserve_stock,
        ChargePayment: handle_charge_payment,
    },
    event_handlers={},
)
bus = bootstrap_instance.create_message_bus()
bus.uow.seen = {}


# ═══════════════════════════════════════════════════════════════════
# Run
# ═══════════════════════════════════════════════════════════════════

def main():
    print("=== Place Order ===")
    bus.handle(PlaceOrder(order_id="ORD-001", product="Widget Pro", amount=149.99))

    print(f"\n=== Dashboard ===")
    print(f"  Orders: {dashboard.order_count}")
    print(f"  Revenue: ${dashboard.total_revenue:.2f}")
    print(f"  Statuses: {dashboard.statuses}")
    print(f"  Saga completed: {fulfillment.completed}")


if __name__ == "__main__":
    main()
