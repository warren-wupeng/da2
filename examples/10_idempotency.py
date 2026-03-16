"""Example 10 -- Idempotent Command Handling.

Demonstrates how to prevent duplicate command execution using the
IdempotencyMiddleware. Commands with an idempotency_key are deduplicated;
commands without it pass through normally.
"""

from da2 import Command, Event, UnitOfWork, InMemoryRepository, Entity, bootstrap
from da2.idempotency import IdempotencyMiddleware, InMemoryIdempotencyStore


# --- Domain ---

class OrderPlaced(Event):
    def __init__(self, order_id: str, amount: float):
        self.order_id = order_id
        self.amount = amount


class PlaceOrder(Command):
    def __init__(self, order_id: str, amount: float, idempotency_key: str = None):
        self.order_id = order_id
        self.amount = amount
        self.idempotency_key = idempotency_key


class Order(Entity[str, dict]):
    @classmethod
    def create(cls, order_id: str, amount: float) -> "Order":
        order = cls(identity=order_id, desc={"amount": amount, "status": "placed"})
        order.raise_event(OrderPlaced(order_id=order_id, amount=amount))
        return order


class AppUoW(UnitOfWork):
    def __init__(self):
        self.seen: dict = {}
        self.orders = InMemoryRepository[Order](add_seen=self.add_seen)

    def _enter(self):
        pass

    def _commit(self):
        pass

    def rollback(self):
        pass


# --- Handler ---

def handle_place_order(cmd: PlaceOrder, uow: AppUoW):
    order = Order.create(cmd.order_id, cmd.amount)
    uow.orders.add(order)
    print(f"  [HANDLER] Order {cmd.order_id} placed for ${cmd.amount}")
    return {"order_id": cmd.order_id, "status": "created"}


def on_order_placed(event: OrderPlaced):
    print(f"  [EVENT] Confirmation email for order {event.order_id}")


# --- Main ---

def main():
    store = InMemoryIdempotencyStore()
    bus = bootstrap(
        uow=AppUoW(),
        commands={PlaceOrder: handle_place_order},
        events={OrderPlaced: [on_order_placed]},
        middleware=[IdempotencyMiddleware(store, ttl_seconds=60)],
    )

    print("=== Idempotent Command Handling Demo ===\n")

    # 1. First request -- executes normally
    print("1. First request (idempotency_key='order-abc-123'):")
    result = bus.handle(PlaceOrder("ORD-1", 99.99, idempotency_key="order-abc-123"))
    print(f"   Result: {result}\n")

    # 2. Duplicate request -- returns cached result, handler NOT called
    print("2. Duplicate request (same key):")
    result = bus.handle(PlaceOrder("ORD-1", 99.99, idempotency_key="order-abc-123"))
    print(f"   Result: {result}  (cached -- no handler/event output above)\n")

    # 3. Different key -- executes normally
    print("3. Different key (idempotency_key='order-def-456'):")
    result = bus.handle(PlaceOrder("ORD-2", 49.99, idempotency_key="order-def-456"))
    print(f"   Result: {result}\n")

    # 4. No key -- always executes
    print("4. No idempotency_key (always executes):")
    bus.handle(PlaceOrder("ORD-3", 19.99))
    bus.handle(PlaceOrder("ORD-4", 29.99))
    print()

    print("=== Done ===")


if __name__ == "__main__":
    main()
