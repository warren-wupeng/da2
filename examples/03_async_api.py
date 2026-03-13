"""Async example -- using MessageBusAsync and UnitOfWorkAsync.

Run::

    cd /path/to/da2
    uv run python examples/03_async_api.py
"""

import asyncio

from da2 import (
    Command,
    Event,
    Entity,
    UnitOfWorkAsync,
    BootstrapAsync,
)


# --- Domain ---

class OrderPlaced(Event):
    def __init__(self, order_id: str, total: float) -> None:
        self.order_id = order_id
        self.total = total


class PlaceOrder(Command):
    def __init__(self, order_id: str, items: list[str], total: float) -> None:
        self.order_id = order_id
        self.items = items
        self.total = total


class Order(Entity[str, dict]):
    @classmethod
    def place(cls, order_id: str, items: list[str], total: float) -> "Order":
        order = cls(
            identity=order_id,
            desc={"items": items, "total": total, "status": "placed"},
        )
        order.raise_event(OrderPlaced(order_id=order_id, total=total))
        return order


# --- Async UoW ---

class FakeAsyncUoW(UnitOfWorkAsync):
    def __init__(self) -> None:
        super().__init__()
        self.seen: dict = {}  # init seen so bus can collect events
        self.orders: dict[str, Order] = {}
        self.committed = False

    async def _enter(self) -> None:
        self.committed = False

    async def _commit(self) -> None:
        self.committed = True
        print("[uow] committed")

    async def rollback(self) -> None:
        self.committed = False
        print("[uow] rolled back")

    async def _exit(self, exc_type: type | None, exc_val: BaseException | None, exc_tb: object) -> None:
        pass


# --- Async Handlers ---

async def handle_place_order(cmd: PlaceOrder, uow: FakeAsyncUoW) -> None:
    order = Order.place(cmd.order_id, cmd.items, cmd.total)
    uow.orders[order.identity] = order
    uow.add_seen(order)
    print(f"[cmd] Placed order {cmd.order_id}: {cmd.items} = ${cmd.total:.2f}")


async def on_order_placed(event: OrderPlaced) -> None:
    # Simulate async I/O (e.g., sending email)
    await asyncio.sleep(0.01)
    print(f"[event] Order {event.order_id} placed -- sending confirmation email")


async def on_order_placed_analytics(event: OrderPlaced) -> None:
    print(f"[event] Analytics: order {event.order_id} worth ${event.total:.2f}")


# --- Wire & Run ---

async def main() -> None:
    uow = FakeAsyncUoW()
    bus = BootstrapAsync(
        uow=uow,
        command_handlers={PlaceOrder: handle_place_order},
        event_handlers={
            OrderPlaced: [on_order_placed, on_order_placed_analytics],
        },
    ).create_message_bus()

    await bus.handle(PlaceOrder(
        order_id="ORD-001",
        items=["Widget", "Gadget"],
        total=49.99,
    ))

    await bus.handle(PlaceOrder(
        order_id="ORD-002",
        items=["Gizmo"],
        total=19.99,
    ))

    print()
    print(f"Total orders: {len(uow.orders)}")
    for oid, order in uow.orders.items():
        print(f"  {oid}: {order.desc}")


if __name__ == "__main__":
    asyncio.run(main())
