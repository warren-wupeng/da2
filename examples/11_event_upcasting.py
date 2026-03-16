"""Example 11 -- Event Upcasting (Schema Evolution).

Demonstrates how to handle event schema changes over time using Upcasters.
When stored events have an older format, upcasters transform them to the
current schema during loading -- no data migration required.

Scenario:
  - v1: OrderPlaced had only {total}
  - v2: OrderPlaced gained {total, currency}
  - v3: OrderPlaced was renamed to OrderCreated
"""

from da2 import (
    Event,
    EventSourcedEntity,
    EventSourcedRepository,
    InMemoryEventStore,
    StoredEvent,
    Upcaster,
    UpcasterChain,
)


# --- Current domain (v3) ---

class OrderCreated(Event):
    def __init__(self, total: float, currency: str = "USD"):
        self.total = total
        self.currency = currency


class ItemAdded(Event):
    def __init__(self, product: str, quantity: int):
        self.product = product
        self.quantity = quantity


class Order(EventSourcedEntity[str]):
    def __init__(self, identity: str):
        super().__init__(identity)
        self.total = 0.0
        self.currency = ""
        self.items: list[str] = []

    def _when_OrderCreated(self, event):
        self.total = event.total
        self.currency = event.currency

    def _when_ItemAdded(self, event):
        self.items.append(f"{event.product} x{event.quantity}")


# --- Upcasters ---

class AddCurrencyToOrderPlaced(Upcaster):
    """v1 -> v2: add default currency field."""
    event_type = "OrderPlaced"

    def upcast(self, data: dict) -> dict:
        data.setdefault("currency", "USD")
        return data


class RenameOrderPlacedToCreated(Upcaster):
    """v2 -> v3: rename OrderPlaced to OrderCreated."""
    event_type = "OrderPlaced"
    target_event_type = "OrderCreated"

    def upcast(self, data: dict) -> dict:
        return data


class AddQuantityToItemAdded(Upcaster):
    """Legacy ItemAdded had no quantity field."""
    event_type = "ItemAdded"

    def upcast(self, data: dict) -> dict:
        data.setdefault("quantity", 1)
        return data


# --- Main ---

def main():
    print("=== Event Upcasting Demo ===\n")

    # Build upcaster chain (order matters!)
    chain = UpcasterChain([
        AddCurrencyToOrderPlaced(),     # v1 -> v2: add currency
        RenameOrderPlacedToCreated(),   # v2 -> v3: rename type
        AddQuantityToItemAdded(),       # add default quantity
    ])

    store = InMemoryEventStore()

    # Simulate legacy events already in the store (as they were written
    # by older versions of the application)
    legacy_stream = store._streams.setdefault("order-1", [])
    legacy_stream.append(StoredEvent(
        aggregate_id="order-1",
        event_type="OrderPlaced",        # old type name
        data={"total": 149.99},          # v1 format: no currency
        version=1,
    ))
    legacy_stream.append(StoredEvent(
        aggregate_id="order-1",
        event_type="ItemAdded",
        data={"product": "Keyboard"},    # old format: no quantity
        version=2,
    ))
    legacy_stream.append(StoredEvent(
        aggregate_id="order-1",
        event_type="ItemAdded",
        data={"product": "Mouse", "quantity": 2},  # newer event has quantity
        version=3,
    ))

    print("Stored events (legacy format):")
    for se in store.load("order-1"):
        print(f"  [{se.event_type}] {se.data}")

    # Create repository with upcaster chain
    repo = EventSourcedRepository(
        event_store=store,
        entity_cls=Order,
        event_registry={
            "OrderCreated": OrderCreated,  # current name
            "ItemAdded": ItemAdded,
        },
        upcaster_chain=chain,
    )

    # Load -- upcasters transparently transform legacy events
    order = repo.get("order-1")

    print(f"\nLoaded order (after upcasting):")
    print(f"  Total:    ${order.total}")
    print(f"  Currency: {order.currency}")
    print(f"  Items:    {order.items}")
    print(f"  Version:  {order.version}")

    # Verify upcasting worked
    assert order.total == 149.99
    assert order.currency == "USD"       # added by upcaster
    assert order.items == ["Keyboard x1", "Mouse x2"]
    assert order.version == 3

    print("\n=== Done ===")


if __name__ == "__main__":
    main()
