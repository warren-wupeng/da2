"""Example 05 -- Projections (CQRS Read Models).

Projections build read-optimized views by replaying domain events.
This example shows:
  1. Define a Projection with _on_<EventClassName> handlers
  2. Apply live events
  3. Replay from StoredEvents
  4. Incremental catch-up
"""

from __future__ import annotations

import time

from da2 import Event, StoredEvent, Projection


# ── Events ────────────────────────────────────────────────────────

class ItemAdded(Event):
    def __init__(self, item_id: str, name: str, price: float):
        self.item_id = item_id
        self.name = name
        self.price = price


class ItemRemoved(Event):
    def __init__(self, item_id: str):
        self.item_id = item_id


# ── Projection ────────────────────────────────────────────────────

class CartSummary(Projection):
    """Read model that tracks cart total and item count."""

    def __init__(self):
        super().__init__()
        self.items: dict[str, float] = {}  # item_id -> price
        self.total: float = 0.0

    @property
    def count(self) -> int:
        return len(self.items)

    def _on_ItemAdded(self, event: ItemAdded):
        self.items[event.item_id] = event.price
        self.total += event.price

    def _on_ItemRemoved(self, event: ItemRemoved):
        price = self.items.pop(event.item_id, 0.0)
        self.total -= price


EVENT_REGISTRY = {
    "ItemAdded": ItemAdded,
    "ItemRemoved": ItemRemoved,
}


# ── Demo ──────────────────────────────────────────────────────────

def main():
    # 1. Apply live events
    print("=== Live apply ===")
    cart = CartSummary()
    cart.apply(ItemAdded("a", "Widget", 9.99))
    cart.apply(ItemAdded("b", "Gadget", 24.99))
    cart.apply(ItemRemoved("a"))
    print(f"Items: {cart.count}, Total: ${cart.total:.2f}")
    # -> Items: 1, Total: $24.99

    # 2. Replay from stored events (simulating EventStore output)
    print("\n=== Replay from StoredEvents ===")
    stored = [
        StoredEvent("cart-1", "ItemAdded", {"item_id": "x", "name": "Foo", "price": 5.0}, version=1, timestamp=time.time()),
        StoredEvent("cart-1", "ItemAdded", {"item_id": "y", "name": "Bar", "price": 15.0}, version=2, timestamp=time.time()),
        StoredEvent("cart-1", "ItemRemoved", {"item_id": "x"}, version=3, timestamp=time.time()),
    ]
    cart2 = CartSummary()
    cart2.replay(stored, EVENT_REGISTRY)
    print(f"Items: {cart2.count}, Total: ${cart2.total:.2f}, Version: {cart2.last_version}")
    # -> Items: 1, Total: $15.00, Version: 3

    # 3. Incremental catch-up
    print("\n=== Catch-up (incremental) ===")
    new_events = [
        StoredEvent("cart-1", "ItemAdded", {"item_id": "x", "name": "Foo", "price": 5.0}, version=1, timestamp=time.time()),
        StoredEvent("cart-1", "ItemAdded", {"item_id": "y", "name": "Bar", "price": 15.0}, version=2, timestamp=time.time()),
        StoredEvent("cart-1", "ItemRemoved", {"item_id": "x"}, version=3, timestamp=time.time()),
        StoredEvent("cart-1", "ItemAdded", {"item_id": "z", "name": "Baz", "price": 7.50}, version=4, timestamp=time.time()),
    ]
    cart2.catch_up(new_events, EVENT_REGISTRY)
    print(f"Items: {cart2.count}, Total: ${cart2.total:.2f}, Version: {cart2.last_version}")
    # -> Items: 2, Total: $22.50, Version: 4

    print("\nDone.")


if __name__ == "__main__":
    main()
