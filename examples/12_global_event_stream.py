"""Example 12 -- Global Event Stream (Cross-Aggregate Projections).

Demonstrates how to build read models that span multiple aggregate types
using the global event stream. This is essential for real-world CQRS where
dashboards and reports need data from different aggregates.

Scenario:
  - Orders and Users are separate aggregates
  - A Dashboard projection processes events from BOTH to build a unified view
  - catch_up_all() incrementally processes only new events
"""

from da2 import (
    Event,
    InMemoryEventStore,
    Projection,
)


# --- Events from different aggregates ---

class OrderPlaced(Event):
    def __init__(self, order_id: str, amount: float):
        self.order_id = order_id
        self.amount = amount


class OrderShipped(Event):
    def __init__(self, order_id: str):
        self.order_id = order_id


class UserRegistered(Event):
    def __init__(self, user_id: str, name: str):
        self.user_id = user_id
        self.name = name


EVENT_REGISTRY = {
    "OrderPlaced": OrderPlaced,
    "OrderShipped": OrderShipped,
    "UserRegistered": UserRegistered,
}


# --- Cross-aggregate projection ---

class DashboardProjection(Projection):
    """Unified read model spanning orders and users."""

    def __init__(self):
        super().__init__()
        self.total_orders = 0
        self.total_revenue = 0.0
        self.shipped_count = 0
        self.user_count = 0
        self.recent_users: list[str] = []

    def _on_OrderPlaced(self, event):
        self.total_orders += 1
        self.total_revenue += event.amount

    def _on_OrderShipped(self, event):
        self.shipped_count += 1

    def _on_UserRegistered(self, event):
        self.user_count += 1
        self.recent_users.append(event.name)


def main():
    print("=== Global Event Stream Demo ===\n")

    store = InMemoryEventStore()

    # Events from different aggregates, interleaved
    store.append("order-1", [OrderPlaced("o1", 99.99)], expected_version=0)
    store.append("user-1", [UserRegistered("u1", "Alice")], expected_version=0)
    store.append("order-2", [OrderPlaced("o2", 49.50)], expected_version=0)
    store.append("order-1", [OrderShipped("o1")], expected_version=1)
    store.append("user-2", [UserRegistered("u2", "Bob")], expected_version=0)

    # Show global stream with positions
    print("Global event stream:")
    for se in store.load_all():
        print(f"  pos={se.position}  [{se.aggregate_id}] {se.event_type}")

    # Build cross-aggregate projection
    dashboard = DashboardProjection()
    dashboard.replay_all(store, EVENT_REGISTRY)

    print(f"\nDashboard after replay_all:")
    print(f"  Orders:   {dashboard.total_orders}")
    print(f"  Revenue:  ${dashboard.total_revenue:.2f}")
    print(f"  Shipped:  {dashboard.shipped_count}")
    print(f"  Users:    {dashboard.user_count}")
    print(f"  Names:    {dashboard.recent_users}")
    print(f"  Position: {dashboard.last_position}")

    # --- More events arrive ---
    store.append("order-3", [OrderPlaced("o3", 199.00)], expected_version=0)
    store.append("user-3", [UserRegistered("u3", "Charlie")], expected_version=0)
    store.append("order-2", [OrderShipped("o2")], expected_version=1)

    # Incremental catch-up (only processes events after position 5)
    dashboard.catch_up_all(store, EVENT_REGISTRY)

    print(f"\nDashboard after catch_up_all:")
    print(f"  Orders:   {dashboard.total_orders}")
    print(f"  Revenue:  ${dashboard.total_revenue:.2f}")
    print(f"  Shipped:  {dashboard.shipped_count}")
    print(f"  Users:    {dashboard.user_count}")
    print(f"  Names:    {dashboard.recent_users}")
    print(f"  Position: {dashboard.last_position}")

    # Verify
    assert dashboard.total_orders == 3
    assert dashboard.total_revenue == 348.49
    assert dashboard.shipped_count == 2
    assert dashboard.user_count == 3
    assert dashboard.last_position == 8

    print("\n=== Done ===")


if __name__ == "__main__":
    main()
