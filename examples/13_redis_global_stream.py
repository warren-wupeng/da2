"""Example 13 -- Redis Global Event Stream.

Demonstrates RedisEventStore with load_all() for production-grade
cross-aggregate projections. Uses fakeredis for local testing.

Requires: pip install da2[redis] fakeredis

Scenario:
  - Orders and Users stored in Redis-backed event store
  - Global event stream tracks monotonically increasing positions
  - Dashboard projection replays from the global stream
"""

try:
    import fakeredis
except ImportError:
    raise SystemExit("Run: pip install fakeredis")

from da2 import Event, Projection
from da2.redis_event_store import RedisEventStore


# --- Events ---

class OrderPlaced(Event):
    def __init__(self, order_id: str, amount: float):
        self.order_id = order_id
        self.amount = amount


class UserRegistered(Event):
    def __init__(self, user_id: str, name: str):
        self.user_id = user_id
        self.name = name


EVENT_REGISTRY = {
    "OrderPlaced": OrderPlaced,
    "UserRegistered": UserRegistered,
}


# --- Cross-aggregate projection ---

class DashboardProjection(Projection):
    def __init__(self):
        super().__init__()
        self.total_orders = 0
        self.total_revenue = 0.0
        self.user_count = 0

    def _on_OrderPlaced(self, event):
        self.total_orders += 1
        self.total_revenue += event.amount

    def _on_UserRegistered(self, event):
        self.user_count += 1


def main():
    print("=== Redis Global Event Stream Demo ===\n")

    # Create a Redis-backed event store (fakeredis for demo)
    client = fakeredis.FakeRedis(decode_responses=True)
    store = RedisEventStore(client, prefix="demo:events")

    # Append events from different aggregates
    store.append("order-1", [OrderPlaced("o1", 99.99)], expected_version=0)
    store.append("user-1", [UserRegistered("u1", "Alice")], expected_version=0)
    store.append("order-2", [OrderPlaced("o2", 49.50)], expected_version=0)
    store.append("user-2", [UserRegistered("u2", "Bob")], expected_version=0)

    # Global stream -- each event has a position assigned atomically by Lua
    print("Global event stream (Redis):")
    for se in store.load_all():
        print(f"  pos={se.position}  [{se.aggregate_id}] {se.event_type}")

    # Per-aggregate load also includes position
    print("\nPer-aggregate (order-1):")
    for se in store.load("order-1"):
        print(f"  pos={se.position}  v{se.version}  {se.event_type}")

    # Build cross-aggregate projection from global stream
    dashboard = DashboardProjection()
    dashboard.replay_all(store, EVENT_REGISTRY)

    print(f"\nDashboard:")
    print(f"  Orders:  {dashboard.total_orders}")
    print(f"  Revenue: ${dashboard.total_revenue:.2f}")
    print(f"  Users:   {dashboard.user_count}")
    print(f"  Position: {dashboard.last_position}")

    # Incremental catch-up after new events
    store.append("order-3", [OrderPlaced("o3", 200.00)], expected_version=0)
    dashboard.catch_up_all(store, EVENT_REGISTRY)

    print(f"\nAfter catch_up_all:")
    print(f"  Orders:  {dashboard.total_orders}")
    print(f"  Revenue: ${dashboard.total_revenue:.2f}")
    print(f"  Position: {dashboard.last_position}")

    # Verify
    assert dashboard.total_orders == 3
    assert dashboard.total_revenue == 349.49
    assert dashboard.user_count == 2
    assert dashboard.last_position == 5

    print("\n=== Done ===")


if __name__ == "__main__":
    main()
