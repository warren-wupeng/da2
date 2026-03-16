"""Tests for global event stream (cross-aggregate projections)."""

import pytest
from da2 import (
    Event,
    InMemoryEventStore,
    InMemoryEventStoreAsync,
    Projection,
    ProjectionAsync,
    StoredEvent,
)


# --- Domain events ---

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


# --- Projections ---

class DashboardProjection(Projection):
    """Cross-aggregate read model: counts orders and users."""
    def __init__(self):
        super().__init__()
        self.total_orders = 0
        self.total_revenue = 0.0
        self.shipped_orders = 0
        self.total_users = 0

    def _on_OrderPlaced(self, event):
        self.total_orders += 1
        self.total_revenue += event.amount

    def _on_OrderShipped(self, event):
        self.shipped_orders += 1

    def _on_UserRegistered(self, event):
        self.total_users += 1


class DashboardProjectionAsync(ProjectionAsync):
    def __init__(self):
        super().__init__()
        self.total_orders = 0
        self.total_revenue = 0.0
        self.total_users = 0

    def _on_OrderPlaced(self, event):
        self.total_orders += 1
        self.total_revenue += event.amount

    def _on_UserRegistered(self, event):
        self.total_users += 1


# --- StoredEvent.position ---

class TestStoredEventPosition:
    def test_default_position_is_zero(self):
        se = StoredEvent(
            aggregate_id="a", event_type="X", data={}, version=1,
        )
        assert se.position == 0

    def test_explicit_position(self):
        se = StoredEvent(
            aggregate_id="a", event_type="X", data={}, version=1, position=42,
        )
        assert se.position == 42


# --- InMemoryEventStore.load_all ---

class TestInMemoryEventStoreGlobalStream:
    def test_load_all_empty(self):
        store = InMemoryEventStore()
        assert store.load_all() == []

    def test_load_all_returns_events_in_insertion_order(self):
        store = InMemoryEventStore()
        store.append("order-1", [OrderPlaced("o1", 100.0)], expected_version=0)
        store.append("user-1", [UserRegistered("u1", "Alice")], expected_version=0)
        store.append("order-1", [OrderShipped("o1")], expected_version=1)

        all_events = store.load_all()
        assert len(all_events) == 3
        assert all_events[0].event_type == "OrderPlaced"
        assert all_events[1].event_type == "UserRegistered"
        assert all_events[2].event_type == "OrderShipped"

    def test_positions_are_monotonically_increasing(self):
        store = InMemoryEventStore()
        store.append("a", [OrderPlaced("o1", 10.0)], expected_version=0)
        store.append("b", [UserRegistered("u1", "Bob")], expected_version=0)
        store.append("a", [OrderShipped("o1")], expected_version=1)

        all_events = store.load_all()
        positions = [se.position for se in all_events]
        assert positions == [1, 2, 3]

    def test_load_all_after_position_filters_correctly(self):
        store = InMemoryEventStore()
        store.append("a", [OrderPlaced("o1", 10.0)], expected_version=0)
        store.append("b", [UserRegistered("u1", "Bob")], expected_version=0)
        store.append("a", [OrderShipped("o1")], expected_version=1)

        after_2 = store.load_all(after_position=2)
        assert len(after_2) == 1
        assert after_2[0].event_type == "OrderShipped"
        assert after_2[0].position == 3

    def test_load_all_after_position_zero_returns_all(self):
        store = InMemoryEventStore()
        store.append("a", [OrderPlaced("o1", 50.0)], expected_version=0)
        assert len(store.load_all(after_position=0)) == 1

    def test_load_all_after_last_position_returns_empty(self):
        store = InMemoryEventStore()
        store.append("a", [OrderPlaced("o1", 50.0)], expected_version=0)
        assert store.load_all(after_position=1) == []

    def test_per_aggregate_load_still_works(self):
        """Existing per-aggregate load is unaffected by global tracking."""
        store = InMemoryEventStore()
        store.append("order-1", [OrderPlaced("o1", 100.0)], expected_version=0)
        store.append("user-1", [UserRegistered("u1", "Alice")], expected_version=0)

        order_events = store.load("order-1")
        assert len(order_events) == 1
        assert order_events[0].event_type == "OrderPlaced"
        assert order_events[0].position == 1

    def test_batch_append_assigns_consecutive_positions(self):
        store = InMemoryEventStore()
        store.append("a", [
            OrderPlaced("o1", 10.0),
            OrderShipped("o1"),
        ], expected_version=0)

        all_events = store.load_all()
        assert len(all_events) == 2
        assert all_events[0].position == 1
        assert all_events[1].position == 2


# --- Projection.replay_all / catch_up_all ---

class TestProjectionGlobalStream:
    def test_replay_all_processes_all_aggregates(self):
        store = InMemoryEventStore()
        store.append("order-1", [OrderPlaced("o1", 100.0)], expected_version=0)
        store.append("user-1", [UserRegistered("u1", "Alice")], expected_version=0)
        store.append("order-2", [OrderPlaced("o2", 200.0)], expected_version=0)
        store.append("order-1", [OrderShipped("o1")], expected_version=1)

        proj = DashboardProjection()
        proj.replay_all(store, EVENT_REGISTRY)

        assert proj.total_orders == 2
        assert proj.total_revenue == 300.0
        assert proj.shipped_orders == 1
        assert proj.total_users == 1
        assert proj.last_position == 4

    def test_catch_up_all_processes_only_new_events(self):
        store = InMemoryEventStore()
        store.append("order-1", [OrderPlaced("o1", 100.0)], expected_version=0)
        store.append("user-1", [UserRegistered("u1", "Alice")], expected_version=0)

        proj = DashboardProjection()
        proj.replay_all(store, EVENT_REGISTRY)
        assert proj.total_orders == 1
        assert proj.total_users == 1
        assert proj.last_position == 2

        # More events arrive
        store.append("order-2", [OrderPlaced("o2", 50.0)], expected_version=0)
        store.append("user-2", [UserRegistered("u2", "Bob")], expected_version=0)

        proj.catch_up_all(store, EVENT_REGISTRY)
        assert proj.total_orders == 2
        assert proj.total_users == 2
        assert proj.last_position == 4

    def test_catch_up_all_is_idempotent(self):
        store = InMemoryEventStore()
        store.append("a", [OrderPlaced("o1", 10.0)], expected_version=0)

        proj = DashboardProjection()
        proj.replay_all(store, EVENT_REGISTRY)
        proj.catch_up_all(store, EVENT_REGISTRY)  # no new events
        assert proj.total_orders == 1
        assert proj.last_position == 1

    def test_replay_all_with_unknown_events_skips_gracefully(self):
        store = InMemoryEventStore()
        store.append("a", [OrderPlaced("o1", 10.0)], expected_version=0)

        proj = DashboardProjection()
        # Only register OrderPlaced, not UserRegistered
        proj.replay_all(store, {"OrderPlaced": OrderPlaced})
        assert proj.total_orders == 1
        assert proj.last_position == 1

    def test_last_position_starts_at_zero(self):
        proj = DashboardProjection()
        assert proj.last_position == 0


# --- Async ---

class TestInMemoryEventStoreAsyncGlobalStream:
    async def test_load_all_returns_events_in_order(self):
        store = InMemoryEventStoreAsync()
        await store.append("a", [OrderPlaced("o1", 10.0)], expected_version=0)
        await store.append("b", [UserRegistered("u1", "X")], expected_version=0)

        all_events = await store.load_all()
        assert len(all_events) == 2
        assert all_events[0].position == 1
        assert all_events[1].position == 2

    async def test_load_all_after_position(self):
        store = InMemoryEventStoreAsync()
        await store.append("a", [OrderPlaced("o1", 10.0)], expected_version=0)
        await store.append("b", [UserRegistered("u1", "X")], expected_version=0)

        after_1 = await store.load_all(after_position=1)
        assert len(after_1) == 1
        assert after_1[0].event_type == "UserRegistered"


class TestProjectionAsyncGlobalStream:
    async def test_replay_all(self):
        store = InMemoryEventStoreAsync()
        await store.append("order-1", [OrderPlaced("o1", 100.0)], expected_version=0)
        await store.append("user-1", [UserRegistered("u1", "Alice")], expected_version=0)

        proj = DashboardProjectionAsync()
        await proj.replay_all(store, EVENT_REGISTRY)

        assert proj.total_orders == 1
        assert proj.total_users == 1
        assert proj.last_position == 2

    async def test_catch_up_all(self):
        store = InMemoryEventStoreAsync()
        await store.append("a", [OrderPlaced("o1", 10.0)], expected_version=0)

        proj = DashboardProjectionAsync()
        await proj.replay_all(store, EVENT_REGISTRY)
        assert proj.last_position == 1

        await store.append("b", [UserRegistered("u1", "X")], expected_version=0)
        await proj.catch_up_all(store, EVENT_REGISTRY)
        assert proj.total_users == 1
        assert proj.last_position == 2
