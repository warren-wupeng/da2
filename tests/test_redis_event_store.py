"""Tests for RedisEventStore using fakeredis."""

import json
import pytest

try:
    import fakeredis
    HAS_FAKEREDIS = True
except ImportError:
    HAS_FAKEREDIS = False

try:
    import redis
    HAS_REDIS = True
except ImportError:
    HAS_REDIS = False

from da2 import Event, ConcurrencyError

pytestmark = pytest.mark.skipif(
    not (HAS_REDIS and HAS_FAKEREDIS),
    reason="redis and fakeredis required",
)


class OrderPlaced(Event):
    def __init__(self, total: float):
        self.total = total


class ItemAdded(Event):
    def __init__(self, name: str):
        self.name = name


@pytest.fixture
def store():
    from da2.redis_event_store import RedisEventStore
    client = fakeredis.FakeRedis(decode_responses=True)
    return RedisEventStore(client, prefix="test:events")


class TestRedisEventStore:

    def test_append_and_load(self, store):
        store.append("agg-1", [OrderPlaced(total=99.0)], expected_version=0)
        events = store.load("agg-1")
        assert len(events) == 1
        assert events[0].event_type == "OrderPlaced"
        assert events[0].data == {"total": 99.0}
        assert events[0].version == 1
        assert events[0].aggregate_id == "agg-1"

    def test_append_multiple(self, store):
        store.append("agg-1", [OrderPlaced(total=10.0), ItemAdded(name="Widget")], expected_version=0)
        events = store.load("agg-1")
        assert len(events) == 2
        assert events[0].version == 1
        assert events[1].version == 2
        assert events[1].event_type == "ItemAdded"

    def test_append_incremental(self, store):
        store.append("agg-1", [OrderPlaced(total=10.0)], expected_version=0)
        store.append("agg-1", [ItemAdded(name="Widget")], expected_version=1)
        events = store.load("agg-1")
        assert len(events) == 2
        assert events[0].version == 1
        assert events[1].version == 2

    def test_concurrency_error(self, store):
        store.append("agg-1", [OrderPlaced(total=10.0)], expected_version=0)
        with pytest.raises(ConcurrencyError):
            store.append("agg-1", [ItemAdded(name="X")], expected_version=0)

    def test_load_empty(self, store):
        events = store.load("nonexistent")
        assert events == []

    def test_load_since(self, store):
        store.append("agg-1", [OrderPlaced(total=10.0), ItemAdded(name="A")], expected_version=0)
        store.append("agg-1", [ItemAdded(name="B")], expected_version=2)
        events = store.load_since("agg-1", after_version=1)
        assert len(events) == 2
        assert events[0].version == 2
        assert events[1].version == 3

    def test_separate_aggregates(self, store):
        store.append("agg-1", [OrderPlaced(total=10.0)], expected_version=0)
        store.append("agg-2", [OrderPlaced(total=20.0)], expected_version=0)
        assert len(store.load("agg-1")) == 1
        assert len(store.load("agg-2")) == 1
        assert store.load("agg-1")[0].data["total"] == 10.0
        assert store.load("agg-2")[0].data["total"] == 20.0

    def test_append_empty_is_noop(self, store):
        store.append("agg-1", [], expected_version=0)
        assert store.load("agg-1") == []


class TestRedisEventStoreGlobalStream:
    """Tests for RedisEventStore.load_all (global event stream)."""

    def test_load_all_empty(self, store):
        assert store.load_all() == []

    def test_load_all_returns_events_in_insertion_order(self, store):
        store.append("order-1", [OrderPlaced(total=100.0)], expected_version=0)
        store.append("order-2", [OrderPlaced(total=200.0)], expected_version=0)

        all_events = store.load_all()
        assert len(all_events) == 2
        assert all_events[0].data["total"] == 100.0
        assert all_events[1].data["total"] == 200.0

    def test_positions_are_monotonically_increasing(self, store):
        store.append("a", [OrderPlaced(total=10.0)], expected_version=0)
        store.append("b", [OrderPlaced(total=20.0)], expected_version=0)
        store.append("a", [ItemAdded(name="X")], expected_version=1)

        all_events = store.load_all()
        positions = [se.position for se in all_events]
        assert positions == [1, 2, 3]

    def test_load_all_after_position_filters(self, store):
        store.append("a", [OrderPlaced(total=10.0)], expected_version=0)
        store.append("b", [OrderPlaced(total=20.0)], expected_version=0)
        store.append("a", [ItemAdded(name="X")], expected_version=1)

        after_2 = store.load_all(after_position=2)
        assert len(after_2) == 1
        assert after_2[0].event_type == "ItemAdded"
        assert after_2[0].position == 3

    def test_load_all_after_position_zero_returns_all(self, store):
        store.append("a", [OrderPlaced(total=50.0)], expected_version=0)
        assert len(store.load_all(after_position=0)) == 1

    def test_load_all_after_last_position_returns_empty(self, store):
        store.append("a", [OrderPlaced(total=50.0)], expected_version=0)
        assert store.load_all(after_position=1) == []

    def test_batch_append_assigns_consecutive_positions(self, store):
        store.append("a", [
            OrderPlaced(total=10.0),
            ItemAdded(name="W"),
        ], expected_version=0)

        all_events = store.load_all()
        assert len(all_events) == 2
        assert all_events[0].position == 1
        assert all_events[1].position == 2

    def test_per_aggregate_load_includes_position(self, store):
        store.append("a", [OrderPlaced(total=10.0)], expected_version=0)
        store.append("b", [OrderPlaced(total=20.0)], expected_version=0)

        events_a = store.load("a")
        assert len(events_a) == 1
        assert events_a[0].position == 1

    def test_cross_aggregate_ordering(self, store):
        store.append("order-1", [OrderPlaced(total=100.0)], expected_version=0)
        store.append("order-2", [OrderPlaced(total=200.0)], expected_version=0)
        store.append("order-1", [ItemAdded(name="A")], expected_version=1)

        all_events = store.load_all()
        assert len(all_events) == 3
        assert all_events[0].aggregate_id == "order-1"
        assert all_events[1].aggregate_id == "order-2"
        assert all_events[2].aggregate_id == "order-1"
