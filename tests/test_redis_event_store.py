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
