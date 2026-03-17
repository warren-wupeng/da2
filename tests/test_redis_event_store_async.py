"""Tests for RedisEventStoreAsync using fakeredis."""

import pytest

try:
    import fakeredis
    import fakeredis.aioredis
    HAS_FAKEREDIS = True
except ImportError:
    HAS_FAKEREDIS = False

try:
    import redis
    HAS_REDIS = True
except ImportError:
    HAS_REDIS = False

from da2 import Event, ConcurrencyError

pytestmark = [
    pytest.mark.skipif(
        not (HAS_REDIS and HAS_FAKEREDIS),
        reason="redis and fakeredis required",
    ),
    pytest.mark.asyncio,
]


class OrderPlaced(Event):
    def __init__(self, total: float):
        self.total = total


class ItemAdded(Event):
    def __init__(self, name: str):
        self.name = name


@pytest.fixture
def store():
    from da2.redis_event_store_async import RedisEventStoreAsync
    client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    return RedisEventStoreAsync(client, prefix="test:events")


async def test_append_and_load(store):
    await store.append("agg-1", [OrderPlaced(total=99.0)], expected_version=0)
    events = await store.load("agg-1")
    assert len(events) == 1
    assert events[0].event_type == "OrderPlaced"
    assert events[0].data == {"total": 99.0}
    assert events[0].version == 1


async def test_append_multiple(store):
    await store.append("agg-1", [OrderPlaced(total=10.0), ItemAdded(name="W")], expected_version=0)
    events = await store.load("agg-1")
    assert len(events) == 2
    assert events[1].event_type == "ItemAdded"


async def test_append_incremental(store):
    await store.append("agg-1", [OrderPlaced(total=10.0)], expected_version=0)
    await store.append("agg-1", [ItemAdded(name="W")], expected_version=1)
    events = await store.load("agg-1")
    assert len(events) == 2
    assert events[1].version == 2


async def test_concurrency_error(store):
    await store.append("agg-1", [OrderPlaced(total=10.0)], expected_version=0)
    with pytest.raises(ConcurrencyError):
        await store.append("agg-1", [ItemAdded(name="X")], expected_version=0)


async def test_load_empty(store):
    events = await store.load("nonexistent")
    assert events == []


async def test_load_since(store):
    await store.append("agg-1", [OrderPlaced(total=10.0), ItemAdded(name="A")], expected_version=0)
    await store.append("agg-1", [ItemAdded(name="B")], expected_version=2)
    events = await store.load_since("agg-1", after_version=2)
    assert len(events) == 1
    assert events[0].version == 3


# --- Global stream tests ---


async def test_load_all_empty(store):
    assert await store.load_all() == []


async def test_load_all_returns_events_in_order(store):
    await store.append("a", [OrderPlaced(total=10.0)], expected_version=0)
    await store.append("b", [OrderPlaced(total=20.0)], expected_version=0)

    all_events = await store.load_all()
    assert len(all_events) == 2
    assert all_events[0].data["total"] == 10.0
    assert all_events[1].data["total"] == 20.0


async def test_positions_monotonically_increasing(store):
    await store.append("a", [OrderPlaced(total=10.0)], expected_version=0)
    await store.append("b", [OrderPlaced(total=20.0)], expected_version=0)
    await store.append("a", [ItemAdded(name="X")], expected_version=1)

    all_events = await store.load_all()
    positions = [se.position for se in all_events]
    assert positions == [1, 2, 3]


async def test_load_all_after_position(store):
    await store.append("a", [OrderPlaced(total=10.0)], expected_version=0)
    await store.append("b", [OrderPlaced(total=20.0)], expected_version=0)

    after_1 = await store.load_all(after_position=1)
    assert len(after_1) == 1
    assert after_1[0].data["total"] == 20.0
    assert after_1[0].position == 2


async def test_load_all_after_last_returns_empty(store):
    await store.append("a", [OrderPlaced(total=10.0)], expected_version=0)
    assert await store.load_all(after_position=1) == []


async def test_batch_append_consecutive_positions(store):
    await store.append("a", [OrderPlaced(total=10.0), ItemAdded(name="W")], expected_version=0)

    all_events = await store.load_all()
    assert len(all_events) == 2
    assert all_events[0].position == 1
    assert all_events[1].position == 2


async def test_per_aggregate_load_includes_position(store):
    await store.append("a", [OrderPlaced(total=10.0)], expected_version=0)
    events = await store.load("a")
    assert events[0].position == 1
