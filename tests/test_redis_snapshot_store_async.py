"""Tests for RedisSnapshotStoreAsync using fakeredis."""

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

from da2.snapshot import Snapshot

pytestmark = [
    pytest.mark.skipif(
        not (HAS_REDIS and HAS_FAKEREDIS),
        reason="redis and fakeredis required",
    ),
    pytest.mark.asyncio,
]


@pytest.fixture
def store():
    from da2.redis_snapshot_store_async import RedisSnapshotStoreAsync
    client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    return RedisSnapshotStoreAsync(client, prefix="test:snapshots")


async def test_save_and_load(store):
    snap = Snapshot(aggregate_id="agg-1", version=10, state={"balance": 500.0})
    await store.save(snap)
    loaded = await store.load("agg-1")
    assert loaded is not None
    assert loaded.version == 10
    assert loaded.state == {"balance": 500.0}


async def test_load_missing(store):
    assert await store.load("nonexistent") is None


async def test_save_overwrites(store):
    await store.save(Snapshot(aggregate_id="a", version=1, state={"v": 1}))
    await store.save(Snapshot(aggregate_id="a", version=2, state={"v": 2}))
    loaded = await store.load("a")
    assert loaded is not None
    assert loaded.version == 2


async def test_separate_aggregates(store):
    await store.save(Snapshot(aggregate_id="a", version=1, state={"x": 1}))
    await store.save(Snapshot(aggregate_id="b", version=2, state={"x": 2}))
    a = await store.load("a")
    b = await store.load("b")
    assert a is not None and a.version == 1
    assert b is not None and b.version == 2
