"""Tests for RedisSnapshotStore using fakeredis."""

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

from da2.snapshot import Snapshot

pytestmark = pytest.mark.skipif(
    not (HAS_REDIS and HAS_FAKEREDIS),
    reason="redis and fakeredis required",
)


@pytest.fixture
def store():
    from da2.redis_snapshot_store import RedisSnapshotStore
    client = fakeredis.FakeRedis(decode_responses=True)
    return RedisSnapshotStore(client, prefix="test:snapshots")


class TestRedisSnapshotStore:

    def test_save_and_load(self, store):
        snap = Snapshot(aggregate_id="agg-1", version=10, state={"balance": 500.0})
        store.save(snap)
        loaded = store.load("agg-1")
        assert loaded is not None
        assert loaded.aggregate_id == "agg-1"
        assert loaded.version == 10
        assert loaded.state == {"balance": 500.0}

    def test_load_missing_returns_none(self, store):
        assert store.load("nonexistent") is None

    def test_save_overwrites(self, store):
        store.save(Snapshot(aggregate_id="agg-1", version=10, state={"v": 1}))
        store.save(Snapshot(aggregate_id="agg-1", version=20, state={"v": 2}))
        loaded = store.load("agg-1")
        assert loaded is not None
        assert loaded.version == 20
        assert loaded.state == {"v": 2}

    def test_separate_aggregates(self, store):
        store.save(Snapshot(aggregate_id="a", version=1, state={"x": 1}))
        store.save(Snapshot(aggregate_id="b", version=2, state={"x": 2}))
        a = store.load("a")
        b = store.load("b")
        assert a is not None and a.version == 1
        assert b is not None and b.version == 2

    def test_timestamp_preserved(self, store):
        snap = Snapshot(aggregate_id="agg-1", version=5, state={}, timestamp=1234567890.0)
        store.save(snap)
        loaded = store.load("agg-1")
        assert loaded is not None
        assert loaded.timestamp == 1234567890.0
