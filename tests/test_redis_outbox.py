"""Tests for RedisOutbox using fakeredis."""

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

from da2 import StoredEvent
from da2.outbox import OutboxRelay

pytestmark = pytest.mark.skipif(
    not (HAS_REDIS and HAS_FAKEREDIS),
    reason="redis and fakeredis required",
)


def _make_events() -> list[StoredEvent]:
    return [
        StoredEvent("order-1", "OrderPlaced", {"amount": 99.0}, 1, 1000.0, 1),
        StoredEvent("order-2", "OrderPlaced", {"amount": 49.5}, 1, 1001.0, 2),
    ]


@pytest.fixture
def outbox():
    from da2.redis_outbox import RedisOutbox
    client = fakeredis.FakeRedis(decode_responses=True)
    return RedisOutbox(client, prefix="test:outbox")


class TestRedisOutbox:

    def test_store_and_fetch(self, outbox):
        entries = outbox.store(_make_events())
        assert len(entries) == 2

        unpublished = outbox.fetch_unpublished()
        assert len(unpublished) == 2
        assert unpublished[0].event_type == "OrderPlaced"
        assert unpublished[0].aggregate_id == "order-1"
        assert unpublished[1].aggregate_id == "order-2"

    def test_store_empty(self, outbox):
        entries = outbox.store([])
        assert entries == []
        assert outbox.fetch_unpublished() == []

    def test_mark_published_removes_from_pending(self, outbox):
        entries = outbox.store(_make_events())
        outbox.mark_published([entries[0].id])

        remaining = outbox.fetch_unpublished()
        assert len(remaining) == 1
        assert remaining[0].aggregate_id == "order-2"

    def test_mark_all_published(self, outbox):
        entries = outbox.store(_make_events())
        outbox.mark_published([e.id for e in entries])
        assert outbox.fetch_unpublished() == []

    def test_mark_published_empty(self, outbox):
        outbox.mark_published([])  # should not raise

    def test_fetch_respects_limit(self, outbox):
        outbox.store(_make_events())
        batch = outbox.fetch_unpublished(limit=1)
        assert len(batch) == 1
        assert batch[0].aggregate_id == "order-1"

    def test_entry_data_preserved(self, outbox):
        events = [StoredEvent("a", "Foo", {"x": 1, "y": "bar"}, 3, 500.0, 42)]
        entries = outbox.store(events)

        fetched = outbox.fetch_unpublished()
        assert len(fetched) == 1
        assert fetched[0].event_type == "Foo"
        assert fetched[0].data == {"x": 1, "y": "bar"}
        assert fetched[0].version == 3
        assert fetched[0].position == 42
        assert fetched[0].published is False

    def test_fifo_ordering(self, outbox):
        outbox.store([StoredEvent("a", "First", {}, 1, 0.0, 1)])
        outbox.store([StoredEvent("b", "Second", {}, 1, 0.0, 2)])
        outbox.store([StoredEvent("c", "Third", {}, 1, 0.0, 3)])

        all_entries = outbox.fetch_unpublished()
        types = [e.event_type for e in all_entries]
        assert types == ["First", "Second", "Third"]

    def test_relay_integration(self, outbox):
        outbox.store(_make_events())

        published = []
        relay = OutboxRelay(outbox, publisher=lambda entries: published.extend(entries))

        count = relay.poll_and_publish()
        assert count == 2
        assert len(published) == 2
        assert outbox.fetch_unpublished() == []

    def test_relay_failure_preserves(self, outbox):
        outbox.store(_make_events())

        def failing_publisher(entries):
            raise ConnectionError("broker down")

        relay = OutboxRelay(outbox, publisher=failing_publisher)

        with pytest.raises(ConnectionError):
            relay.poll_and_publish()

        # Entries should still be pending
        assert len(outbox.fetch_unpublished()) == 2
