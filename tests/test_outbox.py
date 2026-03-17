"""Tests for the Transactional Outbox pattern."""

import pytest

from da2 import (
    InMemoryOutbox,
    OutboxEntry,
    OutboxRelay,
    StoredEvent,
)


def _make_events() -> list[StoredEvent]:
    return [
        StoredEvent(
            aggregate_id="order-1",
            event_type="OrderPlaced",
            data={"amount": 99.0},
            version=1,
            timestamp=1000.0,
            position=1,
        ),
        StoredEvent(
            aggregate_id="order-1",
            event_type="ItemAdded",
            data={"name": "Widget"},
            version=2,
            timestamp=1001.0,
            position=2,
        ),
    ]


class TestOutboxEntry:

    def test_from_stored_event(self):
        se = StoredEvent(
            aggregate_id="agg-1",
            event_type="Foo",
            data={"x": 1},
            version=3,
            timestamp=500.0,
            position=42,
        )
        entry = OutboxEntry.from_stored_event(se)
        assert entry.aggregate_id == "agg-1"
        assert entry.event_type == "Foo"
        assert entry.data == {"x": 1}
        assert entry.version == 3
        assert entry.position == 42
        assert entry.published is False
        assert entry.id  # UUID assigned

    def test_data_is_copied(self):
        se = StoredEvent("a", "E", {"k": "v"}, 1, 0.0, 0)
        entry = OutboxEntry.from_stored_event(se)
        entry.data["k"] = "mutated"
        assert se.data["k"] == "v"


class TestInMemoryOutbox:

    def test_store_and_fetch(self):
        outbox = InMemoryOutbox()
        events = _make_events()
        entries = outbox.store(events)
        assert len(entries) == 2
        assert len(outbox) == 2

        unpublished = outbox.fetch_unpublished()
        assert len(unpublished) == 2
        assert unpublished[0].event_type == "OrderPlaced"
        assert unpublished[1].event_type == "ItemAdded"

    def test_mark_published(self):
        outbox = InMemoryOutbox()
        entries = outbox.store(_make_events())

        outbox.mark_published([entries[0].id])
        unpublished = outbox.fetch_unpublished()
        assert len(unpublished) == 1
        assert unpublished[0].event_type == "ItemAdded"

    def test_mark_all_published(self):
        outbox = InMemoryOutbox()
        entries = outbox.store(_make_events())

        outbox.mark_published([e.id for e in entries])
        assert outbox.fetch_unpublished() == []
        assert outbox.unpublished_count == 0

    def test_fetch_respects_limit(self):
        outbox = InMemoryOutbox()
        outbox.store(_make_events())

        batch = outbox.fetch_unpublished(limit=1)
        assert len(batch) == 1
        assert batch[0].event_type == "OrderPlaced"

    def test_empty_outbox(self):
        outbox = InMemoryOutbox()
        assert outbox.fetch_unpublished() == []
        assert len(outbox) == 0
        assert outbox.unpublished_count == 0

    def test_store_empty_list(self):
        outbox = InMemoryOutbox()
        entries = outbox.store([])
        assert entries == []
        assert len(outbox) == 0


class TestOutboxRelay:

    def test_poll_publishes_and_marks(self):
        outbox = InMemoryOutbox()
        outbox.store(_make_events())

        published = []
        relay = OutboxRelay(outbox, publisher=lambda entries: published.extend(entries))

        count = relay.poll_and_publish()
        assert count == 2
        assert len(published) == 2
        assert outbox.unpublished_count == 0

    def test_poll_empty_returns_zero(self):
        outbox = InMemoryOutbox()
        relay = OutboxRelay(outbox, publisher=lambda entries: None)

        count = relay.poll_and_publish()
        assert count == 0

    def test_publisher_failure_does_not_mark_published(self):
        outbox = InMemoryOutbox()
        outbox.store(_make_events())

        def failing_publisher(entries):
            raise ConnectionError("broker down")

        relay = OutboxRelay(outbox, publisher=failing_publisher)

        with pytest.raises(ConnectionError):
            relay.poll_and_publish()

        # Entries should still be unpublished
        assert outbox.unpublished_count == 2

    def test_batch_size_limits_poll(self):
        outbox = InMemoryOutbox()
        outbox.store(_make_events())

        published = []
        relay = OutboxRelay(
            outbox,
            publisher=lambda entries: published.extend(entries),
            batch_size=1,
        )

        count = relay.poll_and_publish()
        assert count == 1
        assert outbox.unpublished_count == 1

        count = relay.poll_and_publish()
        assert count == 1
        assert outbox.unpublished_count == 0

    def test_multiple_polls_are_idempotent(self):
        outbox = InMemoryOutbox()
        outbox.store(_make_events())

        published = []
        relay = OutboxRelay(outbox, publisher=lambda entries: published.extend(entries))

        relay.poll_and_publish()
        relay.poll_and_publish()  # second poll should find nothing

        assert len(published) == 2  # not 4

    def test_incremental_store_and_poll(self):
        outbox = InMemoryOutbox()
        published = []
        relay = OutboxRelay(outbox, publisher=lambda entries: published.extend(entries))

        # First batch
        outbox.store(_make_events()[:1])
        relay.poll_and_publish()
        assert len(published) == 1

        # Second batch
        outbox.store(_make_events()[1:])
        relay.poll_and_publish()
        assert len(published) == 2
