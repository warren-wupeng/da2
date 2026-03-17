"""Tests for the async Transactional Outbox."""

import pytest

from da2 import StoredEvent
from da2.outbox_async import InMemoryOutboxAsync, OutboxRelayAsync

pytestmark = pytest.mark.asyncio


def _make_events() -> list[StoredEvent]:
    return [
        StoredEvent("order-1", "OrderPlaced", {"amount": 99.0}, 1, 1000.0, 1),
        StoredEvent("order-1", "ItemAdded", {"name": "Widget"}, 2, 1001.0, 2),
    ]


async def test_store_and_fetch():
    outbox = InMemoryOutboxAsync()
    entries = await outbox.store(_make_events())
    assert len(entries) == 2

    unpublished = await outbox.fetch_unpublished()
    assert len(unpublished) == 2


async def test_mark_published():
    outbox = InMemoryOutboxAsync()
    entries = await outbox.store(_make_events())

    await outbox.mark_published([entries[0].id])
    unpublished = await outbox.fetch_unpublished()
    assert len(unpublished) == 1


async def test_relay_publishes():
    outbox = InMemoryOutboxAsync()
    await outbox.store(_make_events())

    published = []

    async def publisher(entries):
        published.extend(entries)

    relay = OutboxRelayAsync(outbox, publisher=publisher)
    count = await relay.poll_and_publish()
    assert count == 2
    assert len(published) == 2
    assert outbox.unpublished_count == 0


async def test_relay_empty():
    outbox = InMemoryOutboxAsync()

    async def publisher(entries):
        pass

    relay = OutboxRelayAsync(outbox, publisher=publisher)
    count = await relay.poll_and_publish()
    assert count == 0


async def test_relay_failure_preserves_entries():
    outbox = InMemoryOutboxAsync()
    await outbox.store(_make_events())

    async def failing_publisher(entries):
        raise ConnectionError("broker down")

    relay = OutboxRelayAsync(outbox, publisher=failing_publisher)
    with pytest.raises(ConnectionError):
        await relay.poll_and_publish()

    assert outbox.unpublished_count == 2


async def test_relay_batch_size():
    outbox = InMemoryOutboxAsync()
    await outbox.store(_make_events())

    published = []

    async def publisher(entries):
        published.extend(entries)

    relay = OutboxRelayAsync(outbox, publisher=publisher, batch_size=1)
    await relay.poll_and_publish()
    assert len(published) == 1
    assert outbox.unpublished_count == 1
