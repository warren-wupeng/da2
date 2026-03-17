"""Tests for RedisOutboxAsync using fakeredis."""

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

from da2 import StoredEvent
from da2.outbox_async import OutboxRelayAsync

pytestmark = [
    pytest.mark.skipif(
        not (HAS_REDIS and HAS_FAKEREDIS),
        reason="redis and fakeredis required",
    ),
    pytest.mark.asyncio,
]


def _make_events() -> list[StoredEvent]:
    return [
        StoredEvent("order-1", "OrderPlaced", {"amount": 99.0}, 1, 1000.0, 1),
        StoredEvent("order-2", "OrderPlaced", {"amount": 49.5}, 1, 1001.0, 2),
    ]


@pytest.fixture
def outbox():
    from da2.redis_outbox_async import RedisOutboxAsync
    client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    return RedisOutboxAsync(client, prefix="test:outbox")


async def test_store_and_fetch(outbox):
    entries = await outbox.store(_make_events())
    assert len(entries) == 2

    unpublished = await outbox.fetch_unpublished()
    assert len(unpublished) == 2
    assert unpublished[0].aggregate_id == "order-1"


async def test_store_empty(outbox):
    entries = await outbox.store([])
    assert entries == []


async def test_mark_published(outbox):
    entries = await outbox.store(_make_events())
    await outbox.mark_published([entries[0].id])

    remaining = await outbox.fetch_unpublished()
    assert len(remaining) == 1
    assert remaining[0].aggregate_id == "order-2"


async def test_mark_all_published(outbox):
    entries = await outbox.store(_make_events())
    await outbox.mark_published([e.id for e in entries])
    assert await outbox.fetch_unpublished() == []


async def test_fetch_limit(outbox):
    await outbox.store(_make_events())
    batch = await outbox.fetch_unpublished(limit=1)
    assert len(batch) == 1


async def test_relay_integration(outbox):
    await outbox.store(_make_events())

    published = []

    async def publisher(entries):
        published.extend(entries)

    relay = OutboxRelayAsync(outbox, publisher=publisher)
    count = await relay.poll_and_publish()
    assert count == 2
    assert len(published) == 2
    assert await outbox.fetch_unpublished() == []


async def test_relay_failure(outbox):
    await outbox.store(_make_events())

    async def failing_publisher(entries):
        raise ConnectionError("broker down")

    relay = OutboxRelayAsync(outbox, publisher=failing_publisher)
    with pytest.raises(ConnectionError):
        await relay.poll_and_publish()

    assert len(await outbox.fetch_unpublished()) == 2
