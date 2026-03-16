"""Tests for RedisIdempotencyStoreAsync."""

import pytest
import fakeredis.aioredis

from da2 import Command, MessageBusAsync, UnitOfWorkAsync
from da2.idempotency_async import IdempotencyMiddlewareAsync
from da2.redis_idempotency_store_async import RedisIdempotencyStoreAsync


class PlaceOrder(Command):
    def __init__(self, order_id: str, idempotency_key: str | None = None):
        self.order_id = order_id
        self.idempotency_key = idempotency_key


class FakeUoW(UnitOfWorkAsync):
    def __init__(self):
        self.seen = {}

    async def _enter(self):
        pass

    async def _commit(self):
        pass

    async def rollback(self):
        pass

    async def _exit(self, *a):
        pass


pytestmark = pytest.mark.asyncio


@pytest.fixture
def redis_client():
    return fakeredis.aioredis.FakeRedis(decode_responses=True)


@pytest.fixture
def store(redis_client):
    return RedisIdempotencyStoreAsync(redis_client)


async def test_get_missing(store):
    found, result = await store.get("nonexistent")
    assert found is False
    assert result is None


async def test_save_and_get(store):
    await store.save("k1", {"status": "ok"})
    found, result = await store.get("k1")
    assert found is True
    assert result == {"status": "ok"}


async def test_save_none_result(store):
    await store.save("k1", None)
    found, result = await store.get("k1")
    assert found is True
    assert result is None


async def test_separate_keys(store):
    await store.save("a", "result-a")
    await store.save("b", "result-b")
    assert await store.get("a") == (True, "result-a")
    assert await store.get("b") == (True, "result-b")


async def test_ttl_sets_expiration(redis_client):
    store = RedisIdempotencyStoreAsync(redis_client)
    await store.save("k1", "result", ttl_seconds=300)
    ttl = await redis_client.ttl("da2:idempotency:k1")
    assert 0 < ttl <= 300


async def test_full_integration_with_middleware(store):
    call_count = [0]

    async def handler(cmd):
        call_count[0] += 1
        return f"order:{cmd.order_id}"

    bus = MessageBusAsync(
        uow=FakeUoW(),
        event_handlers={},
        command_handlers={PlaceOrder: handler},
        middleware=[IdempotencyMiddlewareAsync(store)],
    )

    r1 = await bus.handle(PlaceOrder("o1", idempotency_key="k1"))
    r2 = await bus.handle(PlaceOrder("o1", idempotency_key="k1"))
    assert r1 == r2 == "order:o1"
    assert call_count[0] == 1
