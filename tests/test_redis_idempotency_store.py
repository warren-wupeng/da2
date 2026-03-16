"""Tests for RedisIdempotencyStore."""

import pytest
import fakeredis

from da2 import Command, MessageBus, UnitOfWork
from da2.idempotency import IdempotencyMiddleware
from da2.redis_idempotency_store import RedisIdempotencyStore


class PlaceOrder(Command):
    def __init__(self, order_id: str, idempotency_key: str | None = None):
        self.order_id = order_id
        self.idempotency_key = idempotency_key


class FakeUoW(UnitOfWork):
    def __init__(self):
        self.seen = {}

    def _enter(self):
        pass

    def _commit(self):
        pass

    def rollback(self):
        pass


@pytest.fixture
def redis_client():
    return fakeredis.FakeRedis(decode_responses=True)


@pytest.fixture
def store(redis_client):
    return RedisIdempotencyStore(redis_client)


class TestRedisIdempotencyStore:

    def test_get_missing(self, store):
        found, result = store.get("nonexistent")
        assert found is False
        assert result is None

    def test_save_and_get(self, store):
        store.save("k1", {"status": "ok"})
        found, result = store.get("k1")
        assert found is True
        assert result == {"status": "ok"}

    def test_save_none_result(self, store):
        store.save("k1", None)
        found, result = store.get("k1")
        assert found is True
        assert result is None

    def test_save_string_result(self, store):
        store.save("k1", "hello")
        found, result = store.get("k1")
        assert found is True
        assert result == "hello"

    def test_save_numeric_result(self, store):
        store.save("k1", 42)
        found, result = store.get("k1")
        assert found is True
        assert result == 42

    def test_separate_keys(self, store):
        store.save("a", "result-a")
        store.save("b", "result-b")
        assert store.get("a") == (True, "result-a")
        assert store.get("b") == (True, "result-b")

    def test_overwrite(self, store):
        store.save("k1", "first")
        store.save("k1", "second")
        found, result = store.get("k1")
        assert found is True
        assert result == "second"

    def test_ttl_sets_expiration(self, redis_client):
        store = RedisIdempotencyStore(redis_client)
        store.save("k1", "result", ttl_seconds=300)
        ttl = redis_client.ttl("da2:idempotency:k1")
        assert 0 < ttl <= 300

    def test_custom_prefix(self, redis_client):
        store = RedisIdempotencyStore(redis_client, prefix="myapp:idem")
        store.save("k1", "result")
        raw = redis_client.get("myapp:idem:k1")
        assert raw is not None

    def test_full_integration_with_middleware(self, store):
        call_count = [0]

        def handler(cmd):
            call_count[0] += 1
            return f"order:{cmd.order_id}"

        bus = MessageBus(
            uow=FakeUoW(),
            event_handlers={},
            command_handlers={PlaceOrder: handler},
            middleware=[IdempotencyMiddleware(store)],
        )

        r1 = bus.handle(PlaceOrder("o1", idempotency_key="k1"))
        r2 = bus.handle(PlaceOrder("o1", idempotency_key="k1"))
        assert r1 == r2 == "order:o1"
        assert call_count[0] == 1
