"""Tests for idempotent command handling."""

import time
from unittest.mock import patch

import pytest
from da2 import Command, Event, MessageBus, UnitOfWork, bootstrap
from da2.idempotency import (
    IdempotencyMiddleware,
    InMemoryIdempotencyStore,
)


class PlaceOrder(Command):
    def __init__(self, order_id: str, idempotency_key: str | None = None):
        self.order_id = order_id
        self.idempotency_key = idempotency_key


class SimpleCmd(Command):
    """Command without idempotency_key."""
    def __init__(self, value: str):
        self.value = value


class FakeUoW(UnitOfWork):
    def __init__(self):
        self.seen = {}

    def _enter(self):
        pass

    def _commit(self):
        pass

    def rollback(self):
        pass


class TestInMemoryIdempotencyStore:

    def test_get_missing_returns_false(self):
        store = InMemoryIdempotencyStore()
        found, result = store.get("nonexistent")
        assert found is False
        assert result is None

    def test_save_and_get(self):
        store = InMemoryIdempotencyStore()
        store.save("key1", {"status": "ok"})
        found, result = store.get("key1")
        assert found is True
        assert result == {"status": "ok"}

    def test_ttl_expiration(self):
        store = InMemoryIdempotencyStore()
        # Save with 0 TTL -- should expire immediately
        base = time.monotonic()
        with patch("da2.idempotency.time") as mock_time:
            mock_time.monotonic.return_value = base
            store.save("key1", "result", ttl_seconds=10)

            # Before expiry
            mock_time.monotonic.return_value = base + 5
            found, _ = store.get("key1")
            assert found is True

            # After expiry
            mock_time.monotonic.return_value = base + 11
            found, _ = store.get("key1")
            assert found is False

    def test_clear(self):
        store = InMemoryIdempotencyStore()
        store.save("a", 1)
        store.save("b", 2)
        store.clear()
        assert store.get("a") == (False, None)
        assert store.get("b") == (False, None)

    def test_save_none_result(self):
        store = InMemoryIdempotencyStore()
        store.save("key1", None)
        found, result = store.get("key1")
        assert found is True
        assert result is None


class TestIdempotencyMiddleware:

    def test_first_call_executes_handler(self):
        call_count = [0]

        def handler(cmd):
            call_count[0] += 1
            return f"order:{cmd.order_id}"

        store = InMemoryIdempotencyStore()
        bus = MessageBus(
            uow=FakeUoW(),
            event_handlers={},
            command_handlers={PlaceOrder: handler},
            middleware=[IdempotencyMiddleware(store)],
        )
        result = bus.handle(PlaceOrder("o1", idempotency_key="key-1"))
        assert result == "order:o1"
        assert call_count[0] == 1

    def test_duplicate_returns_cached_result(self):
        call_count = [0]

        def handler(cmd):
            call_count[0] += 1
            return f"order:{cmd.order_id}"

        store = InMemoryIdempotencyStore()
        mw = IdempotencyMiddleware(store)
        bus = MessageBus(
            uow=FakeUoW(),
            event_handlers={},
            command_handlers={PlaceOrder: handler},
            middleware=[mw],
        )
        r1 = bus.handle(PlaceOrder("o1", idempotency_key="key-1"))
        r2 = bus.handle(PlaceOrder("o1", idempotency_key="key-1"))
        assert r1 == r2 == "order:o1"
        assert call_count[0] == 1  # handler called only once

    def test_different_keys_execute_separately(self):
        call_count = [0]

        def handler(cmd):
            call_count[0] += 1
            return f"order:{cmd.order_id}"

        store = InMemoryIdempotencyStore()
        bus = MessageBus(
            uow=FakeUoW(),
            event_handlers={},
            command_handlers={PlaceOrder: handler},
            middleware=[IdempotencyMiddleware(store)],
        )
        bus.handle(PlaceOrder("o1", idempotency_key="key-1"))
        bus.handle(PlaceOrder("o2", idempotency_key="key-2"))
        assert call_count[0] == 2

    def test_no_idempotency_key_passes_through(self):
        call_count = [0]

        def handler(cmd):
            call_count[0] += 1
            return "ok"

        store = InMemoryIdempotencyStore()
        bus = MessageBus(
            uow=FakeUoW(),
            event_handlers={},
            command_handlers={PlaceOrder: handler},
            middleware=[IdempotencyMiddleware(store)],
        )
        bus.handle(PlaceOrder("o1"))  # no idempotency_key
        bus.handle(PlaceOrder("o1"))  # no idempotency_key
        assert call_count[0] == 2  # both executed

    def test_command_without_key_attribute(self):
        """Commands that don't define idempotency_key at all should work."""
        call_count = [0]

        def handler(cmd):
            call_count[0] += 1
            return cmd.value

        store = InMemoryIdempotencyStore()
        bus = MessageBus(
            uow=FakeUoW(),
            event_handlers={},
            command_handlers={SimpleCmd: handler},
            middleware=[IdempotencyMiddleware(store)],
        )
        bus.handle(SimpleCmd("a"))
        bus.handle(SimpleCmd("a"))
        assert call_count[0] == 2

    def test_caches_none_result(self):
        call_count = [0]

        def handler(cmd):
            call_count[0] += 1
            return None

        store = InMemoryIdempotencyStore()
        bus = MessageBus(
            uow=FakeUoW(),
            event_handlers={},
            command_handlers={PlaceOrder: handler},
            middleware=[IdempotencyMiddleware(store)],
        )
        r1 = bus.handle(PlaceOrder("o1", idempotency_key="key-1"))
        r2 = bus.handle(PlaceOrder("o1", idempotency_key="key-1"))
        assert r1 is None
        assert r2 is None
        assert call_count[0] == 1

    def test_exception_not_cached(self):
        call_count = [0]

        def handler(cmd):
            call_count[0] += 1
            if call_count[0] == 1:
                raise ValueError("transient error")
            return "success"

        store = InMemoryIdempotencyStore()
        bus = MessageBus(
            uow=FakeUoW(),
            event_handlers={},
            command_handlers={PlaceOrder: handler},
            middleware=[IdempotencyMiddleware(store)],
        )
        with pytest.raises(ValueError):
            bus.handle(PlaceOrder("o1", idempotency_key="key-1"))

        # Retry should execute handler again
        result = bus.handle(PlaceOrder("o1", idempotency_key="key-1"))
        assert result == "success"
        assert call_count[0] == 2

    def test_via_bootstrap_shortcut(self):
        call_count = [0]

        def handler(cmd):
            call_count[0] += 1
            return "done"

        store = InMemoryIdempotencyStore()
        bus = bootstrap(
            uow=FakeUoW(),
            commands={PlaceOrder: handler},
            middleware=[IdempotencyMiddleware(store)],
        )
        bus.handle(PlaceOrder("o1", idempotency_key="k1"))
        bus.handle(PlaceOrder("o1", idempotency_key="k1"))
        assert call_count[0] == 1

    def test_composable_with_other_middleware(self):
        log = []

        def logging_mw(message, next):
            log.append("before")
            result = next(message)
            log.append("after")
            return result

        call_count = [0]

        def handler(cmd):
            call_count[0] += 1
            return "ok"

        store = InMemoryIdempotencyStore()
        bus = MessageBus(
            uow=FakeUoW(),
            event_handlers={},
            command_handlers={PlaceOrder: handler},
            middleware=[logging_mw, IdempotencyMiddleware(store)],
        )

        bus.handle(PlaceOrder("o1", idempotency_key="k1"))
        assert log == ["before", "after"]
        assert call_count[0] == 1

        log.clear()
        bus.handle(PlaceOrder("o1", idempotency_key="k1"))
        # Logging still runs, but handler doesn't
        assert log == ["before", "after"]
        assert call_count[0] == 1
