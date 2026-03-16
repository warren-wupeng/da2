"""Tests for async idempotent command handling."""

import pytest
from da2 import Command, MessageBusAsync, UnitOfWorkAsync, BootstrapAsync
from da2.idempotency_async import (
    IdempotencyMiddlewareAsync,
    InMemoryIdempotencyStoreAsync,
)


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


async def test_first_call_executes_handler():
    call_count = [0]

    async def handler(cmd):
        call_count[0] += 1
        return f"order:{cmd.order_id}"

    store = InMemoryIdempotencyStoreAsync()
    bus = MessageBusAsync(
        uow=FakeUoW(),
        event_handlers={},
        command_handlers={PlaceOrder: handler},
        middleware=[IdempotencyMiddlewareAsync(store)],
    )
    result = await bus.handle(PlaceOrder("o1", idempotency_key="k1"))
    assert result == "order:o1"
    assert call_count[0] == 1


async def test_duplicate_returns_cached():
    call_count = [0]

    async def handler(cmd):
        call_count[0] += 1
        return f"order:{cmd.order_id}"

    store = InMemoryIdempotencyStoreAsync()
    mw = IdempotencyMiddlewareAsync(store)
    bus = MessageBusAsync(
        uow=FakeUoW(),
        event_handlers={},
        command_handlers={PlaceOrder: handler},
        middleware=[mw],
    )
    r1 = await bus.handle(PlaceOrder("o1", idempotency_key="k1"))
    r2 = await bus.handle(PlaceOrder("o1", idempotency_key="k1"))
    assert r1 == r2 == "order:o1"
    assert call_count[0] == 1


async def test_no_key_passes_through():
    call_count = [0]

    async def handler(cmd):
        call_count[0] += 1
        return "ok"

    store = InMemoryIdempotencyStoreAsync()
    bus = MessageBusAsync(
        uow=FakeUoW(),
        event_handlers={},
        command_handlers={PlaceOrder: handler},
        middleware=[IdempotencyMiddlewareAsync(store)],
    )
    await bus.handle(PlaceOrder("o1"))
    await bus.handle(PlaceOrder("o1"))
    assert call_count[0] == 2


async def test_exception_not_cached():
    call_count = [0]

    async def handler(cmd):
        call_count[0] += 1
        if call_count[0] == 1:
            raise ValueError("boom")
        return "ok"

    store = InMemoryIdempotencyStoreAsync()
    bus = MessageBusAsync(
        uow=FakeUoW(),
        event_handlers={},
        command_handlers={PlaceOrder: handler},
        middleware=[IdempotencyMiddlewareAsync(store)],
    )
    with pytest.raises(ValueError):
        await bus.handle(PlaceOrder("o1", idempotency_key="k1"))

    result = await bus.handle(PlaceOrder("o1", idempotency_key="k1"))
    assert result == "ok"
    assert call_count[0] == 2


async def test_via_bootstrap_async():
    call_count = [0]

    async def handler(cmd):
        call_count[0] += 1
        return "done"

    store = InMemoryIdempotencyStoreAsync()
    b = BootstrapAsync(
        uow=FakeUoW(),
        command_handlers={PlaceOrder: handler},
        event_handlers={},
        middleware=[IdempotencyMiddlewareAsync(store)],
    )
    bus = b.create_message_bus()
    await bus.handle(PlaceOrder("o1", idempotency_key="k1"))
    await bus.handle(PlaceOrder("o1", idempotency_key="k1"))
    assert call_count[0] == 1


async def test_clear_store():
    call_count = [0]

    async def handler(cmd):
        call_count[0] += 1
        return "ok"

    store = InMemoryIdempotencyStoreAsync()
    bus = MessageBusAsync(
        uow=FakeUoW(),
        event_handlers={},
        command_handlers={PlaceOrder: handler},
        middleware=[IdempotencyMiddlewareAsync(store)],
    )
    await bus.handle(PlaceOrder("o1", idempotency_key="k1"))
    assert call_count[0] == 1

    store.clear()
    await bus.handle(PlaceOrder("o1", idempotency_key="k1"))
    assert call_count[0] == 2  # executed again after clear
