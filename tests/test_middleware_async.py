"""Tests for MessageBusAsync middleware pipeline."""

import pytest
from da2 import Command, Event, MessageBusAsync, UnitOfWorkAsync, BootstrapAsync


class Ping(Command):
    def __init__(self, value: str):
        self.value = value


class Pong(Event):
    def __init__(self, value: str):
        self.value = value


class FakeUoW(UnitOfWorkAsync):
    def __init__(self):
        self.committed = False
        self.seen = {}

    async def _enter(self):
        pass

    async def _commit(self):
        self.committed = True

    async def rollback(self):
        pass

    async def _exit(self, *a):
        pass


def make_bus(handler, middleware=None, event_handlers=None):
    return MessageBusAsync(
        uow=FakeUoW(),
        event_handlers=event_handlers or {},
        command_handlers={Ping: handler},
        middleware=middleware,
    )


pytestmark = pytest.mark.asyncio


async def test_no_middleware():
    async def handler(cmd):
        return f"ok:{cmd.value}"

    bus = make_bus(handler)
    result = await bus.handle(Ping(value="hi"))
    assert result == "ok:hi"


async def test_single_middleware():
    log = []

    async def logging_mw(message, next):
        log.append(f"before:{type(message).__name__}")
        result = await next(message)
        log.append(f"after:{type(message).__name__}")
        return result

    async def handler(cmd):
        log.append("handler")
        return "done"

    bus = make_bus(handler, middleware=[logging_mw])
    result = await bus.handle(Ping(value="x"))
    assert log == ["before:Ping", "handler", "after:Ping"]
    assert result == "done"


async def test_multiple_middleware_order():
    log = []

    async def mw_a(message, next):
        log.append("a:before")
        result = await next(message)
        log.append("a:after")
        return result

    async def mw_b(message, next):
        log.append("b:before")
        result = await next(message)
        log.append("b:after")
        return result

    async def handler(cmd):
        log.append("handler")
        return 99

    bus = make_bus(handler, middleware=[mw_a, mw_b])
    result = await bus.handle(Ping(value="x"))
    assert log == ["a:before", "b:before", "handler", "b:after", "a:after"]
    assert result == 99


async def test_middleware_short_circuit():
    async def blocker(message, next):
        return "blocked"

    async def handler(cmd):
        return "should not reach"

    bus = make_bus(handler, middleware=[blocker])
    result = await bus.handle(Ping(value="x"))
    assert result == "blocked"


async def test_middleware_error_handling():
    async def catcher(message, next):
        try:
            return await next(message)
        except ValueError as e:
            return f"caught:{e}"

    async def handler(cmd):
        raise ValueError("boom")

    bus = make_bus(handler, middleware=[catcher])
    result = await bus.handle(Ping(value="x"))
    assert result == "caught:boom"


async def test_middleware_via_bootstrap_async():
    log = []

    async def mw(message, next):
        log.append("mw")
        return await next(message)

    async def handler(cmd):
        return "ok"

    b = BootstrapAsync(
        uow=FakeUoW(),
        command_handlers={Ping: handler},
        event_handlers={},
        middleware=[mw],
    )
    bus = b.create_message_bus()
    result = await bus.handle(Ping(value="x"))
    assert log == ["mw"]
    assert result == "ok"


async def test_lifecycle_hooks_still_work_with_middleware():
    """Middleware should not interfere with existing lifecycle hooks."""
    hook_log = []

    async def mw(message, next):
        return await next(message)

    async def handler(cmd):
        return "ok"

    bus = make_bus(handler, middleware=[mw])

    @bus.on_bus_event("handle_success")
    def on_success(event_type, message, handler_name, reason):
        hook_log.append(f"success:{handler_name}")

    await bus.handle(Ping(value="x"))
    assert len(hook_log) == 1
    assert hook_log[0].startswith("success:")
