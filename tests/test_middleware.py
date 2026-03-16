"""Tests for MessageBus middleware pipeline."""

import pytest
from da2 import Command, Event, MessageBus, UnitOfWork, Bootstrap, bootstrap


class Ping(Command):
    def __init__(self, value: str):
        self.value = value


class Pong(Event):
    def __init__(self, value: str):
        self.value = value


class FakeUoW(UnitOfWork):
    def __init__(self):
        self.committed = False
        self.seen = {}

    def _enter(self):
        pass

    def _commit(self):
        self.committed = True

    def rollback(self):
        pass


def make_bus(handler, middleware=None, event_handlers=None):
    return MessageBus(
        uow=FakeUoW(),
        event_handlers=event_handlers or {},
        command_handlers={Ping: handler},
        middleware=middleware,
    )


class TestMiddlewarePipeline:

    def test_no_middleware_works_as_before(self):
        results = []

        def handler(cmd):
            results.append(cmd.value)
            return f"ok:{cmd.value}"

        bus = make_bus(handler)
        result = bus.handle(Ping(value="hello"))
        assert results == ["hello"]
        assert result == "ok:hello"

    def test_single_middleware_wraps_handle(self):
        log = []

        def logging_mw(message, next):
            log.append(f"before:{type(message).__name__}")
            result = next(message)
            log.append(f"after:{type(message).__name__}")
            return result

        def handler(cmd):
            log.append("handler")
            return "done"

        bus = make_bus(handler, middleware=[logging_mw])
        result = bus.handle(Ping(value="x"))
        assert log == ["before:Ping", "handler", "after:Ping"]
        assert result == "done"

    def test_multiple_middleware_execute_in_order(self):
        log = []

        def mw_a(message, next):
            log.append("a:before")
            result = next(message)
            log.append("a:after")
            return result

        def mw_b(message, next):
            log.append("b:before")
            result = next(message)
            log.append("b:after")
            return result

        def handler(cmd):
            log.append("handler")
            return 42

        bus = make_bus(handler, middleware=[mw_a, mw_b])
        result = bus.handle(Ping(value="x"))
        # a wraps b wraps handler: a:before -> b:before -> handler -> b:after -> a:after
        assert log == ["a:before", "b:before", "handler", "b:after", "a:after"]
        assert result == 42

    def test_middleware_can_modify_result(self):
        def doubler(message, next):
            result = next(message)
            return result * 2

        def handler(cmd):
            return 5

        bus = make_bus(handler, middleware=[doubler])
        assert bus.handle(Ping(value="x")) == 10

    def test_middleware_can_short_circuit(self):
        log = []

        def blocker(message, next):
            log.append("blocked")
            return "rejected"

        def handler(cmd):
            log.append("handler")
            return "ok"

        bus = make_bus(handler, middleware=[blocker])
        result = bus.handle(Ping(value="x"))
        assert result == "rejected"
        assert log == ["blocked"]  # handler never called

    def test_middleware_can_catch_exceptions(self):
        def error_catcher(message, next):
            try:
                return next(message)
            except Exception as e:
                return f"caught:{e}"

        def handler(cmd):
            raise ValueError("boom")

        bus = make_bus(handler, middleware=[error_catcher])
        result = bus.handle(Ping(value="x"))
        assert result == "caught:boom"

    def test_middleware_with_retry(self):
        attempts = []

        def retry_mw(message, next):
            for attempt in range(3):
                try:
                    return next(message)
                except ValueError:
                    attempts.append(attempt)
            return "failed"

        call_count = [0]

        def handler(cmd):
            call_count[0] += 1
            if call_count[0] < 3:
                raise ValueError("not yet")
            return "success"

        bus = make_bus(handler, middleware=[retry_mw])
        result = bus.handle(Ping(value="x"))
        assert result == "success"
        assert attempts == [0, 1]

    def test_middleware_via_bootstrap(self):
        log = []

        def mw(message, next):
            log.append("mw")
            return next(message)

        def handler(cmd):
            return "ok"

        b = Bootstrap(
            uow=FakeUoW(),
            command_handlers={Ping: handler},
            event_handlers={},
            middleware=[mw],
        )
        bus = b.create_message_bus()
        result = bus.handle(Ping(value="x"))
        assert log == ["mw"]
        assert result == "ok"

    def test_middleware_via_bootstrap_shortcut(self):
        log = []

        def mw(message, next):
            log.append("mw")
            return next(message)

        def handler(cmd):
            return "ok"

        bus = bootstrap(
            uow=FakeUoW(),
            commands={Ping: handler},
            middleware=[mw],
        )
        result = bus.handle(Ping(value="x"))
        assert log == ["mw"]
        assert result == "ok"

    def test_handle_returns_command_result(self):
        """MessageBus.handle() now returns the command handler's result."""
        def handler(cmd):
            return {"status": "created", "value": cmd.value}

        bus = make_bus(handler)
        result = bus.handle(Ping(value="test"))
        assert result == {"status": "created", "value": "test"}

    def test_handle_returns_none_for_events(self):
        log = []

        def event_handler(event):
            log.append(event.value)

        bus = MessageBus(
            uow=FakeUoW(),
            event_handlers={Pong: [event_handler]},
            command_handlers={},
        )
        result = bus.handle(Pong(value="x"))
        assert result is None
        assert log == ["x"]
