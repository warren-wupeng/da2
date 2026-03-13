import pytest
from da2 import Command, Event, UnitOfWorkAsync, MessageBusAsync, Entity


class Ping(Command):
    def __init__(self, value: str):
        self.value = value


class Ponged(Event):
    def __init__(self, value: str):
        self.value = value


class Ball(Entity[str, dict]):
    pass


class FakeUnitOfWorkAsync(UnitOfWorkAsync):

    def __init__(self):
        self.committed = False

    async def _enter(self):
        pass

    async def _commit(self):
        self.committed = True

    async def rollback(self):
        pass

    async def _exit(self, exc_type, exc_val, exc_tb):
        pass


class TestMessageBusAsync:

    @pytest.mark.asyncio
    async def test_handle_command(self):
        results = []
        uow = FakeUnitOfWorkAsync()
        uow.seen = {}

        async def handle_ping(cmd: Ping):
            results.append(f"pong:{cmd.value}")
            return f"result:{cmd.value}"

        bus = MessageBusAsync(
            uow=uow,
            event_handlers={},
            command_handlers={Ping: handle_ping},
        )
        result = await bus.handle(Ping(value="hello"))
        assert results == ["pong:hello"]
        assert result == "result:hello"

    @pytest.mark.asyncio
    async def test_command_triggers_event(self):
        results = []
        uow = FakeUnitOfWorkAsync()
        uow.seen = {}

        ball = Ball(identity="b1", desc={})
        uow.add_seen(ball)

        async def handle_ping(cmd: Ping):
            ball.raise_event(Ponged(value=cmd.value))

        async def on_ponged(event: Ponged):
            results.append(f"ponged:{event.value}")

        bus = MessageBusAsync(
            uow=uow,
            event_handlers={Ponged: [on_ponged]},
            command_handlers={Ping: handle_ping},
        )
        await bus.handle(Ping(value="test"))
        assert results == ["ponged:test"]

    @pytest.mark.asyncio
    async def test_bus_event_listeners(self):
        events_log = []
        uow = FakeUnitOfWorkAsync()
        uow.seen = {}

        async def handle_ping(cmd: Ping):
            return "ok"

        bus = MessageBusAsync(
            uow=uow,
            event_handlers={},
            command_handlers={Ping: handle_ping},
        )

        @bus.on_bus_event("handle_success")
        def log_success(event_type, message, handler_name, reason):
            events_log.append(f"{event_type}:{handler_name}")

        await bus.handle(Ping(value="x"))
        assert len(events_log) == 1
        assert "handle_success:" in events_log[0]
