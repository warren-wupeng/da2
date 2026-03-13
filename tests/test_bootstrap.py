import pytest
from da2 import (
    Command, Event, Entity,
    UnitOfWork, UnitOfWorkAsync,
    Bootstrap, BootstrapAsync,
)


class CreateUser(Command):
    def __init__(self, name: str):
        self.name = name


class UserCreated(Event):
    def __init__(self, name: str):
        self.name = name


class User(Entity[str, dict]):
    pass


class FakeUoW(UnitOfWork):
    def __init__(self):
        self.committed = False

    def _enter(self):
        pass

    def _commit(self):
        self.committed = True

    def rollback(self):
        pass


class FakeUoWAsync(UnitOfWorkAsync):
    def __init__(self):
        self.committed = False

    async def _enter(self):
        pass

    async def _commit(self):
        self.committed = True

    async def rollback(self):
        pass

    async def _exit(self, *args):
        pass


class TestBootstrap:

    def test_dependency_injection(self):
        results = []
        uow = FakeUoW()

        def handle_create_user(cmd: CreateUser, uow: UnitOfWork):
            results.append(f"created:{cmd.name}")

        bootstrap = Bootstrap(
            uow=uow,
            event_handlers={},
            command_handlers={CreateUser: handle_create_user},
        )
        bus = bootstrap.create_message_bus()
        bus.uow.seen = {}
        bus.handle(CreateUser(name="Alice"))
        assert results == ["created:Alice"]

    def test_extra_dependencies(self):
        results = []
        uow = FakeUoW()
        email_service = {"sent": []}

        def handle_create_user(cmd: CreateUser, email_service):
            email_service["sent"].append(cmd.name)

        bootstrap = Bootstrap(
            uow=uow,
            event_handlers={},
            command_handlers={CreateUser: handle_create_user},
            dependencies={"email_service": email_service},
        )
        bus = bootstrap.create_message_bus()
        bus.uow.seen = {}
        bus.handle(CreateUser(name="Bob"))
        assert email_service["sent"] == ["Bob"]


class TestBootstrapAsync:

    @pytest.mark.asyncio
    async def test_dependency_injection(self):
        results = []
        uow = FakeUoWAsync()

        async def handle_create_user(cmd: CreateUser, uow: UnitOfWorkAsync):
            results.append(f"created:{cmd.name}")

        bootstrap = BootstrapAsync(
            uow=uow,
            event_handlers={},
            command_handlers={CreateUser: handle_create_user},
        )
        bus = bootstrap.create_message_bus()
        bus.uow.seen = {}
        await bus.handle(CreateUser(name="Alice"))
        assert results == ["created:Alice"]
