"""da2 -- Python DDD and Event-Driven Architecture framework.

Quick start::

    from da2 import Entity, Command, Event, UnitOfWork, bootstrap

    class CreateUser(Command):
        def __init__(self, name: str):
            self.name = name

    class UserCreated(Event):
        def __init__(self, name: str):
            self.name = name

    class User(Entity[str, dict]):
        pass

    class MyUoW(UnitOfWork):
        def _enter(self): pass
        def _commit(self): pass
        def rollback(self): pass

    def handle_create(cmd: CreateUser, uow):
        print(f"Created {cmd.name}")

    bus = bootstrap(
        uow=MyUoW(),
        commands={CreateUser: handle_create},
        events={},
    )
    bus.handle(CreateUser(name="Alice"))
"""

from __future__ import annotations

from typing import Any, Type, Callable

from .entity import Entity, Identity, Description
from .command import Command
from .event import Event
from .uow import UnitOfWork
from .uow_async import UnitOfWorkAsync
from .message_bus import MessageBus
from .message_bus_async import MessageBusAsync
from .bootstrap import Bootstrap
from .bootstrap_async import BootstrapAsync
from .repository import Repository, InMemoryRepository
from .repository_async import RepositoryAsync
from .container import Container
from .exceptions import EntityNotFound, ConcurrencyError
from .event_store import EventStore, InMemoryEventStore, StoredEvent
from .event_store_async import EventStoreAsync, InMemoryEventStoreAsync
from .event_sourced import EventSourcedEntity
from .event_sourced_repository import EventSourcedRepository
from .event_sourced_repository_async import EventSourcedRepositoryAsync
from .snapshot import Snapshot, SnapshotStore, InMemorySnapshotStore
from .snapshot_async import SnapshotStoreAsync, InMemorySnapshotStoreAsync

__all__ = [
    # Core
    "Entity",
    "Identity",
    "Description",
    "Command",
    "Event",
    # Unit of Work
    "UnitOfWork",
    "UnitOfWorkAsync",
    # Message Bus
    "MessageBus",
    "MessageBusAsync",
    # Bootstrap
    "Bootstrap",
    "BootstrapAsync",
    "bootstrap",
    # Repository (state-based)
    "Repository",
    "RepositoryAsync",
    "InMemoryRepository",
    # Event Sourcing
    "EventSourcedEntity",
    "EventStore",
    "InMemoryEventStore",
    "EventStoreAsync",
    "InMemoryEventStoreAsync",
    "StoredEvent",
    "EventSourcedRepository",
    "EventSourcedRepositoryAsync",
    # Snapshot
    "Snapshot",
    "SnapshotStore",
    "InMemorySnapshotStore",
    "SnapshotStoreAsync",
    "InMemorySnapshotStoreAsync",
    # DI
    "Container",
    # Exceptions
    "EntityNotFound",
    "ConcurrencyError",
]

__version__ = "0.3.1"


def bootstrap(
    uow: UnitOfWork,
    commands: dict[Type[Command], Callable[..., Any]] | None = None,
    events: dict[Type[Event], list[Callable[..., Any]]] | None = None,
    dependencies: dict[str, Any] | None = None,
) -> MessageBus:
    """One-liner to create a wired MessageBus. Shortcut for Bootstrap.

    Example::

        from da2 import bootstrap, Command, UnitOfWork

        bus = bootstrap(
            uow=my_uow,
            commands={CreateUser: handle_create_user},
            events={UserCreated: [send_email, update_stats]},
        )
        bus.handle(CreateUser(name="Alice"))
    """
    return Bootstrap(
        uow=uow,
        command_handlers=commands or {},
        event_handlers=events or {},
        dependencies=dependencies,
    ).create_message_bus()
