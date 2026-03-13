"""da2 — Python DDD and Event-Driven Architecture framework."""

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
from .exceptions import EntityNotFound

__all__ = [
    "Entity",
    "Identity",
    "Description",
    "Command",
    "Event",
    "UnitOfWork",
    "UnitOfWorkAsync",
    "MessageBus",
    "MessageBusAsync",
    "Bootstrap",
    "BootstrapAsync",
    "Repository",
    "RepositoryAsync",
    "InMemoryRepository",
    "Container",
    "EntityNotFound",
]

__version__ = "0.2.0"
