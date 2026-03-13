import abc
from typing import TypeVar, Generic, Callable

from .entity import Entity, Identity, Description

T = TypeVar("T", bound=Entity)


class RepositoryAsync(abc.ABC, Generic[T]):

    def __init__(self, add_seen: Callable[[Entity], None] | None = None):
        self._add_seen = add_seen

    @abc.abstractmethod
    async def get(self, identity: Identity) -> T:
        raise NotImplementedError

    @abc.abstractmethod
    async def add(self, entity: T) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    async def update(self, entity: T) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    async def delete(self, identity: Identity) -> None:
        raise NotImplementedError

    def _track(self, entity: T) -> T:
        if self._add_seen and entity is not None:
            self._add_seen(entity)
        return entity
