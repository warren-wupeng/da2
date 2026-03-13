import abc
from typing import TypeVar, Generic, Callable

from .entity import Entity

T = TypeVar("T", bound=Entity)


class Repository(abc.ABC, Generic[T]):

    def __init__(self, add_seen: Callable[[Entity], None] | None = None):
        self._add_seen = add_seen

    @abc.abstractmethod
    def get(self, identity) -> T:
        raise NotImplementedError

    @abc.abstractmethod
    def add(self, entity: T) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def update(self, entity: T) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def delete(self, identity) -> None:
        raise NotImplementedError

    def _track(self, entity: T) -> T:
        if self._add_seen and entity is not None:
            self._add_seen(entity)
        return entity


class InMemoryRepository(Repository[T]):

    def __init__(self, add_seen: Callable[[Entity], None] | None = None):
        super().__init__(add_seen)
        self._store: dict = {}

    def get(self, identity) -> T:
        entity = self._store.get(identity)
        if entity is None:
            raise Entity.NotFound(f"Entity {identity} not found")
        return self._track(entity)

    def add(self, entity: T) -> None:
        self._store[entity.identity] = entity
        self._track(entity)

    def update(self, entity: T) -> None:
        self._store[entity.identity] = entity

    def delete(self, identity) -> None:
        self._store.pop(identity, None)

    def find(self, predicate: Callable[[T], bool] | None = None) -> list[T]:
        if predicate is None:
            return list(self._store.values())
        return [e for e in self._store.values() if predicate(e)]

    def __len__(self) -> int:
        return len(self._store)
