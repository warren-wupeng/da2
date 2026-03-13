from __future__ import annotations

import abc
from typing import Any, TypeVar, Generic, Callable

from .entity import Entity

T = TypeVar("T", bound=Entity)


class Repository(abc.ABC, Generic[T]):
    """Abstract base for synchronous repositories.

    Provides the standard persistence operations: get, add, update, delete.
    Pass ``add_seen`` (from UnitOfWork) to auto-track retrieved/added entities.

    Example::

        from da2 import Entity, Repository

        class User(Entity[str, dict]):
            pass

        class UserRepo(Repository[User]):
            def __init__(self):
                super().__init__()
                self._db = {}

            def get(self, identity: str) -> User:
                return self._db[identity]

            def add(self, entity: User) -> None:
                self._db[entity.identity] = entity

            def update(self, entity: User) -> None:
                self._db[entity.identity] = entity

            def delete(self, identity: str) -> None:
                del self._db[identity]
    """

    def __init__(self, add_seen: Callable[[Entity], None] | None = None) -> None:
        self._add_seen = add_seen

    @abc.abstractmethod
    def get(self, identity: Any) -> T:
        raise NotImplementedError

    @abc.abstractmethod
    def add(self, entity: T) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def update(self, entity: T) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def delete(self, identity: Any) -> None:
        raise NotImplementedError

    def _track(self, entity: T) -> T:
        """Register entity with UoW for event collection."""
        if self._add_seen and entity is not None:
            self._add_seen(entity)
        return entity


class InMemoryRepository(Repository[T]):
    """Dict-backed repository for testing and prototyping.

    Example::

        from da2 import Entity, InMemoryRepository

        class Task(Entity[str, dict]):
            pass

        repo = InMemoryRepository[Task]()
        repo.add(Task(identity="t-1", desc={"title": "Write tests"}))

        task = repo.get("t-1")
        assert task.desc["title"] == "Write tests"

        all_tasks = repo.find()
        assert len(all_tasks) == 1
    """

    def __init__(self, add_seen: Callable[[Entity], None] | None = None) -> None:
        super().__init__(add_seen)
        self._store: dict[Any, T] = {}

    def get(self, identity: Any) -> T:
        entity = self._store.get(identity)
        if entity is None:
            raise Entity.NotFound(
                f"Entity with identity={identity!r} not found. "
                f"Available: {list(self._store.keys())[:5]}"
            )
        return self._track(entity)

    def add(self, entity: T) -> None:
        self._store[entity.identity] = entity
        self._track(entity)

    def update(self, entity: T) -> None:
        self._store[entity.identity] = entity

    def delete(self, identity: Any) -> None:
        self._store.pop(identity, None)

    def find(self, predicate: Callable[[T], bool] | None = None) -> list[T]:
        """Return entities matching predicate, or all if predicate is None."""
        if predicate is None:
            return list(self._store.values())
        return [e for e in self._store.values() if predicate(e)]

    def __len__(self) -> int:
        return len(self._store)
