from __future__ import annotations

import abc
from typing import Any, TypeVar, Generic, Callable

from .entity import Entity

T = TypeVar("T", bound=Entity)


class RepositoryAsync(abc.ABC, Generic[T]):
    """Abstract base for asynchronous repositories.

    Async counterpart of Repository. Implement with your async database driver.

    Example::

        from da2 import Entity, RepositoryAsync

        class User(Entity[str, dict]):
            pass

        class UserRepoAsync(RepositoryAsync[User]):
            async def get(self, identity: str) -> User:
                row = await db.fetch_one("SELECT ...", identity)
                return User(identity=row["id"], desc=row)

            async def add(self, entity: User) -> None:
                await db.execute("INSERT ...", entity.identity, entity.desc)

            async def update(self, entity: User) -> None:
                await db.execute("UPDATE ...", entity.identity, entity.desc)

            async def delete(self, identity: str) -> None:
                await db.execute("DELETE ...", identity)
    """

    def __init__(self, add_seen: Callable[[Entity], None] | None = None) -> None:
        self._add_seen = add_seen

    @abc.abstractmethod
    async def get(self, identity: Any) -> T:
        raise NotImplementedError

    @abc.abstractmethod
    async def add(self, entity: T) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    async def update(self, entity: T) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    async def delete(self, identity: Any) -> None:
        raise NotImplementedError

    def _track(self, entity: T) -> T:
        """Register entity with UoW for event collection."""
        if self._add_seen and entity is not None:
            self._add_seen(entity)
        return entity
