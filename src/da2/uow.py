from __future__ import annotations

import abc
from typing import Any, Generator

from .entity import Entity
from .event import Event


class UnitOfWork(abc.ABC):
    """Synchronous Unit of Work -- tracks entities and collects their events.

    Use as a context manager. Tracks entities via ``add_seen()``, and after
    each handler call the MessageBus drains events via ``collect_new_events()``.

    Subclass and implement ``_enter()``, ``_commit()``, and ``rollback()``.

    Example::

        from da2 import UnitOfWork, InMemoryRepository

        class AppUoW(UnitOfWork):
            def _enter(self) -> None:
                self.users = InMemoryRepository(add_seen=self.add_seen)

            def _commit(self) -> None:
                pass  # persist to database

            def rollback(self) -> None:
                pass

        with AppUoW() as uow:
            uow.users.add(some_user)
            uow.commit()
    """

    seen: dict[Any, Entity]

    def __enter__(self) -> UnitOfWork:
        self.seen = {}
        self._enter()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: Any,
    ) -> None:
        if exc_type:
            self.rollback()

    def commit(self) -> None:
        """Persist changes."""
        self._commit()

    def add_seen(self, entity: Entity) -> None:
        """Track an entity so its events are collected by the MessageBus."""
        self.seen[entity.identity] = entity

    def collect_new_events(self) -> Generator[Event, None, None]:
        """Yield and drain all pending events from tracked entities."""
        for entity in self.seen.values():
            while entity.events:
                yield entity.events.pop(0)

    @abc.abstractmethod
    def _commit(self) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def rollback(self) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def _enter(self) -> None:
        raise NotImplementedError
