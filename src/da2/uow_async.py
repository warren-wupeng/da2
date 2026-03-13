from __future__ import annotations

import abc
import logging
from typing import Any, Generator

from .entity import Entity
from .event import Event

logger = logging.getLogger(__name__)


class UnitOfWorkAsync(abc.ABC):
    """Async Unit of Work -- async context manager version of UnitOfWork.

    Subclass and implement ``_enter()``, ``_commit()``, ``rollback()``,
    and ``_exit()``.

    Example::

        from da2 import UnitOfWorkAsync

        class AppUoW(UnitOfWorkAsync):
            async def _enter(self) -> None:
                self.conn = await get_connection()

            async def _commit(self) -> None:
                await self.conn.commit()

            async def rollback(self) -> None:
                await self.conn.rollback()

            async def _exit(self, exc_type, exc_val, exc_tb) -> None:
                await self.conn.close()

        async with AppUoW() as uow:
            ...
            await uow.commit()
    """

    seen: dict[Any, Entity]

    async def __aenter__(self) -> UnitOfWorkAsync:
        self.seen = {}
        await self._enter()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        try:
            if exc_type:
                logger.error("Exception occurred (%s), rolling back", exc_type)
                await self.rollback()
        finally:
            await self._exit(exc_type, exc_val, exc_tb)

    async def commit(self) -> None:
        """Persist changes."""
        await self._commit()

    def add_seen(self, entity: Entity) -> None:
        """Track an entity so its events are collected by the MessageBus."""
        self.seen[entity.identity] = entity

    def collect_new_events(self) -> Generator[Event, None, None]:
        """Yield and drain all pending events from tracked entities."""
        for entity in self.seen.values():
            while entity.events:
                yield entity.events.pop(0)

    @abc.abstractmethod
    async def _commit(self) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    async def rollback(self) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    async def _enter(self) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    async def _exit(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        raise NotImplementedError
