import abc
import logging

from .entity import Entity

logger = logging.getLogger(__name__)


class UnitOfWorkAsync(abc.ABC):

    seen: dict

    async def __aenter__(self):
        self.seen = {}
        await self._enter()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        try:
            if exc_type:
                logger.error("Exception occurred (%s), rolling back", exc_type)
                await self.rollback()
        finally:
            await self._exit(exc_type, exc_val, exc_tb)

    async def commit(self):
        await self._commit()

    def add_seen(self, entity: Entity):
        self.seen[entity.identity] = entity

    def collect_new_events(self):
        for entity in self.seen.values():
            while entity.events:
                yield entity.events.pop(0)

    @abc.abstractmethod
    async def _commit(self):
        raise NotImplementedError

    @abc.abstractmethod
    async def rollback(self):
        raise NotImplementedError

    @abc.abstractmethod
    async def _enter(self):
        raise NotImplementedError

    @abc.abstractmethod
    async def _exit(self, exc_type, exc_val, exc_tb):
        raise NotImplementedError
