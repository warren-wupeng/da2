import abc

from utils.logger import logger
from .entity import Entity


class UnitOfWorkAsync(abc.ABC):

    seen: dict[int, Entity]

    async def __aenter__(self):
        self.seen = dict()
        await self._enter()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        try:
            if exc_type:
                logger.error(f"{exc_type=}")
                logger.error("Exception occurred, rolling back")
                await self.rollback()
        finally:
            await self._exit(exc_type, exc_val, exc_tb)

    async def commit(self):
        await self._commit()

    def addSeen(self, entity: Entity):
        self.seen[entity.identity] = entity

    def collect_new_events(self):
        # auth
        for _, entity in self.seen.items():
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
