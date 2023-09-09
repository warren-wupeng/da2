from typing import Generic, Callable, Type

from .entity import Entity, Identity, Description
from .repo import Repository, T


class RepoAsync(Generic[T]):
    entityClass: Callable[[Identity, Description], T]
    entityDescType: Type[Description]

    async def getEntity(self, identity: Identity, forUpdate=False) -> T:
        raise NotImplementedError

    def addEntity(self, desc: Description) -> T:
        raise NotImplementedError

    def _injectRelatedObject(self, entity: T) -> T:
        raise NotImplementedError

    async def updateEntity(self, entity: Entity, updateAtInclude: bool=True):
        raise NotImplementedError

    async def deleteEntity(self, identifier: Identity) -> None:
        pass

