import abc
from typing import Optional, Generic, TypeVar, Any
from .entity import Entity, Description
from .repoAsync import RepoAsync

ID = TypeVar("ID")
E = TypeVar("E", bound=Entity[Any, Any])
RD = TypeVar("RD")


class HasManyAsync(RepoAsync, abc.ABC, Generic[ID, E]):

    async def relateEntity(self, entity: E, relationDesc: Optional[RD] = None) -> E:
        raise NotImplementedError

    async def updateEntity(self, entity: E) -> E:
        pass

    async def deleteEntity(self, identity: ID) -> None:
        pass

    async def countEntities(self) -> int:
        pass

    async def findEntities(self, query: dict) -> list[E]:
        pass

    async def listEntities(self) -> list[E]:
        pass

    async def getEntity(self, identity: ID, forUpdate=False) -> Optional[E]:
        raise NotImplementedError

    async def listEntitiesWithRelationDesc(self) -> list[tuple[E, Description]]:
        raise NotImplementedError

    async def getRelationDesc(self, identity: ID) -> Optional[Description]:
        raise NotImplementedError


