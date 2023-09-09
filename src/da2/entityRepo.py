from abc import ABC
from typing import Generic, Callable, Type

from .entity import Entity, Identity, Description
from .repo import T


class EntityRepo(Generic[T], ABC):
    entityFactory: Callable[[Identity, Description], T]
    descriptionType: Type[Description]

    async def getEntity(self, identity: Identity) -> T:
        raise NotImplementedError
