import abc
from typing import Optional, Generic, TypeVar, Any
from .entity import Entity

from .many import Many

ID = TypeVar("ID")
E = TypeVar("E", bound=Entity[Any, Any])


class HasMany(abc.ABC, Generic[ID, E]):

    def list(self) -> Many[E]:
        raise NotImplementedError

    def getById(self, identifier: ID) -> Optional[E]:
        raise NotImplementedError
