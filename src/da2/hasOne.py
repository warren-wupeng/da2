import abc
from typing import Generic, TypeVar, Any
from .entity import Entity

E = TypeVar("E", bound=Entity[Any, Any])
ID = TypeVar("ID")


class HasOne(abc.ABC, Generic[ID, E]):

    def get(self) -> E:
        raise NotImplementedError
