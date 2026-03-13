import abc
from typing import Generic, TypeVar

Identity = TypeVar("Identity")
Description = TypeVar("Description")


class Entity(abc.ABC, Generic[Identity, Description]):

    def __init__(
        self,
        identity: Identity,
        desc: Description,
        created_at: int = 0,
        updated_at: int = 0,
    ):
        self._identity = identity
        self._desc = desc
        self._events: list = []
        self.created_at = created_at
        self.updated_at = updated_at

    @property
    def identity(self) -> Identity:
        return self._identity

    @property
    def desc(self) -> Description:
        return self._desc

    @property
    def events(self) -> list:
        return self._events

    def raise_event(self, event) -> None:
        self._events.append(event)

    def __repr__(self):
        return f"{self.__class__.__name__}(identity={self.identity})"

    class NotFound(Exception):
        pass
