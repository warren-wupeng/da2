import abc
from typing import Union, Generic, TypeVar

Identity = TypeVar('Identity')
Description = TypeVar('Description')


class Entity(abc.ABC, Generic[Identity, Description]):

    def __init__(self, identity: Identity, desc: Description, createAt: int = 0,
                 updateAt: int = 0):
        self._identity = identity
        self._desc = desc
        self._events = []
        self.createAt = createAt
        self.updateAt = updateAt

    @property
    def identity(self) -> Identity:
        return self._identity

    @property
    def desc(self) -> Description:
        return self._desc

    @property
    def events(self):
        return self._events
    
    def raise_event(self, event):
        self._events.append(event)

    class EntityNotFound(Exception):
        pass

    def __repr__(self):
        return f'{self.__class__.__name__}(identity={self.identity})'
