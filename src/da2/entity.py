from __future__ import annotations

import abc
from typing import Any, Generic, TypeVar

from .event import Event

Identity = TypeVar("Identity")
Description = TypeVar("Description")


class Entity(abc.ABC, Generic[Identity, Description]):
    """Base class for domain entities with identity, description, and events.

    An entity has a unique identity, a description (value object holding
    its data), and can raise domain events via ``raise_event()``.

    Example::

        from pydantic import BaseModel
        from da2 import Entity, Event

        class UserProfile(BaseModel):
            name: str
            email: str

        class UserRenamed(Event):
            def __init__(self, user_id: str, new_name: str):
                self.user_id = user_id
                self.new_name = new_name

        class User(Entity[str, UserProfile]):
            def rename(self, new_name: str) -> None:
                self._desc = self.desc.model_copy(update={"name": new_name})
                self.raise_event(UserRenamed(self.identity, new_name))

        user = User(identity="u-1", desc=UserProfile(name="Alice", email="a@b.com"))
        user.rename("Bob")
        assert user.desc.name == "Bob"
        assert len(user.events) == 1
    """

    def __init__(
        self,
        identity: Identity,
        desc: Description,
        created_at: int = 0,
        updated_at: int = 0,
    ) -> None:
        self._identity = identity
        self._desc = desc
        self._events: list[Event] = []
        self.created_at = created_at
        self.updated_at = updated_at

    @property
    def identity(self) -> Identity:
        """The unique identifier of this entity."""
        return self._identity

    @property
    def desc(self) -> Description:
        """The description (value object) holding entity data."""
        return self._desc

    @property
    def events(self) -> list[Event]:
        """Pending domain events raised by this entity."""
        return self._events

    def raise_event(self, event: Event) -> None:
        """Append a domain event to this entity's pending event list."""
        self._events.append(event)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(identity={self.identity})"

    class NotFound(Exception):
        """Raised when an entity cannot be found by its identity."""
        pass
