"""Repository for event-sourced entities.

Bridges ``EventStore`` and ``EventSourcedEntity`` so they work with the
existing ``UnitOfWork`` / ``MessageBus`` infrastructure.
"""

from __future__ import annotations

from typing import Any, Callable, Generic, Type, TypeVar

from .event import Event
from .event_sourced import EventSourcedEntity
from .event_store import EventStore
from .exceptions import EntityNotFound

T = TypeVar("T", bound=EventSourcedEntity)


class EventSourcedRepository(Generic[T]):
    """Load and save event-sourced aggregates via an EventStore.

    Example::

        from da2 import Event
        from da2.event_sourced import EventSourcedEntity
        from da2.event_store import InMemoryEventStore
        from da2.event_sourced_repository import EventSourcedRepository

        class UserRegistered(Event):
            def __init__(self, name: str):
                self.name = name

        class User(EventSourcedEntity[str]):
            def __init__(self, identity: str) -> None:
                super().__init__(identity)
                self.name: str = ""

            @classmethod
            def register(cls, user_id: str, name: str) -> "User":
                user = cls(user_id)
                user._apply_and_record(UserRegistered(name=name))
                return user

            def _when_UserRegistered(self, event: UserRegistered) -> None:
                self.name = event.name

        store = InMemoryEventStore()
        repo = EventSourcedRepository(
            event_store=store,
            entity_cls=User,
            event_registry={"UserRegistered": UserRegistered},
        )

        user = User.register("u-1", "Alice")
        repo.save(user)

        loaded = repo.get("u-1")
        assert loaded.name == "Alice"
    """

    def __init__(
        self,
        event_store: EventStore,
        entity_cls: Type[T],
        event_registry: dict[str, Type[Event]],
        add_seen: Callable[[EventSourcedEntity], None] | None = None,
    ) -> None:
        self._store = event_store
        self._entity_cls = entity_cls
        self._event_registry = event_registry
        self._add_seen = add_seen

    def get(self, identity: Any) -> T:
        """Load an aggregate by replaying its event stream.

        Raises ``EntityNotFound`` if no events exist for *identity*.
        """
        stored_events = self._store.load(identity)
        if not stored_events:
            raise EntityNotFound(
                f"No events found for {self._entity_cls.__name__} "
                f"with identity={identity!r}."
            )
        domain_events = [
            self._deserialize(se.event_type, se.data)
            for se in stored_events
        ]
        entity = self._entity_cls.from_events(identity, domain_events)
        if self._add_seen is not None:
            self._add_seen(entity)
        return entity  # type: ignore[return-value]

    def save(self, entity: T) -> None:
        """Persist pending events to the event store.

        Uses optimistic concurrency: ``expected_version`` is the entity's
        version minus the number of pending events (i.e., the version
        at load time).
        """
        pending = entity.pending_events
        if not pending:
            return
        expected_version = entity.version - len(pending)
        self._store.append(entity.identity, pending, expected_version)
        entity.clear_pending_events()

    def _deserialize(self, event_type: str, data: dict) -> Event:
        cls = self._event_registry.get(event_type)
        if cls is None:
            registered = list(self._event_registry.keys())
            raise KeyError(
                f"Unknown event type {event_type!r}. "
                f"Register it in event_registry. Known types: {registered}"
            )
        return cls.from_dict(data)
