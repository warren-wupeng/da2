"""Repository for event-sourced entities.

Bridges ``EventStore`` and ``EventSourcedEntity`` so they work with the
existing ``UnitOfWork`` / ``MessageBus`` infrastructure.  Optionally uses
a ``SnapshotStore`` to avoid replaying the full event stream.
"""

from __future__ import annotations

from typing import Any, Callable, Generic, Type, TypeVar

from .event import Event
from .event_sourced import EventSourcedEntity
from .event_store import EventStore
from .exceptions import EntityNotFound
from .snapshot import SnapshotStore
from .upcaster import UpcasterChain

T = TypeVar("T", bound=EventSourcedEntity)


class EventSourcedRepository(Generic[T]):
    """Load and save event-sourced aggregates via an EventStore.

    Optionally accepts a ``snapshot_store`` and ``snapshot_interval``
    to periodically snapshot state and speed up future loads.

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
        snapshot_store: SnapshotStore | None = None,
        snapshot_interval: int = 0,
        upcaster_chain: UpcasterChain | None = None,
    ) -> None:
        self._store = event_store
        self._entity_cls = entity_cls
        self._event_registry = event_registry
        self._add_seen = add_seen
        self._snapshot_store = snapshot_store
        self._snapshot_interval = snapshot_interval
        self._upcaster_chain = upcaster_chain

    def get(self, identity: Any) -> T:
        """Load an aggregate, using snapshot if available.

        Raises ``EntityNotFound`` if no events (and no snapshot) exist.
        """
        snapshot = None
        if self._snapshot_store is not None:
            snapshot = self._snapshot_store.load(identity)

        if snapshot is not None:
            stored_events = self._store.load_since(identity, snapshot.version)
            domain_events = [
                self._deserialize(se.event_type, se.data)
                for se in stored_events
            ]
            entity = self._entity_cls.from_snapshot(
                identity, snapshot, domain_events
            )
        else:
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
        """Persist pending events and optionally take a snapshot.

        A snapshot is taken when ``snapshot_interval > 0`` and the entity's
        version crosses an interval boundary.
        """
        pending = entity.pending_events
        if not pending:
            return
        expected_version = entity.version - len(pending)
        self._store.append(entity.identity, pending, expected_version)
        entity.clear_pending_events()

        if self._should_snapshot(expected_version, entity.version):
            self._snapshot_store.save(entity.take_snapshot())  # type: ignore[union-attr]

    def _should_snapshot(self, old_version: int, new_version: int) -> bool:
        if self._snapshot_store is None or self._snapshot_interval <= 0:
            return False
        old_bucket = old_version // self._snapshot_interval
        new_bucket = new_version // self._snapshot_interval
        return new_bucket > old_bucket

    def _deserialize(self, event_type: str, data: dict) -> Event:
        if self._upcaster_chain is not None:
            event_type, data = self._upcaster_chain.upcast(event_type, data)
        cls = self._event_registry.get(event_type)
        if cls is None:
            registered = list(self._event_registry.keys())
            raise KeyError(
                f"Unknown event type {event_type!r}. "
                f"Register it in event_registry. Known types: {registered}"
            )
        return cls.from_dict(data)
