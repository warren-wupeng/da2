"""Append-only event store for event sourcing.

Provides ``EventStore`` (abstract) and ``InMemoryEventStore`` (for testing).
Events are stored as ``StoredEvent`` envelopes with aggregate ID, version,
type name, serialized data, and timestamp.
"""

from __future__ import annotations

import abc
import time
from dataclasses import dataclass, field
from typing import Any

from .event import Event
from .exceptions import ConcurrencyError


@dataclass(frozen=True)
class StoredEvent:
    """Immutable envelope that wraps a domain event for persistence.

    Example::

        from da2.event_store import StoredEvent

        stored = StoredEvent(
            aggregate_id="order-1",
            event_type="OrderPlaced",
            data={"total": 99.0},
            version=1,
            timestamp=1700000000.0,
        )
        assert stored.version == 1
    """

    aggregate_id: Any
    event_type: str
    data: dict
    version: int
    timestamp: float = field(default_factory=time.time)


class EventStore(abc.ABC):
    """Abstract append-only event store with optimistic concurrency.

    Subclass and implement ``append()`` and ``load()`` for your storage
    backend (database, file, etc.).

    Example::

        class PostgresEventStore(EventStore):
            def append(self, aggregate_id, events, expected_version):
                # INSERT INTO events ... WHERE version = expected_version
                ...

            def load(self, aggregate_id):
                # SELECT * FROM events WHERE aggregate_id = ...
                ...
    """

    @abc.abstractmethod
    def append(
        self,
        aggregate_id: Any,
        events: list[Event],
        expected_version: int,
    ) -> None:
        """Append events to the stream for *aggregate_id*.

        Raises ``ConcurrencyError`` if the current stream version does
        not equal *expected_version*.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def load(self, aggregate_id: Any) -> list[StoredEvent]:
        """Load all stored events for *aggregate_id*, ordered by version."""
        raise NotImplementedError

    def load_since(self, aggregate_id: Any, after_version: int) -> list[StoredEvent]:
        """Load events with version > *after_version*.

        Default implementation filters ``load()``; override for efficiency.
        """
        return [
            se for se in self.load(aggregate_id)
            if se.version > after_version
        ]


class InMemoryEventStore(EventStore):
    """Dict-backed event store for testing and prototyping.

    Example::

        from da2 import Event
        from da2.event_store import InMemoryEventStore

        class OrderPlaced(Event):
            def __init__(self, total: float):
                self.total = total

        store = InMemoryEventStore()
        store.append("order-1", [OrderPlaced(total=99.0)], expected_version=0)

        stored = store.load("order-1")
        assert len(stored) == 1
        assert stored[0].event_type == "OrderPlaced"
        assert stored[0].data == {"total": 99.0}
    """

    def __init__(self) -> None:
        self._streams: dict[Any, list[StoredEvent]] = {}

    def append(
        self,
        aggregate_id: Any,
        events: list[Event],
        expected_version: int,
    ) -> None:
        stream = self._streams.setdefault(aggregate_id, [])
        current_version = len(stream)
        if current_version != expected_version:
            raise ConcurrencyError(
                f"Expected version {expected_version} for aggregate "
                f"{aggregate_id!r}, but current version is {current_version}. "
                f"Another process may have written events concurrently."
            )
        for i, event in enumerate(events):
            stored = StoredEvent(
                aggregate_id=aggregate_id,
                event_type=type(event).__name__,
                data=event.to_dict(),
                version=current_version + i + 1,
            )
            stream.append(stored)

    def load(self, aggregate_id: Any) -> list[StoredEvent]:
        return list(self._streams.get(aggregate_id, []))

    def load_since(self, aggregate_id: Any, after_version: int) -> list[StoredEvent]:
        stream = self._streams.get(aggregate_id, [])
        return [se for se in stream if se.version > after_version]
