"""Async append-only event store for event sourcing.

Provides ``EventStoreAsync`` (abstract) and ``InMemoryEventStoreAsync``
(for testing).  Async counterpart of ``event_store.py``.
"""

from __future__ import annotations

import abc
import time
from typing import Any

from .event import Event
from .event_store import StoredEvent
from .exceptions import ConcurrencyError


class EventStoreAsync(abc.ABC):
    """Abstract async append-only event store with optimistic concurrency.

    Subclass and implement ``append()`` and ``load()`` for your async
    storage backend (asyncpg, aiosqlite, etc.).

    Example::

        class AsyncPgEventStore(EventStoreAsync):
            async def append(self, aggregate_id, events, expected_version):
                async with self.pool.acquire() as conn:
                    ...

            async def load(self, aggregate_id):
                async with self.pool.acquire() as conn:
                    ...
    """

    @abc.abstractmethod
    async def append(
        self,
        aggregate_id: Any,
        events: list[Event],
        expected_version: int,
    ) -> None:
        """Append events. Raises ``ConcurrencyError`` on version mismatch."""
        raise NotImplementedError

    @abc.abstractmethod
    async def load(self, aggregate_id: Any) -> list[StoredEvent]:
        """Load all stored events for *aggregate_id*, ordered by version."""
        raise NotImplementedError

    async def load_since(self, aggregate_id: Any, after_version: int) -> list[StoredEvent]:
        """Load events with version > *after_version*.

        Default implementation filters ``load()``; override for efficiency.
        """
        all_events = await self.load(aggregate_id)
        return [se for se in all_events if se.version > after_version]

    async def load_all(self, after_position: int = 0) -> list[StoredEvent]:
        """Load all events across all aggregates, ordered by global position.

        Returns events with ``position > after_position``.
        Override in subclasses for efficient database queries.
        Default raises ``NotImplementedError``.
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} does not support load_all(). "
            f"Use InMemoryEventStoreAsync or a store that tracks global position."
        )


class InMemoryEventStoreAsync(EventStoreAsync):
    """Async dict-backed event store for testing and prototyping.

    Example::

        from da2 import Event
        from da2.event_store_async import InMemoryEventStoreAsync

        class ItemAdded(Event):
            def __init__(self, name: str):
                self.name = name

        store = InMemoryEventStoreAsync()
        await store.append("cart-1", [ItemAdded(name="Widget")], expected_version=0)

        stored = await store.load("cart-1")
        assert len(stored) == 1
    """

    def __init__(self) -> None:
        self._streams: dict[Any, list[StoredEvent]] = {}
        self._all_events: list[StoredEvent] = []
        self._global_position: int = 0

    async def append(
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
            self._global_position += 1
            stored = StoredEvent(
                aggregate_id=aggregate_id,
                event_type=type(event).__name__,
                data=event.to_dict(),
                version=current_version + i + 1,
                timestamp=time.time(),
                position=self._global_position,
            )
            stream.append(stored)
            self._all_events.append(stored)

    async def load(self, aggregate_id: Any) -> list[StoredEvent]:
        return list(self._streams.get(aggregate_id, []))

    async def load_since(self, aggregate_id: Any, after_version: int) -> list[StoredEvent]:
        stream = self._streams.get(aggregate_id, [])
        return [se for se in stream if se.version > after_version]

    async def load_all(self, after_position: int = 0) -> list[StoredEvent]:
        """Load all events across all aggregates, ordered by global position."""
        if after_position == 0:
            return list(self._all_events)
        return [se for se in self._all_events if se.position > after_position]
