"""Async Projection -- async event-driven read model for CQRS query side."""

from __future__ import annotations

import abc
from typing import Sequence

from .event import Event
from .event_store import StoredEvent
from .event_store_async import EventStoreAsync


class ProjectionAsync(abc.ABC):
    """Async base class for event-driven read models.

    Same convention as :class:`Projection`: define
    ``_on_<EventClassName>(self, event)`` methods.  Handlers may be
    coroutines (``async def``) or plain functions.
    """

    def __init__(self) -> None:
        self._last_version: int = 0
        self._last_position: int = 0

    @property
    def last_version(self) -> int:
        return self._last_version

    @property
    def last_position(self) -> int:
        """Global position of the last event processed (cross-aggregate)."""
        return self._last_position

    async def apply(self, event: Event) -> None:
        handler = getattr(self, f"_on_{type(event).__name__}", None)
        if handler is not None:
            import asyncio
            if asyncio.iscoroutinefunction(handler):
                await handler(event)
            else:
                handler(event)

    async def replay(
        self,
        stored_events: Sequence[StoredEvent],
        event_registry: dict[str, type[Event]] | None = None,
    ) -> None:
        for se in stored_events:
            if event_registry is not None and se.event_type in event_registry:
                cls = event_registry[se.event_type]
                event = cls.from_dict(se.data)
                await self.apply(event)
            self._last_version = se.version

    async def catch_up(
        self,
        stored_events: Sequence[StoredEvent],
        event_registry: dict[str, type[Event]],
    ) -> None:
        for se in stored_events:
            if se.version <= self._last_version:
                continue
            if se.event_type in event_registry:
                cls = event_registry[se.event_type]
                event = cls.from_dict(se.data)
                await self.apply(event)
            self._last_version = se.version

    async def replay_all(
        self,
        event_store: EventStoreAsync,
        event_registry: dict[str, type[Event]],
    ) -> None:
        """Rebuild this projection from the global event stream."""
        stored_events = await event_store.load_all(after_position=0)
        for se in stored_events:
            if se.event_type in event_registry:
                cls = event_registry[se.event_type]
                event = cls.from_dict(se.data)
                await self.apply(event)
            if se.position > 0:
                self._last_position = se.position
            self._last_version = se.version

    async def catch_up_all(
        self,
        event_store: EventStoreAsync,
        event_registry: dict[str, type[Event]],
    ) -> None:
        """Process new events from the global stream since last_position."""
        stored_events = await event_store.load_all(
            after_position=self._last_position,
        )
        for se in stored_events:
            if se.event_type in event_registry:
                cls = event_registry[se.event_type]
                event = cls.from_dict(se.data)
                await self.apply(event)
            if se.position > 0:
                self._last_position = se.position
            self._last_version = se.version
