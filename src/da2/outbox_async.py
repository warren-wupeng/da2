"""Async Transactional Outbox for reliable event publishing.

Async counterpart of :mod:`da2.outbox`. See that module for the full
pattern description.

Example::

    from da2.outbox_async import InMemoryOutboxAsync, OutboxRelayAsync

    outbox = InMemoryOutboxAsync()
    relay = OutboxRelayAsync(outbox, publisher=my_async_publisher)

    await outbox.store(stored_events)
    count = await relay.poll_and_publish()
"""

from __future__ import annotations

import abc
from typing import Any, Callable, Awaitable

from .event_store import StoredEvent
from .outbox import OutboxEntry


class OutboxAsync(abc.ABC):
    """Abstract async outbox store."""

    @abc.abstractmethod
    async def store(self, events: list[StoredEvent]) -> list[OutboxEntry]:
        raise NotImplementedError

    @abc.abstractmethod
    async def fetch_unpublished(self, limit: int = 100) -> list[OutboxEntry]:
        raise NotImplementedError

    @abc.abstractmethod
    async def mark_published(self, entry_ids: list[str]) -> None:
        raise NotImplementedError


class InMemoryOutboxAsync(OutboxAsync):
    """In-memory async outbox for testing."""

    def __init__(self) -> None:
        self._entries: list[OutboxEntry] = []

    async def store(self, events: list[StoredEvent]) -> list[OutboxEntry]:
        entries = [OutboxEntry.from_stored_event(se) for se in events]
        self._entries.extend(entries)
        return entries

    async def fetch_unpublished(self, limit: int = 100) -> list[OutboxEntry]:
        return [e for e in self._entries if not e.published][:limit]

    async def mark_published(self, entry_ids: list[str]) -> None:
        id_set = set(entry_ids)
        for entry in self._entries:
            if entry.id in id_set:
                entry.published = True

    def __len__(self) -> int:
        return len(self._entries)

    @property
    def unpublished_count(self) -> int:
        return sum(1 for e in self._entries if not e.published)


class OutboxRelayAsync:
    """Async relay that polls and publishes outbox entries.

    Args:
        outbox: The async outbox store to poll.
        publisher: Async callable that publishes entries.
            Signature: ``async (entries: list[OutboxEntry]) -> None``.
        batch_size: Maximum entries per poll. Default 100.
    """

    def __init__(
        self,
        outbox: OutboxAsync,
        publisher: Callable[[list[OutboxEntry]], Awaitable[None]],
        batch_size: int = 100,
    ) -> None:
        self._outbox = outbox
        self._publisher = publisher
        self._batch_size = batch_size

    async def poll_and_publish(self) -> int:
        """Fetch, publish, and mark as published. Returns count published."""
        entries = await self._outbox.fetch_unpublished(limit=self._batch_size)
        if not entries:
            return 0
        await self._publisher(entries)
        await self._outbox.mark_published([e.id for e in entries])
        return len(entries)
