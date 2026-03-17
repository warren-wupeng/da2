"""Transactional Outbox for reliable event publishing.

The Outbox Pattern ensures domain events are reliably published to external
consumers (message brokers, other bounded contexts) without losing events on
crash. Events are first stored in the outbox alongside the aggregate write,
then a relay process polls and publishes them.

Flow::

    EventStore.append()  -->  Outbox.store()       (same transaction / call)
                              OutboxRelay.poll()    (separate process/tick)
                              publisher(entries)    (your callback)
                              Outbox.mark_published()

Example::

    from da2.outbox import InMemoryOutbox, OutboxRelay

    outbox = InMemoryOutbox()
    relay = OutboxRelay(outbox, publisher=lambda entries: print(entries))

    # Store events (normally done by an OutboxEventStore wrapper)
    outbox.store(stored_events)

    # Poll and publish
    count = relay.poll_and_publish()
"""

from __future__ import annotations

import abc
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable

from .event_store import StoredEvent


@dataclass
class OutboxEntry:
    """A single entry in the outbox, representing an event to be published."""

    id: str
    aggregate_id: Any
    event_type: str
    data: dict
    version: int
    position: int
    created_at: float
    published: bool = False

    @classmethod
    def from_stored_event(cls, se: StoredEvent) -> OutboxEntry:
        """Create an OutboxEntry from a StoredEvent."""
        return cls(
            id=str(uuid.uuid4()),
            aggregate_id=se.aggregate_id,
            event_type=se.event_type,
            data=dict(se.data),
            version=se.version,
            position=se.position,
            created_at=time.time(),
        )


class Outbox(abc.ABC):
    """Abstract outbox store.

    Implementations must provide atomic store, fetch, and mark operations.
    """

    @abc.abstractmethod
    def store(self, events: list[StoredEvent]) -> list[OutboxEntry]:
        """Store events in the outbox. Returns the created entries."""
        raise NotImplementedError

    @abc.abstractmethod
    def fetch_unpublished(self, limit: int = 100) -> list[OutboxEntry]:
        """Fetch unpublished entries in insertion order."""
        raise NotImplementedError

    @abc.abstractmethod
    def mark_published(self, entry_ids: list[str]) -> None:
        """Mark entries as published by their IDs."""
        raise NotImplementedError


class InMemoryOutbox(Outbox):
    """In-memory outbox for testing."""

    def __init__(self) -> None:
        self._entries: list[OutboxEntry] = []

    def store(self, events: list[StoredEvent]) -> list[OutboxEntry]:
        entries = [OutboxEntry.from_stored_event(se) for se in events]
        self._entries.extend(entries)
        return entries

    def fetch_unpublished(self, limit: int = 100) -> list[OutboxEntry]:
        return [e for e in self._entries if not e.published][:limit]

    def mark_published(self, entry_ids: list[str]) -> None:
        id_set = set(entry_ids)
        for entry in self._entries:
            if entry.id in id_set:
                entry.published = True

    def __len__(self) -> int:
        return len(self._entries)

    @property
    def unpublished_count(self) -> int:
        return sum(1 for e in self._entries if not e.published)


class OutboxRelay:
    """Polls the outbox and publishes entries via a callback.

    The publisher callable receives a list of OutboxEntry objects and should
    publish them to the external system (e.g., message broker, HTTP endpoint).

    Args:
        outbox: The outbox store to poll.
        publisher: Callable that receives entries and publishes them.
            Signature: ``(entries: list[OutboxEntry]) -> None``.
            Must raise on failure to prevent marking as published.
        batch_size: Maximum entries to process per poll. Default 100.
    """

    def __init__(
        self,
        outbox: Outbox,
        publisher: Callable[[list[OutboxEntry]], None],
        batch_size: int = 100,
    ) -> None:
        self._outbox = outbox
        self._publisher = publisher
        self._batch_size = batch_size

    def poll_and_publish(self) -> int:
        """Fetch unpublished entries, publish them, and mark as published.

        Returns the number of entries published. If the publisher raises,
        entries are NOT marked as published (at-least-once delivery).
        """
        entries = self._outbox.fetch_unpublished(limit=self._batch_size)
        if not entries:
            return 0
        self._publisher(entries)
        self._outbox.mark_published([e.id for e in entries])
        return len(entries)
