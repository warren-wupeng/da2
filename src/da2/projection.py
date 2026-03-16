"""Projection -- event-driven read model for CQRS query side.

A :class:`Projection` subscribes to domain events and builds a
denormalized read model that is optimised for queries.  This is the
"Q" half of CQRS.

Convention: define ``_on_<EventClassName>`` methods.

Example::

    class OrderSummary(Projection):
        def __init__(self):
            super().__init__()
            self.totals: dict[str, float] = {}

        def _on_OrderPlaced(self, event: OrderPlaced):
            self.totals[event.order_id] = event.amount

        def _on_OrderCancelled(self, event: OrderCancelled):
            self.totals.pop(event.order_id, None)
"""

from __future__ import annotations

import abc
from typing import Sequence

from .event import Event
from .event_store import StoredEvent


class Projection(abc.ABC):
    """Base class for event-driven read models.

    Subclasses define ``_on_<EventClassName>(self, event)`` methods.
    Call :meth:`apply` to feed events, or :meth:`replay` to rebuild
    from a sequence of :class:`StoredEvent` envelopes.
    """

    def __init__(self) -> None:
        self._last_version: int = 0

    @property
    def last_version(self) -> int:
        """Version of the last event processed."""
        return self._last_version

    # ── public API ──────────────────────────────────────────────

    def apply(self, event: Event) -> None:
        """Route *event* to the matching ``_on_*`` handler."""
        handler = getattr(self, f"_on_{type(event).__name__}", None)
        if handler is not None:
            handler(event)

    def replay(
        self,
        stored_events: Sequence[StoredEvent],
        event_registry: dict[str, type[Event]] | None = None,
    ) -> None:
        """Rebuild this projection from persisted :class:`StoredEvent` s.

        Parameters
        ----------
        stored_events:
            Sequence of :class:`StoredEvent` envelopes, ordered by version.
        event_registry:
            Mapping from ``event_type`` string to :class:`Event` class.
            Required to deserialize stored events.  If *None*, the
            projection will only track ``last_version`` without
            calling handlers.
        """
        for se in stored_events:
            if event_registry is not None and se.event_type in event_registry:
                cls = event_registry[se.event_type]
                event = cls.from_dict(se.data)
                self.apply(event)
            self._last_version = se.version

    def catch_up(
        self,
        stored_events: Sequence[StoredEvent],
        event_registry: dict[str, type[Event]],
    ) -> None:
        """Process only events newer than :attr:`last_version`.

        This is the incremental counterpart to :meth:`replay`.
        """
        for se in stored_events:
            if se.version <= self._last_version:
                continue
            if se.event_type in event_registry:
                cls = event_registry[se.event_type]
                event = cls.from_dict(se.data)
                self.apply(event)
            self._last_version = se.version
