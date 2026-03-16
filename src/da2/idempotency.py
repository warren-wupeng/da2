"""Idempotent command handling via middleware.

Prevents duplicate command execution by caching results keyed on
an ``idempotency_key`` attribute on the command. Commands without
this attribute pass through unchanged.

Example::

    from da2 import Command, bootstrap
    from da2.idempotency import IdempotencyMiddleware, InMemoryIdempotencyStore

    class PlaceOrder(Command):
        def __init__(self, order_id: str, idempotency_key: str):
            self.order_id = order_id
            self.idempotency_key = idempotency_key

    store = InMemoryIdempotencyStore()
    bus = bootstrap(
        uow=my_uow,
        commands={PlaceOrder: handle_place_order},
        middleware=[IdempotencyMiddleware(store)],
    )

    # First call executes the handler
    bus.handle(PlaceOrder("o1", idempotency_key="abc-123"))

    # Second call with same key returns cached result
    bus.handle(PlaceOrder("o1", idempotency_key="abc-123"))
"""

from __future__ import annotations

import abc
import time
from typing import Any, Callable

from .message_bus import Message


class IdempotencyStore(abc.ABC):
    """Abstract store for idempotency results."""

    @abc.abstractmethod
    def get(self, key: str) -> tuple[bool, Any]:
        """Return ``(True, cached_result)`` if found, ``(False, None)`` otherwise."""

    @abc.abstractmethod
    def save(self, key: str, result: Any, ttl_seconds: int = 3600) -> None:
        """Persist result for the given key with a TTL."""


class InMemoryIdempotencyStore(IdempotencyStore):
    """In-memory idempotency store with TTL expiration (for testing)."""

    def __init__(self) -> None:
        self._store: dict[str, tuple[Any, float]] = {}

    def get(self, key: str) -> tuple[bool, Any]:
        entry = self._store.get(key)
        if entry is None:
            return False, None
        result, expires_at = entry
        if time.monotonic() > expires_at:
            del self._store[key]
            return False, None
        return True, result

    def save(self, key: str, result: Any, ttl_seconds: int = 3600) -> None:
        self._store[key] = (result, time.monotonic() + ttl_seconds)

    def clear(self) -> None:
        """Remove all entries."""
        self._store.clear()


class IdempotencyMiddleware:
    """Sync middleware that deduplicates commands by ``idempotency_key``.

    Only messages that have an ``idempotency_key`` attribute are checked.
    All other messages pass through unchanged.

    Args:
        store: An :class:`IdempotencyStore` implementation.
        ttl_seconds: How long to cache results (default 3600 = 1 hour).
    """

    def __init__(self, store: IdempotencyStore, ttl_seconds: int = 3600) -> None:
        self._store = store
        self._ttl = ttl_seconds

    def __call__(self, message: Message, next: Callable[[Message], Any]) -> Any:
        key = getattr(message, "idempotency_key", None)
        if key is None:
            return next(message)

        found, cached = self._store.get(key)
        if found:
            return cached

        result = next(message)
        self._store.save(key, result, self._ttl)
        return result
