"""Async idempotent command handling via middleware.

Async counterpart of :mod:`da2.idempotency`. See that module's
docstring for full usage details.
"""

from __future__ import annotations

import abc
from typing import Any, Awaitable, Callable

from .message_bus_async import Message


class IdempotencyStoreAsync(abc.ABC):
    """Async abstract store for idempotency results."""

    @abc.abstractmethod
    async def get(self, key: str) -> tuple[bool, Any]:
        """Return ``(True, cached_result)`` if found, ``(False, None)`` otherwise."""

    @abc.abstractmethod
    async def save(self, key: str, result: Any, ttl_seconds: int = 3600) -> None:
        """Persist result for the given key with a TTL."""


class InMemoryIdempotencyStoreAsync(IdempotencyStoreAsync):
    """Async in-memory idempotency store (for testing).

    Uses the sync :class:`~da2.idempotency.InMemoryIdempotencyStore`
    internally -- async wrappers for API consistency.
    """

    def __init__(self) -> None:
        from .idempotency import InMemoryIdempotencyStore
        self._inner = InMemoryIdempotencyStore()

    async def get(self, key: str) -> tuple[bool, Any]:
        return self._inner.get(key)

    async def save(self, key: str, result: Any, ttl_seconds: int = 3600) -> None:
        self._inner.save(key, result, ttl_seconds)

    def clear(self) -> None:
        """Remove all entries."""
        self._inner.clear()


class IdempotencyMiddlewareAsync:
    """Async middleware that deduplicates commands by ``idempotency_key``.

    Only messages that have an ``idempotency_key`` attribute are checked.
    All other messages pass through unchanged.

    Args:
        store: An :class:`IdempotencyStoreAsync` implementation.
        ttl_seconds: How long to cache results (default 3600 = 1 hour).
    """

    def __init__(self, store: IdempotencyStoreAsync, ttl_seconds: int = 3600) -> None:
        self._store = store
        self._ttl = ttl_seconds

    async def __call__(
        self,
        message: Message,
        next: Callable[[Message], Awaitable[Any]],
    ) -> Any:
        key = getattr(message, "idempotency_key", None)
        if key is None:
            return await next(message)

        found, cached = await self._store.get(key)
        if found:
            return cached

        result = await next(message)
        await self._store.save(key, result, self._ttl)
        return result
