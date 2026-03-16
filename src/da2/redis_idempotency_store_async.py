"""Async Redis-backed idempotency store for production use.

Async counterpart of :mod:`da2.redis_idempotency_store`.

Requires the ``redis`` extra::

    pip install da2[redis]

Example::

    import redis.asyncio as redis
    from da2.idempotency_async import IdempotencyMiddlewareAsync
    from da2.redis_idempotency_store_async import RedisIdempotencyStoreAsync

    r = redis.Redis(host="localhost", port=6379, decode_responses=True)
    store = RedisIdempotencyStoreAsync(r)
    bus = BootstrapAsync(
        uow=my_uow,
        command_handlers={PlaceOrder: handle},
        event_handlers={},
        middleware=[IdempotencyMiddlewareAsync(store)],
    ).create_message_bus()
"""

from __future__ import annotations

import json
from typing import Any

from .idempotency_async import IdempotencyStoreAsync


class RedisIdempotencyStoreAsync(IdempotencyStoreAsync):
    """Async Redis-backed idempotency store with native TTL.

    Each result is stored as a JSON string at ``{prefix}:{key}``
    with a Redis ``EX`` (seconds) expiration.

    Args:
        client: A ``redis.asyncio.Redis`` instance (must use ``decode_responses=True``).
        prefix: Key namespace prefix. Default ``"da2:idempotency"``.
    """

    def __init__(self, client: Any, prefix: str = "da2:idempotency") -> None:
        self._client = client
        self._prefix = prefix

    def _redis_key(self, key: str) -> str:
        return f"{self._prefix}:{key}"

    async def get(self, key: str) -> tuple[bool, Any]:
        raw = await self._client.get(self._redis_key(key))
        if raw is None:
            return False, None
        return True, json.loads(raw)

    async def save(self, key: str, result: Any, ttl_seconds: int = 3600) -> None:
        await self._client.set(
            self._redis_key(key),
            json.dumps(result),
            ex=ttl_seconds,
        )
