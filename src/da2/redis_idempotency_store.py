"""Redis-backed idempotency store for production use.

Stores command results as JSON in Redis with native TTL expiration.

Requires the ``redis`` extra::

    pip install da2[redis]

Example::

    import redis
    from da2.idempotency import IdempotencyMiddleware
    from da2.redis_idempotency_store import RedisIdempotencyStore

    r = redis.Redis(host="localhost", port=6379, decode_responses=True)
    store = RedisIdempotencyStore(r)
    bus = bootstrap(
        uow=my_uow,
        commands={PlaceOrder: handle},
        middleware=[IdempotencyMiddleware(store)],
    )
"""

from __future__ import annotations

import json
from typing import Any

from .idempotency import IdempotencyStore


class RedisIdempotencyStore(IdempotencyStore):
    """Redis-backed idempotency store with native TTL.

    Each result is stored as a JSON string at ``{prefix}:{key}``
    with a Redis ``EX`` (seconds) expiration.

    Args:
        client: A ``redis.Redis`` instance (must use ``decode_responses=True``).
        prefix: Key namespace prefix. Default ``"da2:idempotency"``.
    """

    def __init__(self, client: Any, prefix: str = "da2:idempotency") -> None:
        self._client = client
        self._prefix = prefix

    def _redis_key(self, key: str) -> str:
        return f"{self._prefix}:{key}"

    def get(self, key: str) -> tuple[bool, Any]:
        raw = self._client.get(self._redis_key(key))
        if raw is None:
            return False, None
        return True, json.loads(raw)

    def save(self, key: str, result: Any, ttl_seconds: int = 3600) -> None:
        self._client.set(
            self._redis_key(key),
            json.dumps(result),
            ex=ttl_seconds,
        )
