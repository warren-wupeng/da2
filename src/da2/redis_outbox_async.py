"""Async Redis-backed outbox for production use.

Uses ``redis.asyncio`` for non-blocking outbox operations.

Requires the ``redis`` extra::

    pip install da2[redis]

Example::

    import redis.asyncio as aioredis
    from da2.redis_outbox_async import RedisOutboxAsync

    r = aioredis.Redis(host="localhost", port=6379, decode_responses=True)
    outbox = RedisOutboxAsync(r, prefix="myapp:outbox")

    await outbox.store(stored_events)
    entries = await outbox.fetch_unpublished(limit=50)
    await outbox.mark_published([e.id for e in entries])
"""

from __future__ import annotations

import json
from typing import Any

from .event_store import StoredEvent
from .outbox import OutboxEntry
from .outbox_async import OutboxAsync


class RedisOutboxAsync(OutboxAsync):
    """Async Redis-backed outbox using a list as a FIFO queue.

    Same key layout as :class:`RedisOutbox`:
      - ``{prefix}:pending`` -- list of JSON-encoded outbox entries

    Args:
        client: A ``redis.asyncio.Redis`` instance (must use ``decode_responses=True``).
        prefix: Key namespace prefix. Default ``"da2:outbox"``.
    """

    def __init__(self, client: Any, prefix: str = "da2:outbox") -> None:
        self._client = client
        self._prefix = prefix

    def _pending_key(self) -> str:
        return f"{self._prefix}:pending"

    def _serialize_entry(self, entry: OutboxEntry) -> str:
        return json.dumps({
            "id": entry.id,
            "aggregate_id": str(entry.aggregate_id),
            "event_type": entry.event_type,
            "data": entry.data,
            "version": entry.version,
            "position": entry.position,
            "created_at": entry.created_at,
        })

    def _deserialize_entry(self, raw: str) -> OutboxEntry:
        obj = json.loads(raw)
        return OutboxEntry(
            id=obj["id"],
            aggregate_id=obj["aggregate_id"],
            event_type=obj["event_type"],
            data=obj["data"],
            version=obj["version"],
            position=obj["position"],
            created_at=obj["created_at"],
            published=False,
        )

    async def store(self, events: list[StoredEvent]) -> list[OutboxEntry]:
        entries = [OutboxEntry.from_stored_event(se) for se in events]
        if entries:
            payloads = [self._serialize_entry(e) for e in entries]
            await self._client.rpush(self._pending_key(), *payloads)
        return entries

    async def fetch_unpublished(self, limit: int = 100) -> list[OutboxEntry]:
        raw_list = await self._client.lrange(self._pending_key(), 0, limit - 1)
        return [self._deserialize_entry(raw) for raw in raw_list]

    async def mark_published(self, entry_ids: list[str]) -> None:
        if not entry_ids:
            return
        pipe = self._client.pipeline()
        for _ in entry_ids:
            pipe.lpop(self._pending_key())
        await pipe.execute()
