"""Async Redis-backed event store for production use.

Uses ``redis.asyncio`` with Lua scripts for atomic optimistic concurrency.

Requires the ``redis`` extra::

    pip install da2[redis]

Example::

    import redis.asyncio as aioredis
    from da2.redis_event_store_async import RedisEventStoreAsync

    r = aioredis.Redis(host="localhost", port=6379, decode_responses=True)
    store = RedisEventStoreAsync(r, prefix="myapp:events")

    await store.append("order-1", [OrderPlaced(total=99.0)], expected_version=0)
    events = await store.load("order-1")
"""

from __future__ import annotations

import json
import time
from typing import Any

from .event import Event
from .event_store import StoredEvent
from .event_store_async import EventStoreAsync
from .exceptions import ConcurrencyError

_APPEND_LUA = """
local version_key = KEYS[1]
local stream_key = KEYS[2]
local global_pos_key = KEYS[3]
local global_stream_key = KEYS[4]
local expected = tonumber(ARGV[1])
local current = tonumber(redis.call('GET', version_key) or 0)
if current ~= expected then
    return error('CONCURRENCY:' .. current)
end
for i = 2, #ARGV do
    local pos = redis.call('INCR', global_pos_key)
    local obj = cjson.decode(ARGV[i])
    obj['position'] = pos
    local enriched = cjson.encode(obj)
    redis.call('RPUSH', stream_key, enriched)
    redis.call('RPUSH', global_stream_key, enriched)
end
redis.call('SET', version_key, expected + #ARGV - 1)
return expected + #ARGV - 1
"""


class RedisEventStoreAsync(EventStoreAsync):
    """Async Redis-backed event store with optimistic concurrency via Lua.

    Same key layout as :class:`RedisEventStore`:
      - ``{prefix}:{aggregate_id}:version``
      - ``{prefix}:{aggregate_id}:events``

    Global stream keys:
      - ``{prefix}:_position`` -- atomic global position counter
      - ``{prefix}:_all``      -- list of all events in global order

    Args:
        client: A ``redis.asyncio.Redis`` instance (must use ``decode_responses=True``).
        prefix: Key namespace prefix. Default ``"da2:events"``.
    """

    def __init__(self, client: Any, prefix: str = "da2:events") -> None:
        self._client = client
        self._prefix = prefix
        self._append_script = client.register_script(_APPEND_LUA)

    def _version_key(self, aggregate_id: Any) -> str:
        return f"{self._prefix}:{aggregate_id}:version"

    def _stream_key(self, aggregate_id: Any) -> str:
        return f"{self._prefix}:{aggregate_id}:events"

    def _global_position_key(self) -> str:
        return f"{self._prefix}:_position"

    def _global_stream_key(self) -> str:
        return f"{self._prefix}:_all"

    async def append(
        self,
        aggregate_id: Any,
        events: list[Event],
        expected_version: int,
    ) -> None:
        if not events:
            return
        version_key = self._version_key(aggregate_id)
        stream_key = self._stream_key(aggregate_id)

        payloads: list[str] = []
        for i, event in enumerate(events):
            version = expected_version + i + 1
            stored = {
                "aggregate_id": str(aggregate_id),
                "event_type": type(event).__name__,
                "data": event.to_dict(),
                "version": version,
                "timestamp": time.time(),
            }
            payloads.append(json.dumps(stored))

        try:
            await self._append_script(
                keys=[
                    version_key,
                    stream_key,
                    self._global_position_key(),
                    self._global_stream_key(),
                ],
                args=[expected_version] + payloads,
            )
        except Exception as exc:
            msg = str(exc)
            if "CONCURRENCY:" in msg:
                current = msg.split("CONCURRENCY:")[1].strip()
                raise ConcurrencyError(
                    f"Expected version {expected_version} for aggregate "
                    f"{aggregate_id!r}, but current version is {current}. "
                    f"Another process may have written events concurrently."
                ) from exc
            raise

    def _deserialize_stored(self, raw: str) -> StoredEvent:
        obj = json.loads(raw)
        return StoredEvent(
            aggregate_id=obj["aggregate_id"],
            event_type=obj["event_type"],
            data=obj["data"],
            version=obj["version"],
            timestamp=obj["timestamp"],
            position=obj.get("position", 0),
        )

    async def load(self, aggregate_id: Any) -> list[StoredEvent]:
        stream_key = self._stream_key(aggregate_id)
        raw_events = await self._client.lrange(stream_key, 0, -1)
        return [self._deserialize_stored(raw) for raw in raw_events]

    async def load_since(self, aggregate_id: Any, after_version: int) -> list[StoredEvent]:
        all_events = await self.load(aggregate_id)
        return [se for se in all_events if se.version > after_version]

    async def load_all(self, after_position: int = 0) -> list[StoredEvent]:
        """Load all events across all aggregates, ordered by global position.

        Uses efficient Redis LRANGE since positions map directly to list indices.
        """
        global_key = self._global_stream_key()
        # Positions are 1-indexed; list is 0-indexed. after_position=N means
        # skip first N entries (indices 0..N-1), start at index N.
        raw_events = await self._client.lrange(global_key, after_position, -1)
        return [self._deserialize_stored(raw) for raw in raw_events]
