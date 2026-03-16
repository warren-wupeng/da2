"""Redis-backed event store for production use.

Uses Redis hash + list per aggregate stream with Lua scripts for atomic
optimistic concurrency control.

Requires the ``redis`` extra::

    pip install da2[redis]

Example::

    import redis
    from da2.redis_event_store import RedisEventStore

    r = redis.Redis(host="localhost", port=6379, decode_responses=True)
    store = RedisEventStore(r, prefix="myapp:events")

    store.append("order-1", [OrderPlaced(total=99.0)], expected_version=0)
    events = store.load("order-1")
"""

from __future__ import annotations

import json
import time
from typing import Any

from .event import Event
from .event_store import EventStore, StoredEvent
from .exceptions import ConcurrencyError

# Lua script for atomic append with optimistic concurrency.
# KEYS[1] = version key (hash), KEYS[2] = stream key (list)
# ARGV = [expected_version, json1, json2, ...]
_APPEND_LUA = """
local version_key = KEYS[1]
local stream_key = KEYS[2]
local expected = tonumber(ARGV[1])
local current = tonumber(redis.call('GET', version_key) or 0)
if current ~= expected then
    return error('CONCURRENCY:' .. current)
end
for i = 2, #ARGV do
    redis.call('RPUSH', stream_key, ARGV[i])
end
redis.call('SET', version_key, expected + #ARGV - 1)
return expected + #ARGV - 1
"""


class RedisEventStore(EventStore):
    """Redis-backed event store with optimistic concurrency via Lua.

    Each aggregate gets two Redis keys:
      - ``{prefix}:{aggregate_id}:version`` -- current version counter
      - ``{prefix}:{aggregate_id}:events``  -- list of JSON-encoded StoredEvents

    Args:
        client: A ``redis.Redis`` instance (must use ``decode_responses=True``).
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

    def append(
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
            self._append_script(
                keys=[version_key, stream_key],
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

    def load(self, aggregate_id: Any) -> list[StoredEvent]:
        stream_key = self._stream_key(aggregate_id)
        raw_events = self._client.lrange(stream_key, 0, -1)
        result: list[StoredEvent] = []
        for raw in raw_events:
            obj = json.loads(raw)
            result.append(StoredEvent(
                aggregate_id=obj["aggregate_id"],
                event_type=obj["event_type"],
                data=obj["data"],
                version=obj["version"],
                timestamp=obj["timestamp"],
            ))
        return result

    def load_since(self, aggregate_id: Any, after_version: int) -> list[StoredEvent]:
        all_events = self.load(aggregate_id)
        return [se for se in all_events if se.version > after_version]
