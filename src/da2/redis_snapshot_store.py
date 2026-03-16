"""Redis-backed snapshot store for production use.

Stores snapshots as JSON in a Redis key per aggregate.

Requires the ``redis`` extra::

    pip install da2[redis]

Example::

    import redis
    from da2.redis_snapshot_store import RedisSnapshotStore

    r = redis.Redis(host="localhost", port=6379, decode_responses=True)
    store = RedisSnapshotStore(r, prefix="myapp:snapshots")
"""

from __future__ import annotations

import json
from typing import Any

from .snapshot import Snapshot, SnapshotStore


class RedisSnapshotStore(SnapshotStore):
    """Redis-backed snapshot store.

    Each aggregate's snapshot is stored as a JSON string at
    ``{prefix}:{aggregate_id}``.

    Args:
        client: A ``redis.Redis`` instance (must use ``decode_responses=True``).
        prefix: Key namespace prefix. Default ``"da2:snapshots"``.
    """

    def __init__(self, client: Any, prefix: str = "da2:snapshots") -> None:
        self._client = client
        self._prefix = prefix

    def _key(self, aggregate_id: Any) -> str:
        return f"{self._prefix}:{aggregate_id}"

    def save(self, snapshot: Snapshot) -> None:
        data = json.dumps({
            "aggregate_id": str(snapshot.aggregate_id),
            "version": snapshot.version,
            "state": snapshot.state,
            "timestamp": snapshot.timestamp,
        })
        self._client.set(self._key(snapshot.aggregate_id), data)

    def load(self, aggregate_id: Any) -> Snapshot | None:
        raw = self._client.get(self._key(aggregate_id))
        if raw is None:
            return None
        obj = json.loads(raw)
        return Snapshot(
            aggregate_id=obj["aggregate_id"],
            version=obj["version"],
            state=obj["state"],
            timestamp=obj["timestamp"],
        )
