"""Async snapshot store for event-sourced entities.

Async counterpart of ``snapshot.py``.
"""

from __future__ import annotations

import abc
from typing import Any

from .snapshot import Snapshot


class SnapshotStoreAsync(abc.ABC):
    """Abstract async snapshot store."""

    @abc.abstractmethod
    async def save(self, snapshot: Snapshot) -> None:
        """Persist a snapshot."""
        raise NotImplementedError

    @abc.abstractmethod
    async def load(self, aggregate_id: Any) -> Snapshot | None:
        """Load the latest snapshot, or None."""
        raise NotImplementedError


class InMemorySnapshotStoreAsync(SnapshotStoreAsync):
    """Async dict-backed snapshot store for testing.

    Example::

        from da2.snapshot import Snapshot
        from da2.snapshot_async import InMemorySnapshotStoreAsync

        store = InMemorySnapshotStoreAsync()
        await store.save(Snapshot(aggregate_id="a-1", version=10, state={"x": 1}))

        snap = await store.load("a-1")
        assert snap is not None and snap.version == 10
    """

    def __init__(self) -> None:
        self._snapshots: dict[Any, Snapshot] = {}

    async def save(self, snapshot: Snapshot) -> None:
        self._snapshots[snapshot.aggregate_id] = snapshot

    async def load(self, aggregate_id: Any) -> Snapshot | None:
        return self._snapshots.get(aggregate_id)
