"""Snapshot support for event-sourced entities.

Snapshots capture entity state at a point in time so that future loads
only need to replay events *after* the snapshot, instead of the full stream.

Provides ``SnapshotStore`` (abstract) and ``InMemorySnapshotStore`` (for testing).
"""

from __future__ import annotations

import abc
import time
from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class Snapshot:
    """Immutable snapshot of an event-sourced entity's state.

    Example::

        from da2.snapshot import Snapshot

        snap = Snapshot(
            aggregate_id="acc-1",
            version=50,
            state={"owner": "Alice", "balance": 1500.0},
        )
        assert snap.version == 50
    """

    aggregate_id: Any
    version: int
    state: dict
    timestamp: float = field(default_factory=time.time)


class SnapshotStore(abc.ABC):
    """Abstract snapshot store.

    Subclass and implement ``save()`` and ``load()`` for your storage backend.
    """

    @abc.abstractmethod
    def save(self, snapshot: Snapshot) -> None:
        """Persist a snapshot (replaces any previous snapshot for the same aggregate)."""
        raise NotImplementedError

    @abc.abstractmethod
    def load(self, aggregate_id: Any) -> Snapshot | None:
        """Load the latest snapshot for *aggregate_id*, or None if none exists."""
        raise NotImplementedError


class InMemorySnapshotStore(SnapshotStore):
    """Dict-backed snapshot store for testing and prototyping.

    Example::

        from da2.snapshot import InMemorySnapshotStore, Snapshot

        store = InMemorySnapshotStore()
        store.save(Snapshot(aggregate_id="acc-1", version=10, state={"balance": 500}))

        snap = store.load("acc-1")
        assert snap is not None
        assert snap.version == 10
    """

    def __init__(self) -> None:
        self._snapshots: dict[Any, Snapshot] = {}

    def save(self, snapshot: Snapshot) -> None:
        self._snapshots[snapshot.aggregate_id] = snapshot

    def load(self, aggregate_id: Any) -> Snapshot | None:
        return self._snapshots.get(aggregate_id)
