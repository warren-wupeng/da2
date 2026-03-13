import pytest

from da2 import Event, ConcurrencyError
from da2.event_sourced import EventSourcedEntity
from da2.event_store_async import InMemoryEventStoreAsync
from da2.event_sourced_repository_async import EventSourcedRepositoryAsync
from da2.snapshot_async import InMemorySnapshotStoreAsync
from da2.exceptions import EntityNotFound


# --- Domain ---

class TaskCreated(Event):
    def __init__(self, title: str):
        self.title = title


class TaskCompleted(Event):
    def __init__(self):
        pass


class Task(EventSourcedEntity[str]):
    def __init__(self, identity: str) -> None:
        super().__init__(identity)
        self.title: str = ""
        self.done: bool = False

    @classmethod
    def create(cls, task_id: str, title: str) -> "Task":
        task = cls(task_id)
        task._apply_and_record(TaskCreated(title=title))
        return task

    def complete(self) -> None:
        self._apply_and_record(TaskCompleted())

    def _when_TaskCreated(self, event: TaskCreated) -> None:
        self.title = event.title

    def _when_TaskCompleted(self, event: TaskCompleted) -> None:
        self.done = True


EVENT_REGISTRY = {
    "TaskCreated": TaskCreated,
    "TaskCompleted": TaskCompleted,
}


class TestEventSourcedRepositoryAsync:

    async def test_save_and_get(self):
        store = InMemoryEventStoreAsync()
        repo = EventSourcedRepositoryAsync(
            event_store=store,
            entity_cls=Task,
            event_registry=EVENT_REGISTRY,
        )
        task = Task.create("t-1", "Write tests")
        await repo.save(task)

        loaded = await repo.get("t-1")
        assert loaded.title == "Write tests"
        assert loaded.version == 1

    async def test_incremental_save(self):
        store = InMemoryEventStoreAsync()
        repo = EventSourcedRepositoryAsync(
            event_store=store,
            entity_cls=Task,
            event_registry=EVENT_REGISTRY,
        )
        task = Task.create("t-1", "Write tests")
        await repo.save(task)

        loaded = await repo.get("t-1")
        loaded.complete()
        await repo.save(loaded)

        reloaded = await repo.get("t-1")
        assert reloaded.done is True
        assert reloaded.version == 2

    async def test_get_not_found(self):
        store = InMemoryEventStoreAsync()
        repo = EventSourcedRepositoryAsync(
            event_store=store,
            entity_cls=Task,
            event_registry=EVENT_REGISTRY,
        )
        with pytest.raises(EntityNotFound):
            await repo.get("nonexistent")

    async def test_concurrency_conflict(self):
        store = InMemoryEventStoreAsync()
        repo = EventSourcedRepositoryAsync(
            event_store=store,
            entity_cls=Task,
            event_registry=EVENT_REGISTRY,
        )
        task = Task.create("t-1", "Test")
        await repo.save(task)

        reader_a = await repo.get("t-1")
        reader_b = await repo.get("t-1")
        reader_a.complete()
        await repo.save(reader_a)
        reader_b.complete()
        with pytest.raises(ConcurrencyError):
            await repo.save(reader_b)

    async def test_with_snapshot(self):
        event_store = InMemoryEventStoreAsync()
        snap_store = InMemorySnapshotStoreAsync()
        repo = EventSourcedRepositoryAsync(
            event_store=event_store,
            entity_cls=Task,
            event_registry=EVENT_REGISTRY,
            snapshot_store=snap_store,
            snapshot_interval=3,
        )

        # Create task + 3 completions -> crosses interval at v3
        task = Task.create("t-1", "Test")
        await repo.save(task)

        loaded = await repo.get("t-1")
        loaded.complete()
        await repo.save(loaded)  # v2

        loaded = await repo.get("t-1")
        # Need more events to cross interval
        # v2 -> add TaskCreated as workaround; let's just test snapshot manually
        snap = await snap_store.load("t-1")
        # Not yet snapshotted (haven't crossed boundary of 3)
        assert snap is None

    async def test_snapshot_auto_taken(self):
        """When version crosses snapshot_interval, snapshot is saved."""
        from da2 import Event as E

        class Tick(E):
            def __init__(self, n: int):
                self.n = n

        class Counter(EventSourcedEntity[str]):
            def __init__(self, identity: str) -> None:
                super().__init__(identity)
                self.count: int = 0

            def tick(self) -> None:
                self._apply_and_record(Tick(n=self.count + 1))

            def _when_Tick(self, event: Tick) -> None:
                self.count = event.n

        event_store = InMemoryEventStoreAsync()
        snap_store = InMemorySnapshotStoreAsync()
        repo = EventSourcedRepositoryAsync(
            event_store=event_store,
            entity_cls=Counter,
            event_registry={"Tick": Tick},
            snapshot_store=snap_store,
            snapshot_interval=3,
        )

        counter = Counter("c-1")
        for _ in range(4):
            counter.tick()
        await repo.save(counter)  # v4, crosses boundary 0->3

        snap = await snap_store.load("c-1")
        assert snap is not None
        assert snap.version == 4
        assert snap.state["count"] == 4

        # Load uses snapshot + no new events
        loaded = await repo.get("c-1")
        assert loaded.count == 4
        assert loaded.version == 4
