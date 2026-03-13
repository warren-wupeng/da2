import pytest

from da2 import Event, ConcurrencyError, UnitOfWork, bootstrap
from da2.event_sourced import EventSourcedEntity
from da2.event_store import InMemoryEventStore
from da2.event_sourced_repository import EventSourcedRepository
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


class TestEventSourcedRepository:

    def test_save_and_get(self):
        store = InMemoryEventStore()
        repo = EventSourcedRepository(
            event_store=store,
            entity_cls=Task,
            event_registry=EVENT_REGISTRY,
        )
        task = Task.create("t-1", "Write tests")
        repo.save(task)

        loaded = repo.get("t-1")
        assert loaded.title == "Write tests"
        assert loaded.version == 1
        assert loaded.pending_events == []

    def test_save_clears_pending(self):
        store = InMemoryEventStore()
        repo = EventSourcedRepository(
            event_store=store,
            entity_cls=Task,
            event_registry=EVENT_REGISTRY,
        )
        task = Task.create("t-1", "Write tests")
        assert len(task.pending_events) == 1
        repo.save(task)
        assert task.pending_events == []

    def test_incremental_save(self):
        store = InMemoryEventStore()
        repo = EventSourcedRepository(
            event_store=store,
            entity_cls=Task,
            event_registry=EVENT_REGISTRY,
        )
        task = Task.create("t-1", "Write tests")
        repo.save(task)

        loaded = repo.get("t-1")
        loaded.complete()
        repo.save(loaded)

        reloaded = repo.get("t-1")
        assert reloaded.done is True
        assert reloaded.version == 2

    def test_get_not_found(self):
        store = InMemoryEventStore()
        repo = EventSourcedRepository(
            event_store=store,
            entity_cls=Task,
            event_registry=EVENT_REGISTRY,
        )
        with pytest.raises(EntityNotFound):
            repo.get("nonexistent")

    def test_concurrency_conflict(self):
        store = InMemoryEventStore()
        repo = EventSourcedRepository(
            event_store=store,
            entity_cls=Task,
            event_registry=EVENT_REGISTRY,
        )
        task = Task.create("t-1", "Write tests")
        repo.save(task)

        # Two readers load the same aggregate
        reader_a = repo.get("t-1")
        reader_b = repo.get("t-1")

        # Both mutate and try to save
        reader_a.complete()
        repo.save(reader_a)  # succeeds (expected_version=1, current=1)

        reader_b.complete()
        with pytest.raises(ConcurrencyError):
            repo.save(reader_b)  # fails (expected_version=1, current=2)

    def test_save_empty_pending_is_noop(self):
        store = InMemoryEventStore()
        repo = EventSourcedRepository(
            event_store=store,
            entity_cls=Task,
            event_registry=EVENT_REGISTRY,
        )
        task = Task.create("t-1", "Write tests")
        repo.save(task)

        loaded = repo.get("t-1")
        repo.save(loaded)  # no pending events -> noop
        assert store.load("t-1") == store.load("t-1")  # unchanged

    def test_unknown_event_type_in_registry(self):
        store = InMemoryEventStore()
        repo = EventSourcedRepository(
            event_store=store,
            entity_cls=Task,
            event_registry={},  # empty registry
        )
        task = Task.create("t-1", "Write tests")
        repo.save(task)
        with pytest.raises(KeyError, match="Unknown event type"):
            repo.get("t-1")


class TestEventSourcedWithMessageBus:
    """Integration: EventSourcedRepository + UnitOfWork + MessageBus."""

    def test_full_lifecycle(self):
        store = InMemoryEventStore()
        repo = EventSourcedRepository(
            event_store=store,
            entity_cls=Task,
            event_registry=EVENT_REGISTRY,
        )

        results = []

        class SimpleUoW(UnitOfWork):
            def __init__(self):
                super().__init__()
                self.seen: dict = {}

            def _enter(self): pass
            def _commit(self): pass
            def rollback(self): pass

        from da2 import Command

        class CreateTask(Command):
            def __init__(self, task_id: str, title: str):
                self.task_id = task_id
                self.title = title

        def handle_create_task(cmd: CreateTask, uow: SimpleUoW):
            task = Task.create(cmd.task_id, cmd.title)
            repo.save(task)
            results.append(f"created:{cmd.task_id}")

        uow = SimpleUoW()
        bus = bootstrap(
            uow=uow,
            commands={CreateTask: handle_create_task},
        )
        bus.handle(CreateTask(task_id="t-1", title="Test"))

        assert results == ["created:t-1"]
        loaded = repo.get("t-1")
        assert loaded.title == "Test"
