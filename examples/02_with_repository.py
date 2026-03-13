"""Repository pattern example -- using InMemoryRepository with UnitOfWork.

Run::

    cd /path/to/da2
    uv run python examples/02_with_repository.py
"""

from da2 import (
    Command,
    Event,
    Entity,
    UnitOfWork,
    InMemoryRepository,
    bootstrap,
)


# --- Domain ---

class TaskCreated(Event):
    def __init__(self, task_id: str, title: str) -> None:
        self.task_id = task_id
        self.title = title


class TaskCompleted(Event):
    def __init__(self, task_id: str) -> None:
        self.task_id = task_id


class CreateTask(Command):
    def __init__(self, task_id: str, title: str) -> None:
        self.task_id = task_id
        self.title = title


class CompleteTask(Command):
    def __init__(self, task_id: str) -> None:
        self.task_id = task_id


class Task(Entity[str, dict]):
    @classmethod
    def create(cls, task_id: str, title: str) -> "Task":
        task = cls(identity=task_id, desc={"title": title, "done": False})
        task.raise_event(TaskCreated(task_id=task_id, title=title))
        return task

    def complete(self) -> None:
        self.desc["done"] = True
        self.raise_event(TaskCompleted(task_id=self.identity))


# --- UoW with Repository ---

class TaskUoW(UnitOfWork):
    def __init__(self) -> None:
        super().__init__()
        self.seen: dict = {}  # init seen so bus can collect events
        self.tasks = InMemoryRepository[Task](add_seen=self.add_seen)

    def _enter(self) -> None:
        pass

    def _commit(self) -> None:
        pass

    def rollback(self) -> None:
        pass


# --- Handlers ---

def handle_create_task(cmd: CreateTask, uow: TaskUoW) -> None:
    task = Task.create(cmd.task_id, cmd.title)
    uow.tasks.add(task)
    print(f"[cmd] Created task {cmd.task_id}: {cmd.title}")


def handle_complete_task(cmd: CompleteTask, uow: TaskUoW) -> None:
    task = uow.tasks.get(cmd.task_id)
    task.complete()
    uow.tasks.update(task)
    print(f"[cmd] Completed task {cmd.task_id}")


def on_task_created(event: TaskCreated) -> None:
    print(f"[event] Task '{event.title}' was created")


def on_task_completed(event: TaskCompleted) -> None:
    print(f"[event] Task {event.task_id} marked done -- notify team")


# --- Wire & Run ---

def main() -> None:
    uow = TaskUoW()
    bus = bootstrap(
        uow=uow,
        commands={
            CreateTask: handle_create_task,
            CompleteTask: handle_complete_task,
        },
        events={
            TaskCreated: [on_task_created],
            TaskCompleted: [on_task_completed],
        },
    )

    bus.handle(CreateTask(task_id="T-1", title="Write README"))
    bus.handle(CreateTask(task_id="T-2", title="Add tests"))
    bus.handle(CompleteTask(task_id="T-1"))

    print()
    print(f"Total tasks: {len(uow.tasks)}")
    all_tasks = uow.tasks.find()
    for t in all_tasks:
        status = "done" if t.desc["done"] else "open"
        print(f"  {t.identity}: {t.desc['title']} [{status}]")


if __name__ == "__main__":
    main()
