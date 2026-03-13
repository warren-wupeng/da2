"""Basic CQRS example -- commands, events, and a message bus.

Run::

    cd /path/to/da2
    uv run python examples/01_basic_cqrs.py
"""

from da2 import (
    Command,
    Event,
    Entity,
    UnitOfWork,
    bootstrap,
)


# --- Domain Events ---

class UserCreated(Event):
    def __init__(self, name: str) -> None:
        self.name = name


class WelcomeEmailSent(Event):
    def __init__(self, name: str) -> None:
        self.name = name


# --- Commands ---

class CreateUser(Command):
    def __init__(self, name: str) -> None:
        self.name = name


# --- Entity ---

class User(Entity[str, dict]):
    @classmethod
    def create(cls, name: str) -> "User":
        user = cls(identity=name, desc={"name": name})
        user.raise_event(UserCreated(name=name))
        return user


# --- Minimal UoW (no real DB) ---

class InMemoryUoW(UnitOfWork):
    def __init__(self) -> None:
        super().__init__()
        self.seen: dict = {}  # init seen so bus can collect events
        self.committed = False
        self.users: dict[str, User] = {}

    def _enter(self) -> None:
        self.committed = False

    def _commit(self) -> None:
        self.committed = True

    def rollback(self) -> None:
        self.committed = False


# --- Handlers ---

def handle_create_user(cmd: CreateUser, uow: InMemoryUoW) -> None:
    user = User.create(cmd.name)
    uow.users[user.identity] = user
    uow.add_seen(user)  # so events are collected
    print(f"[cmd] Created user: {cmd.name}")


def on_user_created(event: UserCreated) -> None:
    print(f"[event] UserCreated -> sending welcome email to {event.name}")


def on_user_created_log(event: UserCreated) -> None:
    print(f"[event] UserCreated -> logging analytics for {event.name}")


# --- Wire & Run ---

def main() -> None:
    uow = InMemoryUoW()
    bus = bootstrap(
        uow=uow,
        commands={CreateUser: handle_create_user},
        events={UserCreated: [on_user_created, on_user_created_log]},
    )

    print("=== Creating Alice ===")
    bus.handle(CreateUser(name="Alice"))
    print(f"Committed: {uow.committed}")

    print()
    print("=== Creating Bob ===")
    bus.handle(CreateUser(name="Bob"))
    print(f"Users in memory: {list(uow.users.keys())}")


if __name__ == "__main__":
    main()
