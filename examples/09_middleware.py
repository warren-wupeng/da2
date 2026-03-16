"""Example 09 -- MessageBus Middleware Pipeline.

Demonstrates how to add cross-cutting concerns (logging, timing, retry)
to a MessageBus via composable middleware functions.
"""

import time
from da2 import Command, Event, UnitOfWork, InMemoryRepository, Entity, bootstrap


# --- Domain ---

class UserCreated(Event):
    def __init__(self, name: str):
        self.name = name


class CreateUser(Command):
    def __init__(self, name: str, email: str):
        self.name = name
        self.email = email


class User(Entity[str, dict]):
    @classmethod
    def create(cls, email: str, name: str) -> "User":
        user = cls(identity=email, desc={"name": name})
        user.raise_event(UserCreated(name=name))
        return user


class AppUoW(UnitOfWork):
    def __init__(self):
        self.seen: dict = {}
        self.users = InMemoryRepository[User](add_seen=self.add_seen)

    def _enter(self):
        pass

    def _commit(self):
        pass

    def rollback(self):
        pass


# --- Middleware ---

def logging_middleware(message, next):
    """Log every message entering and leaving the bus."""
    msg_type = type(message).__name__
    print(f"  [LOG] >> Handling {msg_type}")
    result = next(message)
    print(f"  [LOG] << Done with {msg_type}")
    return result


def timing_middleware(message, next):
    """Measure how long message processing takes."""
    start = time.perf_counter()
    result = next(message)
    elapsed = (time.perf_counter() - start) * 1000
    print(f"  [TIMING] {type(message).__name__} took {elapsed:.2f}ms")
    return result


def error_boundary_middleware(message, next):
    """Catch and report errors instead of crashing."""
    try:
        return next(message)
    except Exception as e:
        print(f"  [ERROR] {type(message).__name__} failed: {e}")
        return None


# --- Handlers ---

def handle_create_user(cmd: CreateUser, uow: AppUoW):
    user = User.create(cmd.email, cmd.name)
    uow.users.add(user)
    return f"created:{cmd.email}"


def on_user_created(event: UserCreated):
    print(f"  [EVENT] Welcome email sent to {event.name}")


# --- Main ---

def main():
    print("=== Middleware Pipeline Demo ===\n")

    # Build bus with middleware stack: error_boundary -> logging -> timing -> handler
    bus = bootstrap(
        uow=AppUoW(),
        commands={CreateUser: handle_create_user},
        events={UserCreated: [on_user_created]},
        middleware=[error_boundary_middleware, logging_middleware, timing_middleware],
    )

    # Normal message flow
    print("1. Normal command:")
    result = bus.handle(CreateUser(name="Alice", email="alice@example.com"))
    print(f"   Result: {result}\n")

    # Second user
    print("2. Another command:")
    result = bus.handle(CreateUser(name="Bob", email="bob@example.com"))
    print(f"   Result: {result}\n")

    print("=== Done ===")


if __name__ == "__main__":
    main()
