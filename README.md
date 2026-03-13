# da2

Python DDD and Event-Driven Architecture framework.

Lightweight building blocks for Domain-Driven Design: Entity, Repository, Unit of Work, Command/Event, MessageBus, and Bootstrap DI.

## Install

```bash
pip install da2
```

## Quick Start

### 1. Define your domain

```python
from pydantic import BaseModel
from da2 import Entity, Command, Event

# Value object
class UserDesc(BaseModel):
    name: str
    email: str

# Entity
class User(Entity[str, UserDesc]):
    def rename(self, new_name: str):
        self._desc = self.desc.model_copy(update={"name": new_name})
        self.raise_event(UserRenamed(user_id=self.identity, new_name=new_name))

# Events
class UserRenamed(Event):
    def __init__(self, user_id: str, new_name: str):
        self.user_id = user_id
        self.new_name = new_name

# Commands
class CreateUser(Command):
    def __init__(self, name: str, email: str):
        self.name = name
        self.email = email
```

### 2. Set up Repository and Unit of Work

```python
from da2 import UnitOfWork, InMemoryRepository

class UserRepo(InMemoryRepository[User]):
    pass

class AppUnitOfWork(UnitOfWork):
    def _enter(self):
        self.users = UserRepo(add_seen=self.add_seen)

    def _commit(self):
        pass  # persist changes

    def rollback(self):
        pass
```

### 3. Write command handlers

```python
def handle_create_user(cmd: CreateUser, uow: AppUnitOfWork):
    with uow:
        user = User(
            identity=cmd.email,
            desc=UserDesc(name=cmd.name, email=cmd.email),
        )
        uow.users.add(user)
        uow.commit()
```

### 4. Wire it up with Bootstrap

```python
from da2 import Bootstrap

def send_welcome_email(event: UserRenamed):
    print(f"User {event.user_id} renamed to {event.new_name}")

bootstrap = Bootstrap(
    uow=AppUnitOfWork(),
    command_handlers={CreateUser: handle_create_user},
    event_handlers={UserRenamed: [send_welcome_email]},
)
bus = bootstrap.create_message_bus()

# Dispatch a command
bus.handle(CreateUser(name="Alice", email="alice@example.com"))
```

Bootstrap auto-injects `uow` (and any extra dependencies) into handler functions by matching parameter names.

## Async Support

Every building block has an async counterpart:

```python
from da2 import UnitOfWorkAsync, BootstrapAsync, MessageBusAsync

class AsyncUoW(UnitOfWorkAsync):
    async def _enter(self): ...
    async def _commit(self): ...
    async def rollback(self): ...
    async def _exit(self, *args): ...

bootstrap = BootstrapAsync(
    uow=AsyncUoW(),
    command_handlers={CreateUser: handle_create_user_async},
    event_handlers={},
)
bus = bootstrap.create_message_bus()
result = await bus.handle(CreateUser(name="Bob", email="bob@example.com"))
```

`MessageBusAsync` also supports lifecycle hooks:

```python
bus.on_before_handle_event(lambda cmd, event: print(f"{cmd} -> {event}"))

@bus.on_bus_event("handle_success")
def log_success(event_type, message, handler_name, reason):
    print(f"OK: {handler_name}")
```

## API Reference

### Core

| Class | Description |
|-------|-------------|
| `Entity[Identity, Description]` | Base class for domain entities with identity, description, and event list |
| `Command` | Base class for commands (write intent) |
| `Event` | Base class for domain events |

### Infrastructure

| Class | Description |
|-------|-------------|
| `UnitOfWork` | Sync context manager, tracks seen entities, collects events |
| `UnitOfWorkAsync` | Async context manager version |
| `Repository[T]` | Abstract sync repository (get/add/update/delete) |
| `RepositoryAsync[T]` | Abstract async repository |
| `InMemoryRepository[T]` | Dict-backed repository for testing |

### Application

| Class | Description |
|-------|-------------|
| `MessageBus` | Sync command/event dispatcher with event chain propagation |
| `MessageBusAsync` | Async version with lifecycle hooks |
| `Bootstrap` | Wires handlers with DI, produces MessageBus |
| `BootstrapAsync` | Async version |
| `Container` | Standalone DI container with recursive resolution |

## Architecture

```
Command --> MessageBus --> CommandHandler --> Entity.raise_event()
                |                                    |
                |              +---------------------+
                |              v
                +-- EventHandler(s) --> side effects / more events
                        |
                UnitOfWork.collect_new_events() feeds back into queue
```

## License

MIT
