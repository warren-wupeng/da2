# da2

Python DDD and Event-Driven Architecture framework with Event Sourcing.

Lightweight building blocks for Domain-Driven Design: Entity, Repository, Unit of Work, Command/Event, MessageBus, Bootstrap DI, Event Sourcing, Snapshots, Projections, Process Manager, and Policy.

## Install

```bash
pip install da2          # core (zero dependencies)
pip install da2[redis]   # + Redis-backed EventStore
```

## Quick Start

### One-liner bootstrap

```python
from da2 import Entity, Command, Event, UnitOfWork, bootstrap

class CreateUser(Command):
    def __init__(self, name: str):
        self.name = name

class UserCreated(Event):
    def __init__(self, name: str):
        self.name = name

class User(Entity[str, dict]):
    pass

class MyUoW(UnitOfWork):
    def _enter(self): pass
    def _commit(self): pass
    def rollback(self): pass

def handle_create(cmd: CreateUser, uow):
    print(f"Created {cmd.name}")

bus = bootstrap(
    uow=MyUoW(),
    commands={CreateUser: handle_create},
    events={UserCreated: [lambda e: print(f"Welcome {e.name}")]},
)
bus.handle(CreateUser(name="Alice"))
```

### Full example with Repository

```python
from da2 import Entity, Command, Event, UnitOfWork, InMemoryRepository, Bootstrap

class UserCreated(Event):
    def __init__(self, user_id: str):
        self.user_id = user_id

class CreateUser(Command):
    def __init__(self, name: str, email: str):
        self.name = name
        self.email = email

class User(Entity[str, dict]):
    @classmethod
    def create(cls, email: str, name: str) -> "User":
        user = cls(identity=email, desc={"name": name, "email": email})
        user.raise_event(UserCreated(user_id=email))
        return user

class AppUoW(UnitOfWork):
    def _enter(self):
        self.users = InMemoryRepository[User](add_seen=self.add_seen)
    def _commit(self):
        pass
    def rollback(self):
        pass

def handle_create_user(cmd: CreateUser, uow: AppUoW):
    user = User.create(cmd.email, cmd.name)
    uow.users.add(user)

def on_user_created(event: UserCreated):
    print(f"Send welcome email to {event.user_id}")

bootstrap = Bootstrap(
    uow=AppUoW(),
    command_handlers={CreateUser: handle_create_user},
    event_handlers={UserCreated: [on_user_created]},
)
bus = bootstrap.create_message_bus()
bus.uow.seen = {}
bus.handle(CreateUser(name="Alice", email="alice@example.com"))
```

Bootstrap auto-injects `uow` (and any extra dependencies) into handler functions by matching parameter names.

## Event Sourcing

da2 supports two persistence models:

1. **State-based** (Entity + Repository) -- store current state directly
2. **Event-sourced** (EventSourcedEntity + EventStore) -- store events, derive state

### Define an event-sourced aggregate

```python
from da2 import Event
from da2.event_sourced import EventSourcedEntity

class AccountOpened(Event):
    def __init__(self, owner: str):
        self.owner = owner

class MoneyDeposited(Event):
    def __init__(self, amount: float):
        self.amount = amount

class BankAccount(EventSourcedEntity[str]):
    def __init__(self, identity: str):
        super().__init__(identity)
        self.owner = ""
        self.balance = 0.0

    @classmethod
    def open(cls, account_id: str, owner: str) -> "BankAccount":
        account = cls(account_id)
        account._apply_and_record(AccountOpened(owner=owner))
        return account

    def deposit(self, amount: float):
        self._apply_and_record(MoneyDeposited(amount=amount))

    def _when_AccountOpened(self, event):
        self.owner = event.owner

    def _when_MoneyDeposited(self, event):
        self.balance += event.amount
```

State is built by replaying events through `_when_<EventClassName>` methods.

### Persist and load via EventStore

```python
from da2.event_store import InMemoryEventStore
from da2.event_sourced_repository import EventSourcedRepository

store = InMemoryEventStore()
repo = EventSourcedRepository(
    event_store=store,
    entity_cls=BankAccount,
    event_registry={"AccountOpened": AccountOpened, "MoneyDeposited": MoneyDeposited},
)

account = BankAccount.open("acc-1", "Alice")
account.deposit(100)
repo.save(account)          # persists 2 events to store

loaded = repo.get("acc-1")  # replays events, reconstructs state
assert loaded.balance == 100.0
```

### Optimistic concurrency

```python
reader_a = repo.get("acc-1")
reader_b = repo.get("acc-1")
reader_a.deposit(50)
repo.save(reader_a)    # OK
reader_b.deposit(100)
repo.save(reader_b)    # raises ConcurrencyError
```

### Snapshots

Snapshots avoid replaying the full event history:

```python
from da2.snapshot import InMemorySnapshotStore

repo = EventSourcedRepository(
    event_store=store,
    entity_cls=BankAccount,
    event_registry={...},
    snapshot_store=InMemorySnapshotStore(),
    snapshot_interval=100,   # auto-snapshot every 100 events
)

# After 100+ events, repo.get() loads snapshot + recent events only
```

### Redis EventStore

For production, use `RedisEventStore` (requires `pip install da2[redis]`):

```python
import redis
from da2 import RedisEventStore

r = redis.Redis(host="localhost", port=6379, decode_responses=True)
store = RedisEventStore(r, prefix="myapp:events")

# Use like InMemoryEventStore -- same API, backed by Redis
repo = EventSourcedRepository(
    event_store=store, entity_cls=BankAccount, event_registry={...},
)
```

Atomic optimistic concurrency via Lua scripting. `RedisEventStoreAsync` for async.

`RedisSnapshotStore` / `RedisSnapshotStoreAsync` also available for snapshot persistence.

## Projections (CQRS Read Models)

Projections build read-optimized views by applying domain events:

```python
from da2 import Projection, Event, StoredEvent

class OrderPlaced(Event):
    def __init__(self, order_id: str, amount: float):
        self.order_id = order_id
        self.amount = amount

class OrderTotals(Projection):
    def __init__(self):
        super().__init__()
        self.totals: dict[str, float] = {}

    def _on_OrderPlaced(self, event: OrderPlaced):
        self.totals[event.order_id] = event.amount
```

Convention: `_on_<EventClassName>` handlers (similar to `_when_*` in EventSourcedEntity).

### Replay and catch-up

```python
event_registry = {"OrderPlaced": OrderPlaced}

# Full replay from stored events
proj = OrderTotals()
proj.replay(stored_events, event_registry)

# Incremental catch-up (skips events already processed)
proj.catch_up(new_stored_events, event_registry)
print(proj.last_version)  # tracks position
```

`ProjectionAsync` supports both async and sync `_on_*` handlers.

## Process Manager (Saga)

Process Managers orchestrate long-running workflows across multiple aggregates by reacting to events and issuing commands:

```python
from da2 import Command, Event
from da2.process_manager import ProcessManager

class ReserveStock(Command):
    def __init__(self, order_id: str):
        self.order_id = order_id

class ChargePayment(Command):
    def __init__(self, order_id: str, amount: float):
        self.order_id = order_id
        self.amount = amount

class OrderPlaced(Event):
    def __init__(self, order_id: str, amount: float):
        self.order_id = order_id
        self.amount = amount

class StockReserved(Event):
    def __init__(self, order_id: str):
        self.order_id = order_id

class OrderFulfillment(ProcessManager):
    def __init__(self, process_id: str):
        super().__init__(process_id)
        self.amount = 0.0

    def _on_OrderPlaced(self, event: OrderPlaced):
        self.amount = event.amount
        self._dispatch(ReserveStock(order_id=event.order_id))

    def _on_StockReserved(self, event: StockReserved):
        self._dispatch(ChargePayment(order_id=event.order_id, amount=self.amount))
        self._mark_completed()

pm = OrderFulfillment("order-1")
cmds = pm.handle(OrderPlaced(order_id="order-1", amount=99.0))
# cmds == [ReserveStock(order_id="order-1")]
```

`handle(event)` returns the list of commands to dispatch. `completed` tracks terminal state. `ProcessManagerAsync` supports async handlers.

## Policy (Stateless Reactor)

Policies react to events by issuing commands without maintaining state -- simple "when X happens, do Y" rules:

```python
from da2 import Command, Event
from da2.policy import Policy

class SendWelcomeEmail(Command):
    def __init__(self, email: str):
        self.email = email

class UserRegistered(Event):
    def __init__(self, email: str):
        self.email = email

class WelcomePolicy(Policy):
    def _on_UserRegistered(self, event: UserRegistered):
        self._dispatch(SendWelcomeEmail(email=event.email))

policy = WelcomePolicy()
cmds = policy.handle(UserRegistered(email="alice@example.com"))
# cmds == [SendWelcomeEmail(email="alice@example.com")]
```

Policy vs ProcessManager: Policy is stateless (no `process_id`, no `completed`). Use Policy for simple reactive rules; ProcessManager for multi-step workflows. `PolicyAsync` supports async handlers.

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

Event Sourcing also has full async support:

| Sync | Async |
|------|-------|
| `EventStore` | `EventStoreAsync` |
| `InMemoryEventStore` | `InMemoryEventStoreAsync` |
| `EventSourcedRepository` | `EventSourcedRepositoryAsync` |
| `SnapshotStore` | `SnapshotStoreAsync` |
| `InMemorySnapshotStore` | `InMemorySnapshotStoreAsync` |
| `Projection` | `ProjectionAsync` |
| `ProcessManager` | `ProcessManagerAsync` |
| `Policy` | `PolicyAsync` |
| `RedisEventStore` | `RedisEventStoreAsync` |
| `RedisSnapshotStore` | `RedisSnapshotStoreAsync` |

`MessageBusAsync` supports lifecycle hooks:

```python
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
| `Event` | Base class for domain events; supports `to_dict()` / `from_dict()` |

### Infrastructure

| Class | Description |
|-------|-------------|
| `UnitOfWork` / `UnitOfWorkAsync` | Transaction boundary, tracks entities, collects events |
| `Repository[T]` / `RepositoryAsync[T]` | Abstract persistence (get/add/update/delete) |
| `InMemoryRepository[T]` | Dict-backed repository for testing |

### Application

| Class | Description |
|-------|-------------|
| `MessageBus` / `MessageBusAsync` | Command/event dispatcher with event chain propagation |
| `Bootstrap` / `BootstrapAsync` | Wires handlers with DI, produces MessageBus |
| `Container` | Standalone DI container with recursive resolution |

### Event Sourcing

| Class | Description |
|-------|-------------|
| `EventSourcedEntity[Identity]` | Aggregate whose state is derived from events |
| `EventStore` / `EventStoreAsync` | Append-only event persistence with optimistic concurrency |
| `InMemoryEventStore` / `InMemoryEventStoreAsync` | Dict-backed event store for testing |
| `RedisEventStore` / `RedisEventStoreAsync` | Redis-backed event store with Lua concurrency (requires `da2[redis]`) |
| `RedisSnapshotStore` / `RedisSnapshotStoreAsync` | Redis-backed snapshot store (requires `da2[redis]`) |
| `StoredEvent` | Immutable envelope for a persisted event |
| `EventSourcedRepository` / `EventSourcedRepositoryAsync` | Load/save event-sourced entities |
| `Snapshot` | Captured state at a point in time |
| `SnapshotStore` / `SnapshotStoreAsync` | Snapshot persistence |
| `InMemorySnapshotStore` / `InMemorySnapshotStoreAsync` | Dict-backed snapshot store for testing |
| `ConcurrencyError` | Raised on version conflict |

### Projections

| Class | Description |
|-------|-------------|
| `Projection` / `ProjectionAsync` | CQRS read model with `_on_<EventClassName>` dispatch, replay, and catch-up |

### Process Manager

| Class | Description |
|-------|-------------|
| `ProcessManager` / `ProcessManagerAsync` | Orchestrates multi-aggregate workflows; `handle(event) -> list[Command]` |

### Policy

| Class | Description |
|-------|-------------|
| `Policy` / `PolicyAsync` | Stateless event-to-command reactor; `handle(event) -> list[Command]` |

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

Event Sourcing flow:

```
BankAccount._apply_and_record(MoneyDeposited(100))
    --> _when_MoneyDeposited(event)   # mutates state
    --> event added to pending_events
    --> repo.save(account)            # appends to EventStore
    --> repo.get("acc-1")             # replays events (or snapshot + delta)
```

Projection flow:

```
EventStore.load() --> StoredEvent[] --> Projection.replay(events, registry)
                                            --> _on_OrderPlaced(event)
                                            --> _on_OrderShipped(event)
                                            --> last_version updated

New events    --> Projection.catch_up(new_events, registry)
                      --> skips already-processed versions
                      --> applies only new events
```

Process Manager flow:

```
Event --> ProcessManager.handle(event)
              --> _on_OrderPlaced(event)
              --> _dispatch(ReserveStock(...))
              --> returns [ReserveStock(...)]   # feed back into MessageBus
```

## License

MIT
