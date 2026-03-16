# Changelog

## 0.9.0

- Add **Idempotent Command Handling** via middleware
- `IdempotencyStore` / `IdempotencyStoreAsync` protocol for pluggable storage
- `InMemoryIdempotencyStore` / `InMemoryIdempotencyStoreAsync` with TTL expiration
- `IdempotencyMiddleware` / `IdempotencyMiddlewareAsync` -- opt-in via `idempotency_key` attribute
- Failed commands are NOT cached (safe to retry on error)
- Composable with other middleware (logging, retry, etc.)

## 0.8.0

- Add **Middleware** pipeline for `MessageBus` and `MessageBusAsync`
- Middleware wraps the entire `handle()` call: `(message, next) -> result`
- Composable chain: logging, retry, validation, error handling, etc.
- `Bootstrap` / `BootstrapAsync` / `bootstrap()` accept optional `middleware` parameter
- `MessageBus.handle()` now returns the command handler's result (backward-compatible)

## 0.7.1

- Add `RedisSnapshotStore` / `RedisSnapshotStoreAsync` for production snapshot persistence
- Complete Redis production story: EventStore + SnapshotStore

## 0.7.0

- Add `RedisEventStore` / `RedisEventStoreAsync` with Lua-based atomic optimistic concurrency
- Lazy imports for Redis classes (no import error without `redis` installed)
- Requires `pip install da2[redis]` for Redis support

## 0.6.0

- Add `Policy` / `PolicyAsync` -- stateless event-to-command reactors
- Convention: `_on_<EventClassName>` handlers, `_dispatch(cmd)`, `handle(event) -> list[Command]`

## 0.5.0

- Add `ProcessManager` / `ProcessManagerAsync` -- stateful saga orchestration
- Convention: `_on_<EventClassName>` handlers, `_dispatch(cmd)`, `_mark_completed()`
- Tracks `process_id` and `completed` state

## 0.4.0

- Add `Projection` / `ProjectionAsync` -- CQRS read models
- Convention: `_on_<EventClassName>` handlers
- `apply()`, `replay()`, `catch_up()` for live, full, and incremental processing

## 0.3.1

- Add `Snapshot` / `SnapshotStore` / `SnapshotStoreAsync` for event replay optimization
- Add `InMemorySnapshotStore` / `InMemorySnapshotStoreAsync`
- Full async support for Event Sourcing: `EventStoreAsync`, `EventSourcedRepositoryAsync`

## 0.3.0

- Add Event Sourcing: `EventSourcedEntity`, `EventStore`, `EventSourcedRepository`
- `_when_<EventClassName>` convention for state mutation
- `StoredEvent` immutable envelope with `to_dict()` / `from_dict()` serialization
- Optimistic concurrency via version-based `ConcurrencyError`

## 0.2.0

- Add `MessageBusAsync`, `UnitOfWorkAsync`, `RepositoryAsync`, `BootstrapAsync`
- Add `Container` for standalone DI
- `MessageBusAsync` lifecycle hooks (`on_bus_event`)

## 0.1.0

- Initial release
- `Entity`, `Command`, `Event`, `UnitOfWork`, `Repository`, `InMemoryRepository`
- `MessageBus`, `Bootstrap` with dependency injection
- `bootstrap()` one-liner shortcut
