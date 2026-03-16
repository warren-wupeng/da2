"""Integration test: Full Redis production stack working together.

Exercises RedisEventStore + RedisSnapshotStore + RedisIdempotencyStore
in a realistic event-sourced application with idempotent commands.
"""

import pytest
import fakeredis

from da2 import (
    Command,
    Event,
    MessageBus,
    UnitOfWork,
    EventSourcedEntity,
    EventSourcedRepository,
    Snapshot,
)
from da2.idempotency import IdempotencyMiddleware
from da2.redis_event_store import RedisEventStore
from da2.redis_snapshot_store import RedisSnapshotStore
from da2.redis_idempotency_store import RedisIdempotencyStore


# --- Domain ---

class AccountOpened(Event):
    def __init__(self, owner: str, initial_deposit: float):
        self.owner = owner
        self.initial_deposit = initial_deposit


class MoneyDeposited(Event):
    def __init__(self, amount: float):
        self.amount = amount


class MoneyWithdrawn(Event):
    def __init__(self, amount: float):
        self.amount = amount


class OpenAccount(Command):
    def __init__(self, account_id: str, owner: str, deposit: float,
                 idempotency_key: str = None):
        self.account_id = account_id
        self.owner = owner
        self.deposit = deposit
        self.idempotency_key = idempotency_key


class DepositMoney(Command):
    def __init__(self, account_id: str, amount: float,
                 idempotency_key: str = None):
        self.account_id = account_id
        self.amount = amount
        self.idempotency_key = idempotency_key


class BankAccount(EventSourcedEntity[str]):
    def __init__(self, identity: str):
        super().__init__(identity)
        self.owner = ""
        self.balance = 0.0

    @classmethod
    def open(cls, account_id: str, owner: str, deposit: float) -> "BankAccount":
        account = cls(account_id)
        account._apply_and_record(AccountOpened(owner=owner, initial_deposit=deposit))
        return account

    def deposit(self, amount: float):
        self._apply_and_record(MoneyDeposited(amount=amount))

    def _when_AccountOpened(self, event):
        self.owner = event.owner
        self.balance = event.initial_deposit

    def _when_MoneyDeposited(self, event):
        self.balance += event.amount

    def _when_MoneyWithdrawn(self, event):
        self.balance -= event.amount


EVENT_REGISTRY = {
    "AccountOpened": AccountOpened,
    "MoneyDeposited": MoneyDeposited,
    "MoneyWithdrawn": MoneyWithdrawn,
}


# --- Infrastructure ---

class BankUoW(UnitOfWork):
    def __init__(self, repo: EventSourcedRepository):
        self.seen: dict = {}
        self.accounts = repo

    def _enter(self):
        pass

    def _commit(self):
        pass

    def rollback(self):
        pass


# --- Fixtures ---

@pytest.fixture
def redis_client():
    return fakeredis.FakeRedis(decode_responses=True)


@pytest.fixture
def event_store(redis_client):
    return RedisEventStore(redis_client, prefix="test:events")


@pytest.fixture
def snapshot_store(redis_client):
    return RedisSnapshotStore(redis_client, prefix="test:snapshots")


@pytest.fixture
def idempotency_store(redis_client):
    return RedisIdempotencyStore(redis_client, prefix="test:idem")


@pytest.fixture
def repo(event_store, snapshot_store):
    return EventSourcedRepository(
        event_store=event_store,
        entity_cls=BankAccount,
        event_registry=EVENT_REGISTRY,
        snapshot_store=snapshot_store,
        snapshot_interval=5,  # snapshot every 5 events
    )


class TestFullRedisIntegration:
    """End-to-end test: event sourcing + snapshots + idempotency, all on Redis."""

    def test_open_account_and_deposit(self, repo, idempotency_store):
        from da2 import Bootstrap

        call_log = []

        def handle_open(cmd: OpenAccount, uow: BankUoW):
            account = BankAccount.open(cmd.account_id, cmd.owner, cmd.deposit)
            uow.accounts.save(account)
            call_log.append(f"opened:{cmd.account_id}")
            return {"account_id": cmd.account_id, "balance": account.balance}

        def handle_deposit(cmd: DepositMoney, uow: BankUoW):
            account = uow.accounts.get(cmd.account_id)
            account.deposit(cmd.amount)
            uow.accounts.save(account)
            call_log.append(f"deposited:{cmd.amount}")
            return {"account_id": cmd.account_id, "balance": account.balance}

        uow = BankUoW(repo)
        bus = Bootstrap(
            uow=uow,
            command_handlers={
                OpenAccount: handle_open,
                DepositMoney: handle_deposit,
            },
            event_handlers={},
            middleware=[IdempotencyMiddleware(idempotency_store)],
        ).create_message_bus()

        # Open account (idempotent)
        r1 = bus.handle(OpenAccount("acc-1", "Alice", 1000.0,
                                     idempotency_key="open-acc-1"))
        assert r1 == {"account_id": "acc-1", "balance": 1000.0}
        assert call_log == ["opened:acc-1"]

        # Duplicate open -- returns cached, handler NOT called
        r2 = bus.handle(OpenAccount("acc-1", "Alice", 1000.0,
                                     idempotency_key="open-acc-1"))
        assert r2 == r1
        assert call_log == ["opened:acc-1"]  # still just 1 call

        # Deposit (idempotent)
        r3 = bus.handle(DepositMoney("acc-1", 500.0,
                                      idempotency_key="dep-1"))
        assert r3 == {"account_id": "acc-1", "balance": 1500.0}

        # Duplicate deposit -- cached
        r4 = bus.handle(DepositMoney("acc-1", 500.0,
                                      idempotency_key="dep-1"))
        assert r4 == r3
        assert call_log == ["opened:acc-1", "deposited:500.0"]

        # Verify from event store
        loaded = repo.get("acc-1")
        assert loaded.balance == 1500.0
        assert loaded.owner == "Alice"
        assert loaded.version == 2

    def test_snapshot_triggers_after_interval(self, repo, snapshot_store,
                                              event_store, redis_client):
        """After 5 events, a snapshot should be created automatically."""
        account = BankAccount.open("acc-2", "Bob", 100.0)
        repo.save(account)

        for i in range(5):
            loaded = repo.get("acc-2")
            loaded.deposit(10.0)
            repo.save(loaded)

        # 6 events total (1 open + 5 deposits), interval=5 -> snapshot taken
        snap = snapshot_store.load("acc-2")
        assert snap is not None
        assert snap.version >= 5

        # Loading should use snapshot + recent events
        final = repo.get("acc-2")
        assert final.balance == 150.0  # 100 + 5*10
        assert final.version == 6

    def test_concurrent_writes_detected(self, repo):
        """Optimistic concurrency via RedisEventStore Lua script."""
        from da2.exceptions import ConcurrencyError

        account = BankAccount.open("acc-3", "Charlie", 500.0)
        repo.save(account)

        reader_a = repo.get("acc-3")
        reader_b = repo.get("acc-3")

        reader_a.deposit(100.0)
        repo.save(reader_a)

        reader_b.deposit(200.0)
        with pytest.raises(ConcurrencyError):
            repo.save(reader_b)

        # Final state reflects only reader_a's write
        final = repo.get("acc-3")
        assert final.balance == 600.0

    def test_all_redis_keys_use_correct_prefixes(self, redis_client, repo,
                                                  idempotency_store):
        """Verify key namespacing -- all stores use distinct prefixes."""
        account = BankAccount.open("acc-4", "Diana", 200.0)
        repo.save(account)
        idempotency_store.save("test-key", "test-result")

        all_keys = redis_client.keys("*")
        prefixes = set()
        for key in all_keys:
            prefix = key.rsplit(":", 1)[0] if ":" in key else key
            prefixes.add(prefix)

        # Should have event store, snapshot store (if triggered), and idempotency keys
        assert any("test:events" in p for p in prefixes)
        assert any("test:idem" in p for p in prefixes)
