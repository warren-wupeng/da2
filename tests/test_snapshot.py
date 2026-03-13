import pytest

from da2 import Event
from da2.event_sourced import EventSourcedEntity
from da2.event_store import InMemoryEventStore
from da2.event_sourced_repository import EventSourcedRepository
from da2.snapshot import InMemorySnapshotStore, Snapshot


# --- Domain ---

class AccountOpened(Event):
    def __init__(self, owner: str):
        self.owner = owner


class MoneyDeposited(Event):
    def __init__(self, amount: float):
        self.amount = amount


class BankAccount(EventSourcedEntity[str]):
    def __init__(self, identity: str) -> None:
        super().__init__(identity)
        self.owner: str = ""
        self.balance: float = 0.0

    @classmethod
    def open(cls, account_id: str, owner: str) -> "BankAccount":
        account = cls(account_id)
        account._apply_and_record(AccountOpened(owner=owner))
        return account

    def deposit(self, amount: float) -> None:
        self._apply_and_record(MoneyDeposited(amount=amount))

    def _when_AccountOpened(self, event: AccountOpened) -> None:
        self.owner = event.owner

    def _when_MoneyDeposited(self, event: MoneyDeposited) -> None:
        self.balance += event.amount


EVENT_REGISTRY = {
    "AccountOpened": AccountOpened,
    "MoneyDeposited": MoneyDeposited,
}


class TestSnapshotStore:

    def test_save_and_load(self):
        store = InMemorySnapshotStore()
        snap = Snapshot(aggregate_id="a-1", version=5, state={"balance": 100})
        store.save(snap)
        loaded = store.load("a-1")
        assert loaded is not None
        assert loaded.version == 5
        assert loaded.state == {"balance": 100}

    def test_load_missing_returns_none(self):
        store = InMemorySnapshotStore()
        assert store.load("nonexistent") is None

    def test_save_overwrites(self):
        store = InMemorySnapshotStore()
        store.save(Snapshot(aggregate_id="a-1", version=5, state={"x": 1}))
        store.save(Snapshot(aggregate_id="a-1", version=10, state={"x": 2}))
        loaded = store.load("a-1")
        assert loaded is not None
        assert loaded.version == 10
        assert loaded.state == {"x": 2}


class TestEntitySnapshot:

    def test_take_snapshot(self):
        account = BankAccount.open("acc-1", "Alice")
        account.deposit(100)
        snap = account.take_snapshot()
        assert snap.aggregate_id == "acc-1"
        assert snap.version == 2
        assert snap.state == {"owner": "Alice", "balance": 100.0}

    def test_from_snapshot_no_events(self):
        snap = Snapshot(
            aggregate_id="acc-1",
            version=2,
            state={"owner": "Alice", "balance": 100.0},
        )
        account = BankAccount.from_snapshot("acc-1", snap)
        assert account.owner == "Alice"
        assert account.balance == 100.0
        assert account.version == 2
        assert account.pending_events == []

    def test_from_snapshot_with_events(self):
        snap = Snapshot(
            aggregate_id="acc-1",
            version=2,
            state={"owner": "Alice", "balance": 100.0},
        )
        account = BankAccount.from_snapshot(
            "acc-1", snap, [MoneyDeposited(amount=50)]
        )
        assert account.balance == 150.0
        assert account.version == 3

    def test_roundtrip_snapshot(self):
        original = BankAccount.open("acc-1", "Alice")
        for _ in range(10):
            original.deposit(10)
        snap = original.take_snapshot()

        restored = BankAccount.from_snapshot("acc-1", snap)
        assert restored.owner == original.owner
        assert restored.balance == original.balance
        assert restored.version == original.version


class TestRepositoryWithSnapshot:

    def test_auto_snapshot_on_interval(self):
        event_store = InMemoryEventStore()
        snap_store = InMemorySnapshotStore()
        repo = EventSourcedRepository(
            event_store=event_store,
            entity_cls=BankAccount,
            event_registry=EVENT_REGISTRY,
            snapshot_store=snap_store,
            snapshot_interval=5,
        )

        account = BankAccount.open("acc-1", "Alice")
        for _ in range(5):
            account.deposit(10)
        # version=6, crosses interval boundary (0->5)
        repo.save(account)

        snap = snap_store.load("acc-1")
        assert snap is not None
        assert snap.version == 6
        assert snap.state["balance"] == 50.0

    def test_load_uses_snapshot(self):
        event_store = InMemoryEventStore()
        snap_store = InMemorySnapshotStore()
        repo = EventSourcedRepository(
            event_store=event_store,
            entity_cls=BankAccount,
            event_registry=EVENT_REGISTRY,
            snapshot_store=snap_store,
            snapshot_interval=5,
        )

        # Create account with 6 events -> triggers snapshot at v6
        account = BankAccount.open("acc-1", "Alice")
        for _ in range(5):
            account.deposit(10)
        repo.save(account)

        # Add 2 more events (v7, v8)
        loaded = repo.get("acc-1")
        loaded.deposit(100)
        loaded.deposit(200)
        repo.save(loaded)

        # Now load again -> should use snapshot(v6) + events(v7,v8)
        final = repo.get("acc-1")
        assert final.balance == 350.0  # 50 + 100 + 200
        assert final.version == 8

    def test_no_snapshot_store_works_as_before(self):
        event_store = InMemoryEventStore()
        repo = EventSourcedRepository(
            event_store=event_store,
            entity_cls=BankAccount,
            event_registry=EVENT_REGISTRY,
        )
        account = BankAccount.open("acc-1", "Alice")
        account.deposit(100)
        repo.save(account)

        loaded = repo.get("acc-1")
        assert loaded.balance == 100.0

    def test_load_since(self):
        store = InMemoryEventStore()
        store.append("a-1", [AccountOpened(owner="Alice")], expected_version=0)
        store.append("a-1", [MoneyDeposited(amount=50)], expected_version=1)
        store.append("a-1", [MoneyDeposited(amount=100)], expected_version=2)

        since = store.load_since("a-1", after_version=1)
        assert len(since) == 2
        assert since[0].version == 2
        assert since[1].version == 3
