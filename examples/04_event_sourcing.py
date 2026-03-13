"""Event Sourcing example -- state derived from event stream.

Demonstrates:
- EventSourcedEntity with _when_* methods
- EventStore for persisting events
- EventSourcedRepository for load/save
- Optimistic concurrency detection
- State reconstruction from event history

Run::

    cd /path/to/da2
    uv run python examples/04_event_sourcing.py
"""

from da2 import Event, ConcurrencyError
from da2.event_sourced import EventSourcedEntity
from da2.event_store import InMemoryEventStore
from da2.event_sourced_repository import EventSourcedRepository


# --- Domain Events ---

class AccountOpened(Event):
    def __init__(self, owner: str, initial_deposit: float) -> None:
        self.owner = owner
        self.initial_deposit = initial_deposit


class MoneyDeposited(Event):
    def __init__(self, amount: float) -> None:
        self.amount = amount


class MoneyWithdrawn(Event):
    def __init__(self, amount: float) -> None:
        self.amount = amount


# --- Event-Sourced Aggregate ---

class BankAccount(EventSourcedEntity[str]):
    """A bank account whose state is derived from events."""

    def __init__(self, identity: str) -> None:
        super().__init__(identity)
        self.owner: str = ""
        self.balance: float = 0.0

    @classmethod
    def open(cls, account_id: str, owner: str, initial_deposit: float) -> "BankAccount":
        account = cls(account_id)
        account._apply_and_record(
            AccountOpened(owner=owner, initial_deposit=initial_deposit)
        )
        return account

    def deposit(self, amount: float) -> None:
        self._apply_and_record(MoneyDeposited(amount=amount))

    def withdraw(self, amount: float) -> None:
        if amount > self.balance:
            raise ValueError(f"Insufficient funds: balance={self.balance}, requested={amount}")
        self._apply_and_record(MoneyWithdrawn(amount=amount))

    # --- State mutation handlers ---

    def _when_AccountOpened(self, event: AccountOpened) -> None:
        self.owner = event.owner
        self.balance = event.initial_deposit

    def _when_MoneyDeposited(self, event: MoneyDeposited) -> None:
        self.balance += event.amount

    def _when_MoneyWithdrawn(self, event: MoneyWithdrawn) -> None:
        self.balance -= event.amount


# --- Event Registry (for deserialization) ---

EVENT_REGISTRY = {
    "AccountOpened": AccountOpened,
    "MoneyDeposited": MoneyDeposited,
    "MoneyWithdrawn": MoneyWithdrawn,
}


def main() -> None:
    # 1. Create store and repository
    store = InMemoryEventStore()
    repo = EventSourcedRepository(
        event_store=store,
        entity_cls=BankAccount,
        event_registry=EVENT_REGISTRY,
    )

    # 2. Open a new account
    print("=== Opening account ===")
    account = BankAccount.open("ACC-001", owner="Alice", initial_deposit=1000.0)
    print(f"  {account}")
    print(f"  Balance: {account.balance}")
    repo.save(account)
    print(f"  Saved (version {account.version}, pending cleared)")

    # 3. Load and perform transactions
    print("\n=== Transactions ===")
    account = repo.get("ACC-001")
    print(f"  Loaded: {account}, balance={account.balance}")

    account.deposit(500)
    print(f"  Deposited 500 -> balance={account.balance}")

    account.withdraw(200)
    print(f"  Withdrew 200 -> balance={account.balance}")

    repo.save(account)
    print(f"  Saved (version {account.version})")

    # 4. Reconstruct from event history
    print("\n=== Reconstructed from event stream ===")
    fresh = repo.get("ACC-001")
    print(f"  {fresh}")
    print(f"  Owner: {fresh.owner}")
    print(f"  Balance: {fresh.balance}")
    print(f"  Version: {fresh.version}")

    # 5. Inspect the event store
    print("\n=== Event Store contents ===")
    for se in store.load("ACC-001"):
        print(f"  v{se.version}: {se.event_type} {se.data}")

    # 6. Demonstrate optimistic concurrency
    print("\n=== Concurrency conflict ===")
    reader_a = repo.get("ACC-001")
    reader_b = repo.get("ACC-001")

    reader_a.deposit(100)
    repo.save(reader_a)
    print(f"  Reader A saved (balance={reader_a.balance})")

    reader_b.deposit(200)
    try:
        repo.save(reader_b)
        print("  Reader B saved (unexpected!)")
    except ConcurrencyError as e:
        print(f"  Reader B conflict detected: {e}")

    # 7. Final state
    print("\n=== Final state ===")
    final = repo.get("ACC-001")
    print(f"  {final}")
    print(f"  Balance: {final.balance}")
    print(f"  Total events: {final.version}")


if __name__ == "__main__":
    main()
