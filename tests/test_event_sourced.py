from da2 import Event
from da2.event_sourced import EventSourcedEntity


class AccountOpened(Event):
    def __init__(self, owner: str):
        self.owner = owner


class MoneyDeposited(Event):
    def __init__(self, amount: float):
        self.amount = amount


class MoneyWithdrawn(Event):
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

    def withdraw(self, amount: float) -> None:
        if amount > self.balance:
            raise ValueError(f"Insufficient funds: {self.balance}")
        self._apply_and_record(MoneyWithdrawn(amount=amount))

    def _when_AccountOpened(self, event: AccountOpened) -> None:
        self.owner = event.owner

    def _when_MoneyDeposited(self, event: MoneyDeposited) -> None:
        self.balance += event.amount

    def _when_MoneyWithdrawn(self, event: MoneyWithdrawn) -> None:
        self.balance -= event.amount


class TestEventSourcedEntity:

    def test_create_and_mutate(self):
        account = BankAccount.open("acc-1", "Alice")
        assert account.identity == "acc-1"
        assert account.owner == "Alice"
        assert account.version == 1
        assert len(account.pending_events) == 1

    def test_multiple_events(self):
        account = BankAccount.open("acc-1", "Alice")
        account.deposit(100)
        account.deposit(50)
        account.withdraw(30)
        assert account.balance == 120.0
        assert account.version == 4
        assert len(account.pending_events) == 4

    def test_from_events_reconstructs_state(self):
        events = [
            AccountOpened(owner="Bob"),
            MoneyDeposited(amount=200),
            MoneyWithdrawn(amount=75),
        ]
        account = BankAccount.from_events("acc-2", events)
        assert account.identity == "acc-2"
        assert account.owner == "Bob"
        assert account.balance == 125.0
        assert account.version == 3
        assert account.pending_events == []

    def test_clear_pending_events(self):
        account = BankAccount.open("acc-1", "Alice")
        account.deposit(100)
        cleared = account.clear_pending_events()
        assert len(cleared) == 2
        assert account.pending_events == []
        assert account.version == 2  # version unchanged

    def test_unknown_event_raises_type_error(self):
        class UnknownEvent(Event):
            pass

        account = BankAccount("acc-1")
        try:
            account._apply(UnknownEvent())
            assert False, "Should have raised TypeError"
        except TypeError as e:
            assert "_when_UnknownEvent" in str(e)

    def test_repr(self):
        account = BankAccount.open("acc-1", "Alice")
        assert "BankAccount" in repr(account)
        assert "acc-1" in repr(account)
        assert "version=1" in repr(account)

    def test_business_rule_validation(self):
        account = BankAccount.open("acc-1", "Alice")
        account.deposit(50)
        try:
            account.withdraw(100)
            assert False, "Should have raised ValueError"
        except ValueError as e:
            assert "Insufficient funds" in str(e)
        # No event was recorded for the failed withdrawal
        assert account.version == 2
        assert len(account.pending_events) == 2
