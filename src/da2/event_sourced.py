"""Event-sourced entity whose state is derived from a stream of events.

Unlike ``Entity`` (state-based), an ``EventSourcedEntity`` reconstructs
its state by replaying domain events. New state changes are expressed by
applying new events, which are collected as *pending* until persisted.
"""

from __future__ import annotations

from typing import Any, Generic, TypeVar

from .event import Event

Identity = TypeVar("Identity")


class EventSourcedEntity(Generic[Identity]):
    """Base class for event-sourced aggregates.

    State is built by replaying events through ``_when_<EventClassName>``
    methods.  New state changes are made by calling ``_apply_and_record()``,
    which both mutates state and records the event as *pending*.

    Example::

        from da2 import Event
        from da2.event_sourced import EventSourcedEntity

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

        account = BankAccount.open("acc-1", "Alice")
        account.deposit(100)
        account.deposit(50)
        assert account.balance == 150.0
        assert account.version == 3
        assert len(account.pending_events) == 3
    """

    def __init__(self, identity: Identity) -> None:
        self._identity = identity
        self._version: int = 0
        self._pending_events: list[Event] = []

    @property
    def identity(self) -> Identity:
        """The unique identifier of this aggregate."""
        return self._identity

    @property
    def version(self) -> int:
        """Number of events applied (both historical and pending)."""
        return self._version

    @property
    def pending_events(self) -> list[Event]:
        """Uncommitted events waiting to be persisted to the event store."""
        return list(self._pending_events)

    def clear_pending_events(self) -> list[Event]:
        """Remove and return all pending events."""
        events = self._pending_events
        self._pending_events = []
        return events

    def _apply(self, event: Event) -> None:
        """Apply an event to mutate state (no recording).

        Routes to ``_when_<EventClassName>(event)``. Raises ``TypeError``
        if no matching method is found.
        """
        method_name = f"_when_{type(event).__name__}"
        method = getattr(self, method_name, None)
        if method is None:
            raise TypeError(
                f"{self.__class__.__name__} has no method '{method_name}'. "
                f"Add a method: def {method_name}(self, event): ..."
            )
        method(event)
        self._version += 1

    def _apply_and_record(self, event: Event) -> None:
        """Apply an event and record it as pending (for new state changes)."""
        self._apply(event)
        self._pending_events.append(event)

    @classmethod
    def from_events(
        cls,
        identity: Any,
        events: list[Event],
    ) -> "EventSourcedEntity":
        """Reconstruct an entity by replaying a list of historical events.

        Example::

            stored_events = [AccountOpened(owner="Alice"), MoneyDeposited(amount=100)]
            account = BankAccount.from_events("acc-1", stored_events)
            assert account.balance == 100.0
            assert account.version == 2
            assert account.pending_events == []  # no new events
        """
        entity = cls(identity)
        for event in events:
            entity._apply(event)
        return entity

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}"
            f"(identity={self._identity!r}, version={self._version})"
        )
