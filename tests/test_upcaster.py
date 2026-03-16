"""Tests for event upcasting (schema evolution)."""

import pytest
from da2 import (
    Event,
    EventSourcedEntity,
    EventSourcedRepository,
    InMemoryEventStore,
    StoredEvent,
    Upcaster,
    UpcasterChain,
)


# --- Domain events ---

class OrderPlacedV1(Event):
    """Original schema: only total."""
    def __init__(self, total: float):
        self.total = total


class OrderPlaced(Event):
    """Current schema: total + currency."""
    def __init__(self, total: float, currency: str = "USD"):
        self.total = total
        self.currency = currency


class OrderCreated(Event):
    """Renamed event type."""
    def __init__(self, total: float, currency: str = "USD"):
        self.total = total
        self.currency = currency


class ItemAdded(Event):
    def __init__(self, product: str, quantity: int):
        self.product = product
        self.quantity = quantity


# --- Entity ---

class Order(EventSourcedEntity[str]):
    def __init__(self, identity: str):
        super().__init__(identity)
        self.total = 0.0
        self.currency = ""
        self.items: list[str] = []

    def _when_OrderPlaced(self, event):
        self.total = event.total
        self.currency = event.currency

    def _when_OrderCreated(self, event):
        self.total = event.total
        self.currency = event.currency

    def _when_ItemAdded(self, event):
        self.items.append(event.product)


# --- Upcasters ---

class AddCurrencyField(Upcaster):
    """V1 -> V2: add default currency."""
    event_type = "OrderPlaced"

    def upcast(self, data: dict) -> dict:
        data.setdefault("currency", "USD")
        return data


class RenameAmountToTotal(Upcaster):
    """Legacy: rename 'amount' -> 'total'."""
    event_type = "OrderPlaced"

    def upcast(self, data: dict) -> dict:
        if "amount" in data:
            data["total"] = data.pop("amount")
        return data


class RenameOrderPlacedToCreated(Upcaster):
    """Rename event type: OrderPlaced -> OrderCreated."""
    event_type = "OrderPlaced"
    target_event_type = "OrderCreated"

    def upcast(self, data: dict) -> dict:
        return data


class AddDefaultQuantity(Upcaster):
    """ItemAdded: add default quantity if missing."""
    event_type = "ItemAdded"

    def upcast(self, data: dict) -> dict:
        data.setdefault("quantity", 1)
        return data


# --- Unit tests: UpcasterChain ---

class TestUpcasterChain:
    def test_no_upcasters_passes_through(self):
        chain = UpcasterChain()
        et, data = chain.upcast("OrderPlaced", {"total": 99.0})
        assert et == "OrderPlaced"
        assert data == {"total": 99.0}

    def test_single_upcaster_adds_field(self):
        chain = UpcasterChain([AddCurrencyField()])
        et, data = chain.upcast("OrderPlaced", {"total": 99.0})
        assert et == "OrderPlaced"
        assert data == {"total": 99.0, "currency": "USD"}

    def test_upcaster_does_not_match_other_types(self):
        chain = UpcasterChain([AddCurrencyField()])
        et, data = chain.upcast("ItemAdded", {"product": "Widget"})
        assert et == "ItemAdded"
        assert data == {"product": "Widget"}

    def test_multiple_upcasters_same_type(self):
        chain = UpcasterChain([RenameAmountToTotal(), AddCurrencyField()])
        et, data = chain.upcast("OrderPlaced", {"amount": 50.0})
        assert et == "OrderPlaced"
        assert data == {"total": 50.0, "currency": "USD"}

    def test_rename_event_type(self):
        chain = UpcasterChain([RenameOrderPlacedToCreated()])
        et, data = chain.upcast("OrderPlaced", {"total": 99.0})
        assert et == "OrderCreated"
        assert data == {"total": 99.0}

    def test_rename_then_later_upcaster_uses_new_name(self):
        """After renaming OrderPlaced -> OrderCreated, a subsequent
        upcaster matching 'OrderCreated' should fire."""

        class AddTimestamp(Upcaster):
            event_type = "OrderCreated"

            def upcast(self, data: dict) -> dict:
                data["created_at"] = "2026-01-01"
                return data

        chain = UpcasterChain([
            RenameOrderPlacedToCreated(),
            AddTimestamp(),
        ])
        et, data = chain.upcast("OrderPlaced", {"total": 99.0})
        assert et == "OrderCreated"
        assert data == {"total": 99.0, "created_at": "2026-01-01"}

    def test_input_data_not_mutated(self):
        chain = UpcasterChain([AddCurrencyField()])
        original = {"total": 99.0}
        chain.upcast("OrderPlaced", original)
        assert "currency" not in original

    def test_multiple_event_types(self):
        chain = UpcasterChain([AddCurrencyField(), AddDefaultQuantity()])
        et1, d1 = chain.upcast("OrderPlaced", {"total": 10.0})
        assert d1 == {"total": 10.0, "currency": "USD"}

        et2, d2 = chain.upcast("ItemAdded", {"product": "X"})
        assert d2 == {"product": "X", "quantity": 1}

    def test_add_upcaster(self):
        chain = UpcasterChain()
        assert len(chain) == 0
        chain.add(AddCurrencyField())
        assert len(chain) == 1
        et, data = chain.upcast("OrderPlaced", {"total": 5.0})
        assert data["currency"] == "USD"

    def test_len_and_repr(self):
        chain = UpcasterChain([AddCurrencyField()])
        assert len(chain) == 1
        assert "UpcasterChain" in repr(chain)

    def test_already_current_data_unchanged(self):
        chain = UpcasterChain([AddCurrencyField()])
        et, data = chain.upcast("OrderPlaced", {"total": 99.0, "currency": "EUR"})
        assert data == {"total": 99.0, "currency": "EUR"}


# --- Integration with EventSourcedRepository ---

def _inject_stored_event(store, aggregate_id, event_type, data, version):
    """Directly insert a StoredEvent to simulate legacy data."""
    stream = store._streams.setdefault(aggregate_id, [])
    stream.append(StoredEvent(
        aggregate_id=aggregate_id,
        event_type=event_type,
        data=data,
        version=version,
    ))


class TestUpcasterRepositoryIntegration:
    def test_load_with_upcaster_adds_missing_field(self):
        """Events stored without 'currency' are upcasted on load."""
        store = InMemoryEventStore()
        # Simulate legacy: stored as "OrderPlaced" with v1 data (no currency)
        _inject_stored_event(store, "ord-1", "OrderPlaced", {"total": 99.0}, 1)

        chain = UpcasterChain([AddCurrencyField()])
        repo = EventSourcedRepository(
            event_store=store,
            entity_cls=Order,
            event_registry={"OrderPlaced": OrderPlaced},
            upcaster_chain=chain,
        )
        order = repo.get("ord-1")
        assert order.total == 99.0
        assert order.currency == "USD"

    def test_load_with_rename_upcaster(self):
        """Stored 'OrderPlaced' events are renamed to 'OrderCreated'."""
        store = InMemoryEventStore()
        _inject_stored_event(store, "ord-2", "OrderPlaced", {"total": 50.0}, 1)

        chain = UpcasterChain([
            AddCurrencyField(),
            RenameOrderPlacedToCreated(),
        ])
        repo = EventSourcedRepository(
            event_store=store,
            entity_cls=Order,
            event_registry={"OrderCreated": OrderCreated, "ItemAdded": ItemAdded},
            upcaster_chain=chain,
        )
        order = repo.get("ord-2")
        assert order.total == 50.0
        assert order.currency == "USD"

    def test_load_without_upcaster_still_works(self):
        """No upcaster chain -- existing behavior unchanged."""
        store = InMemoryEventStore()
        store.append("ord-3", [OrderPlaced(total=75.0, currency="EUR")],
                      expected_version=0)

        repo = EventSourcedRepository(
            event_store=store,
            entity_cls=Order,
            event_registry={"OrderPlaced": OrderPlaced},
        )
        order = repo.get("ord-3")
        assert order.total == 75.0
        assert order.currency == "EUR"

    def test_chained_upcasters_with_multi_event_stream(self):
        """Mixed event types, each upcasted independently."""
        store = InMemoryEventStore()
        # Legacy OrderPlaced (no currency) + legacy ItemAdded (no quantity)
        _inject_stored_event(store, "ord-4", "OrderPlaced", {"total": 100.0}, 1)
        _inject_stored_event(store, "ord-4", "ItemAdded",
                             {"product": "Widget"}, 2)

        chain = UpcasterChain([AddCurrencyField(), AddDefaultQuantity()])
        repo = EventSourcedRepository(
            event_store=store,
            entity_cls=Order,
            event_registry={"OrderPlaced": OrderPlaced, "ItemAdded": ItemAdded},
            upcaster_chain=chain,
        )
        order = repo.get("ord-4")
        assert order.total == 100.0
        assert order.currency == "USD"
        assert order.items == ["Widget"]
