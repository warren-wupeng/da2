import pytest

from da2 import Event, ConcurrencyError
from da2.event_store import InMemoryEventStore, StoredEvent


class OrderPlaced(Event):
    def __init__(self, product: str, total: float):
        self.product = product
        self.total = total


class OrderShipped(Event):
    def __init__(self, tracking: str):
        self.tracking = tracking


class TestInMemoryEventStore:

    def test_append_and_load(self):
        store = InMemoryEventStore()
        store.append("order-1", [OrderPlaced(product="Widget", total=9.99)], expected_version=0)
        events = store.load("order-1")
        assert len(events) == 1
        assert events[0].aggregate_id == "order-1"
        assert events[0].event_type == "OrderPlaced"
        assert events[0].data == {"product": "Widget", "total": 9.99}
        assert events[0].version == 1

    def test_append_multiple_events(self):
        store = InMemoryEventStore()
        store.append(
            "order-1",
            [OrderPlaced(product="Widget", total=9.99), OrderShipped(tracking="TRK-001")],
            expected_version=0,
        )
        events = store.load("order-1")
        assert len(events) == 2
        assert events[0].version == 1
        assert events[1].version == 2
        assert events[1].event_type == "OrderShipped"

    def test_append_incremental(self):
        store = InMemoryEventStore()
        store.append("order-1", [OrderPlaced(product="Widget", total=9.99)], expected_version=0)
        store.append("order-1", [OrderShipped(tracking="TRK-001")], expected_version=1)
        events = store.load("order-1")
        assert len(events) == 2
        assert events[1].version == 2

    def test_concurrency_error(self):
        store = InMemoryEventStore()
        store.append("order-1", [OrderPlaced(product="Widget", total=9.99)], expected_version=0)
        with pytest.raises(ConcurrencyError) as exc_info:
            store.append("order-1", [OrderShipped(tracking="TRK-001")], expected_version=0)
        assert "Expected version 0" in str(exc_info.value)
        assert "current version is 1" in str(exc_info.value)

    def test_load_empty_returns_empty_list(self):
        store = InMemoryEventStore()
        assert store.load("nonexistent") == []

    def test_separate_aggregates(self):
        store = InMemoryEventStore()
        store.append("order-1", [OrderPlaced(product="A", total=1.0)], expected_version=0)
        store.append("order-2", [OrderPlaced(product="B", total=2.0)], expected_version=0)
        assert len(store.load("order-1")) == 1
        assert len(store.load("order-2")) == 1
        assert store.load("order-1")[0].data["product"] == "A"
        assert store.load("order-2")[0].data["product"] == "B"

    def test_stored_event_is_immutable(self):
        store = InMemoryEventStore()
        store.append("order-1", [OrderPlaced(product="Widget", total=9.99)], expected_version=0)
        events = store.load("order-1")
        with pytest.raises(AttributeError):
            events[0].version = 999


class TestEventSerialization:

    def test_to_dict(self):
        event = OrderPlaced(product="Gadget", total=19.99)
        d = event.to_dict()
        assert d == {"product": "Gadget", "total": 19.99}

    def test_from_dict(self):
        event = OrderPlaced.from_dict({"product": "Gadget", "total": 19.99})
        assert event.product == "Gadget"
        assert event.total == 19.99

    def test_roundtrip(self):
        original = OrderPlaced(product="Gadget", total=19.99)
        restored = OrderPlaced.from_dict(original.to_dict())
        assert restored.product == original.product
        assert restored.total == original.total
