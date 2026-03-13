import pytest

from da2 import Event, ConcurrencyError
from da2.event_store_async import InMemoryEventStoreAsync


class OrderPlaced(Event):
    def __init__(self, product: str, total: float):
        self.product = product
        self.total = total


class OrderShipped(Event):
    def __init__(self, tracking: str):
        self.tracking = tracking


class TestInMemoryEventStoreAsync:

    async def test_append_and_load(self):
        store = InMemoryEventStoreAsync()
        await store.append("order-1", [OrderPlaced(product="Widget", total=9.99)], expected_version=0)
        events = await store.load("order-1")
        assert len(events) == 1
        assert events[0].event_type == "OrderPlaced"
        assert events[0].data == {"product": "Widget", "total": 9.99}
        assert events[0].version == 1

    async def test_append_multiple(self):
        store = InMemoryEventStoreAsync()
        await store.append(
            "order-1",
            [OrderPlaced(product="A", total=1.0), OrderShipped(tracking="T-1")],
            expected_version=0,
        )
        events = await store.load("order-1")
        assert len(events) == 2
        assert events[0].version == 1
        assert events[1].version == 2

    async def test_append_incremental(self):
        store = InMemoryEventStoreAsync()
        await store.append("o-1", [OrderPlaced(product="A", total=1.0)], expected_version=0)
        await store.append("o-1", [OrderShipped(tracking="T-1")], expected_version=1)
        events = await store.load("o-1")
        assert len(events) == 2

    async def test_concurrency_error(self):
        store = InMemoryEventStoreAsync()
        await store.append("o-1", [OrderPlaced(product="A", total=1.0)], expected_version=0)
        with pytest.raises(ConcurrencyError):
            await store.append("o-1", [OrderShipped(tracking="T-1")], expected_version=0)

    async def test_load_empty(self):
        store = InMemoryEventStoreAsync()
        assert await store.load("nonexistent") == []

    async def test_load_since(self):
        store = InMemoryEventStoreAsync()
        await store.append("o-1", [OrderPlaced(product="A", total=1.0)], expected_version=0)
        await store.append("o-1", [OrderPlaced(product="B", total=2.0)], expected_version=1)
        await store.append("o-1", [OrderShipped(tracking="T-1")], expected_version=2)

        since = await store.load_since("o-1", after_version=1)
        assert len(since) == 2
        assert since[0].version == 2
        assert since[1].version == 3

    async def test_separate_aggregates(self):
        store = InMemoryEventStoreAsync()
        await store.append("o-1", [OrderPlaced(product="A", total=1.0)], expected_version=0)
        await store.append("o-2", [OrderPlaced(product="B", total=2.0)], expected_version=0)
        assert len(await store.load("o-1")) == 1
        assert len(await store.load("o-2")) == 1
