"""Tests for ProjectionAsync."""

import time

import pytest

from da2 import Event, StoredEvent
from da2.projection_async import ProjectionAsync


class ItemAdded(Event):
    def __init__(self, item_id: str, qty: int):
        self.item_id = item_id
        self.qty = qty


class ItemRemoved(Event):
    def __init__(self, item_id: str):
        self.item_id = item_id


class InventoryView(ProjectionAsync):
    def __init__(self):
        super().__init__()
        self.stock: dict[str, int] = {}

    def _on_ItemAdded(self, event: ItemAdded):
        self.stock[event.item_id] = self.stock.get(event.item_id, 0) + event.qty

    def _on_ItemRemoved(self, event: ItemRemoved):
        self.stock.pop(event.item_id, None)


class AsyncInventoryView(ProjectionAsync):
    """Projection with async handler."""
    def __init__(self):
        super().__init__()
        self.log: list[str] = []

    async def _on_ItemAdded(self, event: ItemAdded):
        self.log.append(f"added:{event.item_id}")

    def _on_ItemRemoved(self, event: ItemRemoved):
        self.log.append(f"removed:{event.item_id}")


REGISTRY = {
    "ItemAdded": ItemAdded,
    "ItemRemoved": ItemRemoved,
}


def _se(version, etype, data):
    return StoredEvent("agg", etype, data, version, time.time())


@pytest.mark.asyncio
async def test_async_projection_apply():
    proj = InventoryView()
    await proj.apply(ItemAdded("a", 5))
    assert proj.stock == {"a": 5}


@pytest.mark.asyncio
async def test_async_projection_replay():
    events = [
        _se(1, "ItemAdded", {"item_id": "a", "qty": 10}),
        _se(2, "ItemAdded", {"item_id": "b", "qty": 3}),
        _se(3, "ItemRemoved", {"item_id": "a"}),
    ]
    proj = InventoryView()
    await proj.replay(events, REGISTRY)
    assert proj.stock == {"b": 3}
    assert proj.last_version == 3


@pytest.mark.asyncio
async def test_async_projection_catch_up():
    initial = [_se(1, "ItemAdded", {"item_id": "a", "qty": 10})]
    proj = InventoryView()
    await proj.replay(initial, REGISTRY)

    all_events = [
        _se(1, "ItemAdded", {"item_id": "a", "qty": 10}),
        _se(2, "ItemAdded", {"item_id": "b", "qty": 5}),
    ]
    await proj.catch_up(all_events, REGISTRY)
    assert proj.stock == {"a": 10, "b": 5}
    assert proj.last_version == 2


@pytest.mark.asyncio
async def test_async_handler_called():
    proj = AsyncInventoryView()
    await proj.apply(ItemAdded("x", 1))
    await proj.apply(ItemRemoved("y"))
    assert proj.log == ["added:x", "removed:y"]
