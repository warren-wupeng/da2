"""Tests for Projection (sync)."""

import time

import pytest

from da2 import Event, StoredEvent
from da2.projection import Projection


# ── Test events ───────────────────────────────────────────────────

class OrderPlaced(Event):
    def __init__(self, order_id: str, amount: float):
        self.order_id = order_id
        self.amount = amount


class OrderCancelled(Event):
    def __init__(self, order_id: str):
        self.order_id = order_id


class OrderShipped(Event):
    def __init__(self, order_id: str):
        self.order_id = order_id


# ── Test projection ──────────────────────────────────────────────

class OrderTotals(Projection):
    def __init__(self):
        super().__init__()
        self.totals: dict[str, float] = {}
        self.count: int = 0

    def _on_OrderPlaced(self, event: OrderPlaced):
        self.totals[event.order_id] = event.amount
        self.count += 1

    def _on_OrderCancelled(self, event: OrderCancelled):
        self.totals.pop(event.order_id, None)


EVENT_REGISTRY = {
    "OrderPlaced": OrderPlaced,
    "OrderCancelled": OrderCancelled,
    "OrderShipped": OrderShipped,
}


# ── Tests ────────────────────────────────────────────────────────

class TestProjectionApply:
    def test_apply_routes_to_handler(self):
        proj = OrderTotals()
        proj.apply(OrderPlaced("o1", 100.0))
        assert proj.totals == {"o1": 100.0}
        assert proj.count == 1

    def test_apply_ignores_unknown_event(self):
        proj = OrderTotals()
        proj.apply(OrderShipped("o1"))  # no _on_OrderShipped handler
        assert proj.totals == {}
        assert proj.count == 0

    def test_apply_multiple_events(self):
        proj = OrderTotals()
        proj.apply(OrderPlaced("o1", 100.0))
        proj.apply(OrderPlaced("o2", 200.0))
        proj.apply(OrderCancelled("o1"))
        assert proj.totals == {"o2": 200.0}
        assert proj.count == 2


class TestProjectionReplay:
    def _stored(self, version, event_type, data):
        return StoredEvent(
            aggregate_id="agg1",
            event_type=event_type,
            data=data,
            version=version,
            timestamp=time.time(),
        )

    def test_replay_builds_read_model(self):
        events = [
            self._stored(1, "OrderPlaced", {"order_id": "o1", "amount": 50.0}),
            self._stored(2, "OrderPlaced", {"order_id": "o2", "amount": 75.0}),
            self._stored(3, "OrderCancelled", {"order_id": "o1"}),
        ]
        proj = OrderTotals()
        proj.replay(events, EVENT_REGISTRY)
        assert proj.totals == {"o2": 75.0}
        assert proj.last_version == 3

    def test_replay_without_registry_tracks_version_only(self):
        events = [
            self._stored(1, "OrderPlaced", {"order_id": "o1", "amount": 50.0}),
            self._stored(2, "OrderPlaced", {"order_id": "o2", "amount": 75.0}),
        ]
        proj = OrderTotals()
        proj.replay(events, event_registry=None)
        assert proj.totals == {}  # no events applied
        assert proj.last_version == 2

    def test_replay_skips_unknown_event_types(self):
        events = [
            self._stored(1, "OrderPlaced", {"order_id": "o1", "amount": 50.0}),
            self._stored(2, "SomethingElse", {"foo": "bar"}),
        ]
        proj = OrderTotals()
        proj.replay(events, EVENT_REGISTRY)
        assert proj.totals == {"o1": 50.0}
        assert proj.last_version == 2


class TestProjectionCatchUp:
    def _stored(self, version, event_type, data):
        return StoredEvent(
            aggregate_id="agg1",
            event_type=event_type,
            data=data,
            version=version,
            timestamp=time.time(),
        )

    def test_catch_up_skips_already_processed(self):
        initial = [
            self._stored(1, "OrderPlaced", {"order_id": "o1", "amount": 50.0}),
        ]
        proj = OrderTotals()
        proj.replay(initial, EVENT_REGISTRY)
        assert proj.last_version == 1

        # Now catch up with all events including the old one
        all_events = [
            self._stored(1, "OrderPlaced", {"order_id": "o1", "amount": 50.0}),
            self._stored(2, "OrderPlaced", {"order_id": "o2", "amount": 75.0}),
        ]
        proj.catch_up(all_events, EVENT_REGISTRY)
        assert proj.totals == {"o1": 50.0, "o2": 75.0}
        assert proj.last_version == 2
        assert proj.count == 2  # only 2 because o1 was counted once in replay, once in catch_up

    def test_catch_up_empty_does_nothing(self):
        proj = OrderTotals()
        proj.catch_up([], EVENT_REGISTRY)
        assert proj.last_version == 0
