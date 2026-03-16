"""Tests for PolicyAsync."""

import pytest

from da2 import Command, Event
from da2.policy_async import PolicyAsync


# ── Fixtures ──────────────────────────────────────────────────────

class SendEmail(Command):
    def __init__(self, to: str):
        self.to = to


class LogAction(Command):
    def __init__(self, action: str):
        self.action = action


class UserRegistered(Event):
    def __init__(self, email: str):
        self.email = email


class OrderPlaced(Event):
    def __init__(self, order_id: str):
        self.order_id = order_id


class MixedPolicy(PolicyAsync):
    """Has both async and sync handlers."""

    async def _on_UserRegistered(self, event: UserRegistered):
        self._dispatch(SendEmail(to=event.email))

    def _on_OrderPlaced(self, event: OrderPlaced):
        self._dispatch(LogAction(action=f"order:{event.order_id}"))


# ── Tests ─────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_async_handle():
    policy = MixedPolicy()
    cmds = await policy.handle(UserRegistered(email="alice@test.com"))
    assert len(cmds) == 1
    assert isinstance(cmds[0], SendEmail)
    assert cmds[0].to == "alice@test.com"


@pytest.mark.asyncio
async def test_sync_handler_in_async_policy():
    policy = MixedPolicy()
    cmds = await policy.handle(OrderPlaced(order_id="o-1"))
    assert len(cmds) == 1
    assert isinstance(cmds[0], LogAction)
    assert cmds[0].action == "order:o-1"


@pytest.mark.asyncio
async def test_unknown_event():
    class Random(Event):
        pass

    policy = MixedPolicy()
    cmds = await policy.handle(Random())
    assert cmds == []


@pytest.mark.asyncio
async def test_clears_between_calls():
    policy = MixedPolicy()
    cmds1 = await policy.handle(UserRegistered(email="a@b.com"))
    cmds2 = await policy.handle(UserRegistered(email="c@d.com"))
    assert len(cmds1) == 1
    assert len(cmds2) == 1
    assert cmds1[0].to == "a@b.com"
    assert cmds2[0].to == "c@d.com"
