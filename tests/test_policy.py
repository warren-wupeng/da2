"""Tests for Policy."""

from da2 import Command, Event
from da2.policy import Policy


# ── Fixtures ──────────────────────────────────────────────────────

class SendWelcomeEmail(Command):
    def __init__(self, email: str):
        self.email = email


class CreateAuditLog(Command):
    def __init__(self, action: str):
        self.action = action


class UserRegistered(Event):
    def __init__(self, email: str):
        self.email = email


class OrderPlaced(Event):
    def __init__(self, order_id: str):
        self.order_id = order_id


class UnknownEvent(Event):
    pass


class WelcomePolicy(Policy):
    def _on_UserRegistered(self, event: UserRegistered):
        self._dispatch(SendWelcomeEmail(email=event.email))


class AuditPolicy(Policy):
    """Dispatches multiple commands from a single event."""
    def _on_UserRegistered(self, event: UserRegistered):
        self._dispatch(SendWelcomeEmail(email=event.email))
        self._dispatch(CreateAuditLog(action=f"registered:{event.email}"))

    def _on_OrderPlaced(self, event: OrderPlaced):
        self._dispatch(CreateAuditLog(action=f"order:{event.order_id}"))


# ── Tests ─────────────────────────────────────────────────────────

class TestPolicyHandle:

    def test_handle_routes_and_returns_commands(self):
        policy = WelcomePolicy()
        cmds = policy.handle(UserRegistered(email="alice@test.com"))
        assert len(cmds) == 1
        assert isinstance(cmds[0], SendWelcomeEmail)
        assert cmds[0].email == "alice@test.com"

    def test_handle_unknown_event_returns_empty(self):
        policy = WelcomePolicy()
        cmds = policy.handle(UnknownEvent())
        assert cmds == []

    def test_handle_clears_pending(self):
        policy = WelcomePolicy()
        policy.handle(UserRegistered(email="a@b.com"))
        cmds = policy.handle(UnknownEvent())
        assert cmds == []

    def test_multiple_commands_from_single_event(self):
        policy = AuditPolicy()
        cmds = policy.handle(UserRegistered(email="bob@test.com"))
        assert len(cmds) == 2
        assert isinstance(cmds[0], SendWelcomeEmail)
        assert isinstance(cmds[1], CreateAuditLog)

    def test_different_events_different_handlers(self):
        policy = AuditPolicy()
        cmds1 = policy.handle(UserRegistered(email="a@b.com"))
        cmds2 = policy.handle(OrderPlaced(order_id="o-1"))
        assert len(cmds1) == 2
        assert len(cmds2) == 1
        assert isinstance(cmds2[0], CreateAuditLog)
        assert cmds2[0].action == "order:o-1"

    def test_stateless_no_side_effects_between_calls(self):
        policy = WelcomePolicy()
        cmds1 = policy.handle(UserRegistered(email="a@b.com"))
        cmds2 = policy.handle(UserRegistered(email="c@d.com"))
        assert len(cmds1) == 1
        assert len(cmds2) == 1
        assert cmds1[0].email == "a@b.com"
        assert cmds2[0].email == "c@d.com"
