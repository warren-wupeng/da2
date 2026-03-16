"""Example 07 -- Policy (Stateless Event Reactor).

Policies react to events by issuing commands, without maintaining state.
Use for simple "when X happens, do Y" reactive rules.

Contrast with ProcessManager (example 06) which maintains state
across multiple events in a workflow.
"""

from __future__ import annotations

from da2 import Command, Event
from da2.policy import Policy


# ── Commands ──────────────────────────────────────────────────────

class SendWelcomeEmail(Command):
    def __init__(self, email: str):
        self.email = email


class CreateAuditLog(Command):
    def __init__(self, action: str):
        self.action = action


class NotifyAdmin(Command):
    def __init__(self, message: str):
        self.message = message


# ── Events ────────────────────────────────────────────────────────

class UserRegistered(Event):
    def __init__(self, email: str):
        self.email = email


class OrderPlaced(Event):
    def __init__(self, order_id: str, total: float):
        self.order_id = order_id
        self.total = total


# ── Policies ──────────────────────────────────────────────────────

class WelcomePolicy(Policy):
    """Send welcome email when a user registers."""

    def _on_UserRegistered(self, event: UserRegistered):
        self._dispatch(SendWelcomeEmail(email=event.email))


class AuditPolicy(Policy):
    """Log all significant events for compliance."""

    def _on_UserRegistered(self, event: UserRegistered):
        self._dispatch(CreateAuditLog(action=f"user_registered:{event.email}"))

    def _on_OrderPlaced(self, event: OrderPlaced):
        self._dispatch(CreateAuditLog(action=f"order_placed:{event.order_id}"))
        if event.total > 1000:
            self._dispatch(NotifyAdmin(message=f"High-value order {event.order_id}: ${event.total}"))


# ── Demo ──────────────────────────────────────────────────────────

def main():
    welcome = WelcomePolicy()
    audit = AuditPolicy()

    print("=== User Registration ===")
    event = UserRegistered(email="alice@example.com")

    for policy in [welcome, audit]:
        cmds = policy.handle(event)
        for cmd in cmds:
            print(f"  {type(cmd).__name__}: {vars(cmd)}")

    print("\n=== Normal Order ===")
    event = OrderPlaced(order_id="o-1", total=50.0)
    cmds = audit.handle(event)
    for cmd in cmds:
        print(f"  {type(cmd).__name__}: {vars(cmd)}")

    print("\n=== High-Value Order (triggers admin notification) ===")
    event = OrderPlaced(order_id="o-2", total=5000.0)
    cmds = audit.handle(event)
    for cmd in cmds:
        print(f"  {type(cmd).__name__}: {vars(cmd)}")

    print("\nDone.")


if __name__ == "__main__":
    main()
