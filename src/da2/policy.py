"""Policy -- stateless event-to-command reactor.

A Policy reacts to domain events by issuing commands, without maintaining
internal state. Unlike a ProcessManager (which tracks multi-step workflow
state), a Policy is a pure function: event in, commands out.

Convention: define ``_on_<EventClassName>`` methods that call ``_dispatch(cmd)``.

Example::

    from da2 import Command, Event
    from da2.policy import Policy

    class SendWelcomeEmail(Command):
        def __init__(self, email: str): self.email = email

    class UserRegistered(Event):
        def __init__(self, email: str): self.email = email

    class WelcomePolicy(Policy):
        def _on_UserRegistered(self, event: UserRegistered):
            self._dispatch(SendWelcomeEmail(email=event.email))

    policy = WelcomePolicy()
    cmds = policy.handle(UserRegistered(email="alice@example.com"))
    # cmds == [SendWelcomeEmail(email="alice@example.com")]
"""

from __future__ import annotations

from .command import Command
from .event import Event


class Policy:
    """Stateless event-to-command reactor.

    Subclasses define ``_on_<EventClassName>`` methods that call
    ``self._dispatch(cmd)`` to queue commands.

    ``handle(event)`` routes the event, returns queued commands, and clears
    the internal queue.
    """

    def __init__(self) -> None:
        self._pending_commands: list[Command] = []

    def handle(self, event: Event) -> list[Command]:
        """Route *event* to the matching handler and return queued commands."""
        handler = getattr(self, f"_on_{type(event).__name__}", None)
        if handler is not None:
            handler(event)
        cmds = list(self._pending_commands)
        self._pending_commands.clear()
        return cmds

    def _dispatch(self, command: Command) -> None:
        """Queue a command to be issued after the current event is processed."""
        self._pending_commands.append(command)
