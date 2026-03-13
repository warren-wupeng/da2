from __future__ import annotations

import inspect
from typing import Any, Type, Callable

from .command import Command
from .event import Event
from .message_bus import MessageBus
from .uow import UnitOfWork


class Bootstrap:
    """Wire command/event handlers with dependency injection, produce a MessageBus.

    Bootstrap inspects handler function signatures and auto-injects
    matching dependencies (by parameter name). ``uow`` is always available.

    Example::

        from da2 import Command, Event, UnitOfWork, Bootstrap

        class CreateUser(Command):
            def __init__(self, name: str):
                self.name = name

        class FakeUoW(UnitOfWork):
            def _enter(self): pass
            def _commit(self): pass
            def rollback(self): pass

        def handle_create_user(cmd: CreateUser, uow: UnitOfWork):
            print(f"Creating {cmd.name}")

        bootstrap = Bootstrap(
            uow=FakeUoW(),
            command_handlers={CreateUser: handle_create_user},
            event_handlers={},
        )
        bus = bootstrap.create_message_bus()
        bus.uow.seen = {}
        bus.handle(CreateUser(name="Alice"))
    """

    def __init__(
        self,
        uow: UnitOfWork,
        event_handlers: dict[Type[Event], list[Callable[..., Any]]],
        command_handlers: dict[Type[Command], Callable[..., Any]],
        dependencies: dict[str, Any] | None = None,
    ) -> None:
        self._dependencies: dict[str, Any] = {"uow": uow} | (dependencies or {})
        self._event_handlers = event_handlers
        self._command_handlers = command_handlers
        self._uow = uow

    def create_message_bus(self) -> MessageBus:
        """Build a MessageBus with dependency-injected handlers."""
        injected_event_handlers = {
            event_type: [
                self._inject_dependencies(handler, self._dependencies)
                for handler in handlers
            ]
            for event_type, handlers in self._event_handlers.items()
        }
        injected_command_handlers = {
            command_type: self._inject_dependencies(handler, self._dependencies)
            for command_type, handler in self._command_handlers.items()
        }
        return MessageBus(
            uow=self._uow,
            event_handlers=injected_event_handlers,
            command_handlers=injected_command_handlers,
        )

    @staticmethod
    def _inject_dependencies(
        handler: Callable[..., Any],
        dependencies: dict[str, Any],
    ) -> Callable[..., Any]:
        params = inspect.signature(handler).parameters
        deps = {
            name: dep
            for name, dep in dependencies.items()
            if name in params
        }

        def injected(message: Any) -> Any:
            return handler(message, **deps)

        injected.__name__ = getattr(handler, "__name__", repr(handler))
        return injected
