import inspect
from typing import Type, Callable

from .command import Command
from .event import Event
from .message_bus_async import MessageBusAsync
from .uow_async import UnitOfWorkAsync


class BootstrapAsync:

    def __init__(
        self,
        uow: UnitOfWorkAsync,
        event_handlers: dict[Type[Event], list[Callable]],
        command_handlers: dict[Type[Command], Callable],
        dependencies: dict[str, object] | None = None,
    ):
        self._dependencies = {"uow": uow} | (dependencies or {})
        self._event_handlers = event_handlers
        self._command_handlers = command_handlers
        self._uow = uow

    def create_message_bus(self) -> MessageBusAsync:
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
        return MessageBusAsync(
            uow=self._uow,
            event_handlers=injected_event_handlers,
            command_handlers=injected_command_handlers,
        )

    @staticmethod
    def _inject_dependencies(handler, dependencies):
        params = inspect.signature(handler).parameters
        deps = {
            name: dep
            for name, dep in dependencies.items()
            if name in params
        }

        def injected(message):
            return handler(message, **deps)

        injected.__name__ = getattr(handler, "__name__", repr(handler))
        return injected
