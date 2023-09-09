import inspect
from typing import Type, Callable

from .command import Command
from .event import Event
from .messagebus import MessageBus
from .uow import UnitOfWork


class Bootstrap:

    def __init__(
            self,
            uow: UnitOfWork,
            event_handlers: dict[Type[Event], list[Callable]],
            command_handlers: dict[Type[Command], Callable],
            dependencies: dict[str, object] = None,
    ):
        self._dependencies = {"uow": uow} | (dependencies or dict())
        self._event_handlers = event_handlers
        self._command_handlers = command_handlers
        self._uow = uow

    def start(self):

        injected_event_handlers = {
            event_type: [
                self._inject_dependencies(handler, self._dependencies)
                for handler in event_handlers
            ]
            for event_type, event_handlers in self._event_handlers.items()
        }
        injected_command_handlers = {
            command_type: self._inject_dependencies(handler, self._dependencies)
            for command_type, handler in self._command_handlers.items()
        }
        return MessageBus(
            uow=self._uow,
            event_handlers=injected_event_handlers,
            command_handlers=injected_command_handlers
        )

    @staticmethod
    def _inject_dependencies(handler, dependencies):
        params = inspect.signature(handler).parameters
        deps = {
            name: dependency
            for name, dependency in dependencies.items()
            if name in params
        }
        func = lambda message: handler(message, **deps)
        # func.__name__ = f"{handler.__name__}"
        return func