from __future__ import annotations

import logging
from typing import Any, Type, Callable, Union, Generic, TypeVar

from .event import Event
from .command import Command
from .uow import UnitOfWork

logger = logging.getLogger(__name__)

Message = Union[Command, Event]
T = TypeVar("T", bound=UnitOfWork)

Middleware = Callable[[Message, Callable[[Message], Any]], Any]


class MessageBus(Generic[T]):
    """Synchronous command/event dispatcher with event chain propagation.

    Dispatches a Command to its single handler, or an Event to all its
    handlers. After each handler, new events raised by tracked entities
    are collected and added to the processing queue.

    Supports an optional middleware pipeline that wraps the entire
    ``handle()`` call. Each middleware receives ``(message, next)``
    where ``next`` calls the remaining pipeline::

        def logging_mw(message, next):
            print(f">> {type(message).__name__}")
            result = next(message)
            print(f"<< {type(message).__name__}")
            return result

        bus = MessageBus(uow=uow, ..., middleware=[logging_mw])

    Example::

        from da2 import Command, Event, MessageBus, UnitOfWork

        class Ping(Command):
            def __init__(self, value: str):
                self.value = value

        class FakeUoW(UnitOfWork):
            def _enter(self): pass
            def _commit(self): pass
            def rollback(self): pass

        def handle_ping(cmd: Ping):
            print(f"pong: {cmd.value}")

        uow = FakeUoW()
        uow.seen = {}
        bus = MessageBus(
            uow=uow,
            event_handlers={},
            command_handlers={Ping: handle_ping},
        )
        bus.handle(Ping(value="hello"))  # prints "pong: hello"
    """

    def __init__(
        self,
        uow: T,
        event_handlers: dict[Type[Event], list[Callable[..., Any]]],
        command_handlers: dict[Type[Command], Callable[..., Any]],
        middleware: list[Middleware] | None = None,
    ) -> None:
        self.queue: list[Message] = []
        self._uow = uow
        self._event_handlers = event_handlers
        self._command_handlers = command_handlers
        self._middleware: list[Middleware] = middleware or []

    @property
    def uow(self) -> T:
        """The Unit of Work bound to this bus."""
        return self._uow

    def handle(self, message: Message) -> Any:
        """Dispatch a command or event and process the entire event chain.

        Returns the command handler's return value (if the initial message
        is a Command), or ``None`` for Events.
        """
        if not self._middleware:
            return self._inner_handle(message)

        def build_chain(
            mws: list[Middleware], final: Callable[[Message], Any]
        ) -> Callable[[Message], Any]:
            handler = final
            for mw in reversed(mws):
                prev = handler
                handler = (lambda _mw, _prev: lambda msg: _mw(msg, _prev))(mw, prev)
            return handler

        chain = build_chain(self._middleware, self._inner_handle)
        return chain(message)

    def _inner_handle(self, message: Message) -> Any:
        """Core dispatch logic (without middleware)."""
        self.queue = [message]
        result = None
        while self.queue:
            message = self.queue.pop(0)
            if isinstance(message, Event):
                self._handle_event(message)
            elif isinstance(message, Command):
                result = self._handle_command(message)
            else:
                raise TypeError(
                    f"{message!r} is not an Event or Command. "
                    f"Make sure your message class inherits from da2.Command or da2.Event."
                )
        return result

    def _handle_event(self, event: Event) -> None:
        for handler in self._event_handlers.get(type(event), []):
            try:
                logger.debug("handling event %s with %s", event, handler)
                handler(event)
                self.queue.extend(self.uow.collect_new_events())
            except Exception:
                logger.exception("Exception handling event %s", event)
                continue

    def _handle_command(self, command: Command) -> Any:
        logger.debug("handling command %s", command)
        handler = self._command_handlers.get(type(command))
        if handler is None:
            raise KeyError(
                f"No handler registered for {type(command).__name__}. "
                f"Register it via command_handlers={{{type(command).__name__}: your_handler}}"
            )
        try:
            result = handler(command)
            self.queue.extend(self.uow.collect_new_events())
            return result
        except Exception:
            logger.exception("Exception handling command %s", command)
            raise
