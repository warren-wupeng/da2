from __future__ import annotations

import logging
import traceback
from collections import defaultdict
from typing import Any, Type, Callable, Union, Generic, TypeVar, Coroutine, Awaitable

from .event import Event
from .command import Command
from .uow_async import UnitOfWorkAsync

logger = logging.getLogger(__name__)

Message = Union[Command, Event]
T = TypeVar("T", bound=UnitOfWorkAsync)

MiddlewareAsync = Callable[[Message, Callable[[Message], Awaitable[Any]]], Awaitable[Any]]


class MessageBusAsync(Generic[T]):
    """Async command/event dispatcher with lifecycle hooks and middleware.

    Like MessageBus, but async. Also supports lifecycle hooks for
    observability (logging, metrics, tracing) and an optional middleware
    pipeline that wraps the entire ``handle()`` call::

        async def logging_mw(message, next):
            print(f">> {type(message).__name__}")
            result = await next(message)
            print(f"<< {type(message).__name__}")
            return result

        bus = MessageBusAsync(uow=uow, ..., middleware=[logging_mw])

    Example::

        from da2 import Command, MessageBusAsync, UnitOfWorkAsync

        class Ping(Command):
            def __init__(self, value: str):
                self.value = value

        class FakeUoW(UnitOfWorkAsync):
            async def _enter(self): pass
            async def _commit(self): pass
            async def rollback(self): pass
            async def _exit(self, *a): pass

        async def handle_ping(cmd: Ping) -> str:
            return f"pong:{cmd.value}"

        uow = FakeUoW()
        uow.seen = {}
        bus = MessageBusAsync(
            uow=uow,
            event_handlers={},
            command_handlers={Ping: handle_ping},
        )
        result = await bus.handle(Ping(value="hi"))  # "pong:hi"

    Lifecycle hooks::

        @bus.on_bus_event("handle_success")
        def log_ok(event_type, message, handler_name, reason):
            print(f"OK: {handler_name}")
    """

    def __init__(
        self,
        uow: T,
        event_handlers: dict[Type[Event], list[Callable[..., Coroutine[Any, Any, Any]]]],
        command_handlers: dict[Type[Command], Callable[..., Coroutine[Any, Any, Any]]],
        middleware: list[MiddlewareAsync] | None = None,
    ) -> None:
        self.queue: list[Message] = []
        self._uow = uow
        self._event_handlers = event_handlers
        self._command_handlers = command_handlers
        self._middleware: list[MiddlewareAsync] = middleware or []
        self._before_handle_event_hooks: list[Callable[[Command, Event], None]] = []
        self._bus_event_listeners: dict[str, list[Callable[..., Any]]] = defaultdict(list)
        self._current_cmd: Message | None = None
        self._cmd_result: Any = None

    def on_before_handle_event(self, hook: Callable[[Command, Event], None]) -> None:
        """Register a hook called before each event handler runs."""
        self._before_handle_event_hooks.append(hook)

    def on_bus_event(self, event_type: str) -> Callable:
        """Decorator to listen for bus lifecycle events.

        event_type is one of: ``message_received``, ``handle_started``,
        ``handle_success``, ``handle_failed``.
        """
        def wrapper(hook: Callable[..., Any]) -> Callable[..., Any]:
            self._bus_event_listeners[event_type].append(hook)
            return hook
        return wrapper

    @property
    def uow(self) -> T:
        """The Unit of Work bound to this bus."""
        return self._uow

    async def handle(self, message: Message) -> Any:
        """Dispatch a command or event and process the entire event chain.

        If middleware is configured, the message passes through the
        middleware pipeline before reaching the core dispatch logic.
        """
        if not self._middleware:
            return await self._inner_handle(message)

        def build_chain(
            mws: list[MiddlewareAsync],
            final: Callable[[Message], Awaitable[Any]],
        ) -> Callable[[Message], Awaitable[Any]]:
            handler = final
            for mw in reversed(mws):
                prev = handler
                handler = (
                    lambda _mw, _prev: lambda msg: _mw(msg, _prev)
                )(mw, prev)
            return handler

        chain = build_chain(self._middleware, self._inner_handle)
        return await chain(message)

    async def _inner_handle(self, message: Message) -> Any:
        """Core dispatch logic (without middleware)."""
        self._current_cmd = message
        self._cmd_result = None
        self.queue = [message]

        while self.queue:
            message = self.queue.pop(0)
            if isinstance(message, Event):
                await self._handle_event(message)
            elif isinstance(message, Command):
                await self._handle_command(message)
            else:
                raise TypeError(
                    f"{message!r} is not an Event or Command. "
                    f"Make sure your message class inherits from da2.Command or da2.Event."
                )
        return self._cmd_result

    async def _handle_event(self, event: Event) -> None:
        for hook in self._before_handle_event_hooks:
            hook(self._current_cmd, event)

        self._emit("message_received", event)

        for handler in self._event_handlers.get(type(event), []):
            handler_name = getattr(handler, "__name__", repr(handler))
            try:
                self._emit("handle_started", event, handler_name)
                await handler(event)
                self._emit("handle_success", event, handler_name)
                self.queue.extend(self.uow.collect_new_events())
            except Exception as e:
                reason = f"{e}: {traceback.format_exc()}"
                self._emit("handle_failed", event, handler_name, reason)
                logger.exception("Exception handling event %s", event)
                continue

    async def _handle_command(self, command: Command) -> None:
        self._emit("message_received", command)
        handler = self._command_handlers.get(type(command))
        if handler is None:
            raise KeyError(
                f"No handler registered for {type(command).__name__}. "
                f"Register it via command_handlers={{{type(command).__name__}: your_handler}}"
            )
        handler_name = getattr(handler, "__name__", repr(handler))
        try:
            self._emit("handle_started", command, handler_name)
            self._cmd_result = await handler(command)
            self._emit("handle_success", command, handler_name)
            self.queue.extend(self.uow.collect_new_events())
        except Exception as e:
            reason = f"{e}: {traceback.format_exc()}"
            self._emit("handle_failed", command, handler_name, reason)
            raise

    def _emit(
        self,
        event_type: str,
        message: Message,
        handler_name: str = "",
        reason: str = "",
    ) -> None:
        for listener in self._bus_event_listeners.get(event_type, []):
            try:
                listener(event_type, message, handler_name, reason)
            except Exception as e:
                logger.warning("Bus event listener failed: %s", e)
