import logging
import traceback
from collections import defaultdict
from typing import Type, Callable, Union, Generic, TypeVar, Coroutine

from .event import Event
from .command import Command
from .uow_async import UnitOfWorkAsync

logger = logging.getLogger(__name__)

Message = Union[Command, Event]
T = TypeVar("T", bound=UnitOfWorkAsync)


class MessageBusAsync(Generic[T]):

    def __init__(
        self,
        uow: T,
        event_handlers: dict[Type[Event], list[Callable[[Event], Coroutine]]],
        command_handlers: dict[Type[Command], Callable[[Command], Coroutine]],
    ):
        self.queue: list[Message] | None = None
        self._uow = uow
        self._event_handlers = event_handlers
        self._command_handlers = command_handlers
        self._before_handle_event_hooks: list[Callable] = []
        self._bus_event_listeners: dict[str, list[Callable]] = defaultdict(list)
        self._current_cmd: Message | None = None
        self._cmd_result = None

    def on_before_handle_event(self, hook: Callable[[Command, Event], None]):
        self._before_handle_event_hooks.append(hook)

    def on_bus_event(self, event_type: str):
        def wrapper(hook: Callable):
            self._bus_event_listeners[event_type].append(hook)
            return hook
        return wrapper

    @property
    def uow(self) -> T:
        return self._uow

    async def handle(self, message: Message):
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
                raise TypeError(f"{message} is not an Event or Command")
        return self._cmd_result

    async def _handle_event(self, event: Event):
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

    async def _handle_command(self, command: Command):
        self._emit("message_received", command)
        handler = self._command_handlers[type(command)]
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

    def _emit(self, event_type: str, message: Message,
              handler_name: str = "", reason: str = ""):
        for listener in self._bus_event_listeners.get(event_type, []):
            try:
                listener(event_type, message, handler_name, reason)
            except Exception as e:
                logger.warning("Bus event listener failed: %s", e)
