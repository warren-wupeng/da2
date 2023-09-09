import traceback
from collections import defaultdict
from typing import Type, Callable, Union, Generic, TypeVar, Coroutine

from pydantic import BaseModel

from .event import Event
from .command import Command

from .uow import UnitOfWork

Message = Union[Command, Event]

T = TypeVar('T', bound=UnitOfWork)


class MessageBusEvent(BaseModel):
    message: Message

    class Config:
        arbitrary_types_allowed = True


class MessageReceived(MessageBusEvent):
    pass


class MessageHandleStarted(MessageBusEvent):
    handler: str

    def __str__(self) -> str:
        return f"start {self.handler} for {repr(self.message)}"


class MessageHandleSuccess(MessageBusEvent):
    handler: str

    def __str__(self) -> str:
        return f"successfully {self.handler} for {repr(self.message)}"


class MessageHandleFailed(MessageBusEvent):
    handler: str
    reason: str

    def __str__(self) -> str:
        return f"failed to {self.handler} for {repr(self.message)} reason: {self.reason}"


class MessageBusAsync(Generic[T]):

    def __init__(
            self,
            uow: T,
            event_handlers:
            dict[Type[Event], list[Callable[[Event], Coroutine]]],
            command_handlers:
            dict[Type[Command], Callable[[Command], Coroutine]]
    ):
        self.queue = None
        self._uow = uow
        self._event_handlers = event_handlers
        self._command_handlers = command_handlers
        self._before_handle_event_hooks = []
        self._bus_event_listeners = defaultdict(list)
        self._current_cmd = None
        self._cmd_result = None

    def beforeHandleEvent(self, hook: Callable[[Command, Event], None]):
        self._before_handle_event_hooks.append(hook)

    def registerBusEventListener(self, busEvent: MessageBusEvent):
        def wrapper(hook: Callable[[MessageBusEvent], None]):
            self._bus_event_listeners[busEvent].append(hook)
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
                await self.handle_event(message)
            elif isinstance(message, Command):
                await self.handle_command(message)
            else:
                raise self.NotCommandOrEvent(
                    f"{message} was not an Event or Command"
                )
        return self._cmd_result

    async def handle_event(self, event: Event):
        for hook in self._before_handle_event_hooks:
            hook(self._current_cmd, event)

        busEvent = MessageReceived(message=event)
        self._execute_message_bus_event_hooks(busEvent)

        for handler in self._event_handlers[type(event)]:
            try:
                busEvent = MessageHandleStarted(
                    message=event, handler=handler.__name__
                )
                self._execute_message_bus_event_hooks(busEvent)

                await handler(event)

                busEvent = MessageHandleSuccess(
                    message=event, handler=handler.__name__
                )
                self._execute_message_bus_event_hooks(busEvent)

                self.queue.extend(self.uow.collect_new_events())
            except Exception as e:
                busEvent = MessageHandleFailed(
                    message=event, handler=handler.__name__,
                    reason=f"{e}: {traceback.format_exc()}"
                )
                self._execute_message_bus_event_hooks(busEvent)
                print(f"Exception handling {self._formatMessage(event)}: {busEvent.reason}")
                continue

    @staticmethod
    def _formatMessage(message: Message):
        return f"{message.__class__.__name__}({message})"

    async def handle_command(self, command: Command):
        busEvent = MessageReceived(message=command)
        self._execute_message_bus_event_hooks(busEvent)
        handler = self._command_handlers[type(command)]
        try:
            busEvent = MessageHandleStarted(
                message=command, handler=handler.__name__
            )
            self._execute_message_bus_event_hooks(busEvent)

            self._cmd_result = await handler(command)

            busEvent = MessageHandleSuccess(
                message=command, handler=handler.__name__
            )
            self._execute_message_bus_event_hooks(busEvent)

            self.queue.extend(self.uow.collect_new_events())
        except Exception as e:

            busEvent = MessageHandleFailed(
                message=command, handler=handler.__name__,
                reason=f"{e}: {traceback.format_exc()}"
            )
            self._execute_message_bus_event_hooks(busEvent)
            raise

    def _execute_message_bus_event_hooks(
            self, busEvent: MessageBusEvent
    ):
        listeners = self._bus_event_listeners[type(busEvent)]
        # print(f"{listeners=}")
        if listeners:
            for hook in listeners:
                try:
                    hook(busEvent)
                except Exception as e:
                    print(f"execute {busEvent.__class__.__name__} hook failed {e}")

    def getResult(self, cmd: Command):
        assert self._current_cmd == cmd
        return self._cmd_result

    class EventHandleFailed(Exception):
        pass

    class NotCommandOrEvent(Exception):
        pass


