import logging
from typing import Type, Callable, Union, Generic, TypeVar
from .event import Event
from .command import Command

from .uow import UnitOfWork

logger = logging.getLogger(__name__)
Message = Union[Command, Event]

T = TypeVar('T', bound=UnitOfWork)


class MessageBus(Generic[T]):

    def __init__(
            self,
            uow: UnitOfWork,
            event_handlers: dict[Type[Event], list[Callable]],
            command_handlers: dict[Type[Command], Callable],
    ):
        self.queue = None
        self._uow = uow
        self._event_handlers = event_handlers
        self._command_handlers = command_handlers

    @property
    def uow(self) -> T:
        return self._uow

    def handle(self, message: Message):
        self.queue = [message]
        while self.queue:
            message = self.queue.pop(0)
            if isinstance(message, Event):
                self.handle_event(message)
            elif isinstance(message, Command):
                self.handle_command(message)
            else:
                raise Exception(f"{message} was not an Event or Command")

    def handle_event(self, event: Event):
        for handler in self._event_handlers[type(event)]:
            try:
                logger.debug("handling event %s with handler %s", event, handler)
                handler(event)
                self.queue.extend(self.uow.collect_new_events())
            except Exception:
                logger.exception("Exception handling event %s", event)
                continue

    def handle_command(self, command: Command):
        logger.debug("handling command %s", command)
        try:
            handler = self._command_handlers[type(command)]
            handler(command)
            self.queue.extend(self.uow.collect_new_events())
        except Exception:
            logger.exception("Exception handling command %s", command)
            raise
