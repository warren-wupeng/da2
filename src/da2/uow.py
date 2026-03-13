import abc

from .entity import Entity


class UnitOfWork(abc.ABC):

    seen: dict

    def __enter__(self):
        self.seen = {}
        self._enter()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type:
            self.rollback()

    def commit(self):
        self._commit()

    def add_seen(self, entity: Entity):
        self.seen[entity.identity] = entity

    def collect_new_events(self):
        for entity in self.seen.values():
            while entity.events:
                yield entity.events.pop(0)

    @abc.abstractmethod
    def _commit(self):
        raise NotImplementedError

    @abc.abstractmethod
    def rollback(self):
        raise NotImplementedError

    @abc.abstractmethod
    def _enter(self):
        raise NotImplementedError
