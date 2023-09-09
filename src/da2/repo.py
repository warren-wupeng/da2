import abc
import json
from functools import partial
import functools
from abc import ABCMeta, abstractmethod
from collections.abc import MutableMapping
from typing import Iterator, TypeVar, Generic, Callable
from dataclasses import is_dataclass

from redis.client import Redis

from .observable import ObservableMixin
from .entity import Entity


def seen(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        self = args[0]
        result = func(*args, **kwargs)
        if result is not None:
            self._addSeen(result)
        return result
    return wrapper


def subscribeItemUpdate(func: Callable) -> Callable:
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        self, entityId, *_ = args
        result = func(*args, **kwargs)
        if isinstance(result, ObservableMixin):
            for key in result.__dict__:
                if not key.startswith('_'):
                    result.subscribe_key_update(
                        key, partial(self.updateItem, entityId, key)
                    )

        return result

    return wrapper


T = TypeVar('T', bound=Entity)


class Repository(Generic[T], MutableMapping, metaclass=ABCMeta):

    def __init__(self, addSeen: Callable[[Entity], None] = None):
        self._addSeen = addSeen

    @abstractmethod
    def __setitem__(self, k, v: T) -> T:
        pass

    @abstractmethod
    def __delitem__(self, k):
        pass

    @abstractmethod
    def __getitem__(self, k) -> T:
        pass

    @abstractmethod
    def __len__(self) -> int:
        pass

    @abstractmethod
    def __iter__(self) -> Iterator:
        pass

    @abstractmethod
    def getNewKey(self) -> int:
        pass

    def add(self, entity: T):
        if not entity.entityId:
            entity.entityId = self.getNewKey()
        self[entity.entityId] = entity
        return entity

    @abstractmethod
    def updateItem(self, key, itemKey, itemValue):
        pass

    @abstractmethod
    def find(self, conditions: list[Callable[[T], bool]]) -> list[T]:
        pass


class InMemoryRepository(Repository[T]):

    def __init__(self):
        super().__init__()
        self._dict = dict()

    def find(self, conditions: list[Callable[[T], bool]]) -> list[T]:
        table_out = [
            v for k, v in self._dict.items()
            if all(cond(v) for cond in conditions)
        ]
        return table_out

    def __setitem__(self, k, v: T) -> T:
        self._dict[k] = v
        return v

    def __delitem__(self, k):
        pass

    def __getitem__(self, k) -> T:
        return self._dict[k]

    def __len__(self) -> int:
        pass

    def __iter__(self) -> Iterator:
        pass

    def getNewKey(self) -> int:
        pass

    def updateItem(self, key, itemKey, itemValue):
        pass


class RedisRepository(Repository[T]):

    def find(self, conditions: list[Callable[[T], bool]]) -> list[T]:
        pass

    def __init__(
            self, rdb: Redis, domainClass: Entity,
            addSeen: Callable[[Entity], None] = None
    ):
        super(RedisRepository, self).__init__(addSeen=addSeen)
        self._rdb = rdb
        self._domainClass = domainClass

    def _asRedisKey(self, k):
        return f"{self._domainClass.__name__}:{k}"

    def __setitem__(self, k, v: T) -> T:
        redisKey = self._asRedisKey(k)
        redisValue = json.dumps(v.toJsonDict())
        self._rdb.set(redisKey, redisValue, ex=300)
        return v

    def __delitem__(self, k):
        pass

    def __getitem__(self, k) -> T:
        jsonDumps = self._rdb.get(self._asRedisKey(k))
        domainObj = self._domainClass.fromJsonDict(json.loads(jsonDumps))
        return domainObj

    def __len__(self) -> int:
        pass

    def __iter__(self) -> Iterator:
        pass

    def getNewKey(self) -> int:
        pass

    def updateItem(self, key, itemKey, itemValue):
        pass


if __name__ == "__main__":

    @observable
    class MyClass:

        def __init__(self, f, g):
            super().__init__()
            self.f = f
            self._g = g

    class MyRepo(Repository):

        @property
        def type(self):
            return MyClass

        def __setitem__(self, k, v) -> MyClass:
            pass

        def __delitem__(self, k) -> object:
            pass

        def __len__(self) -> int:
            pass

        def __iter__(self) -> Iterator:
            pass

        def getNewKey(self) -> int:
            pass

        def updateItem(self, key, itemKey, itemValue):
            print(key, itemKey, itemValue)

        def __init__(self):
            super().__init__()
            self._data = {1: {"f": 8}}


    myRepo = MyRepo()
    result = myRepo[1]
    result.f = 2
    result.g = 0
