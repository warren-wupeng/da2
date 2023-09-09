import dataclasses
from enum import Enum
from typing import Callable


class ObservableMixin:

    def __post_init__(self):
        self._keyUpdateCallbacks = []

    def __setattr__(self, key, value):
        super().__setattr__(key, value)
        if key.startswith('_'):
            return

        if hasattr(self, "_keyUpdateCallbacks"):
            for callback in self._keyUpdateCallbacks:
                callback(key, value)

    def addKeyUpdateCallback(self, callback: Callable):
        self._keyUpdateCallbacks.append(callback)

    # def _traverse(self, key, value):
    #     # if isinstance(value, JsonDictMixin):
    #     #     return value.toJsonDict()
    #     if isinstance(value, dict):
    #         return self._traverseDict(value)
    #     elif isinstance(value, list):
    #         return [self._traverse(key, i) for i in value]
    #     elif isinstance(value, Enum):
    #         return value.value
    #     elif hasattr(value, '__dict__'):
    #         return self._traverseDict(value.__dict__)
    #     else:
    #         return value
    #
    # def _traverseDict(self, instance_dict):
    #     output = {}
    #     for key, value in instance_dict.items():
    #         if key.startswith('_'):
    #             continue
    #         if isinstance(value, type):
    #             continue
    #         output[key] = self._traverse(key, value)
    #     return output
    #
    # def toJsonDict(self):
    #
    #     result = self._traverseDict(self.__dict__)
    #     return result



class PydanticObservableMixin:
    # _keyUpdateCallbacks: list[Callable]

    def __setattr__(self, key, value):
        super().__setattr__(key, value)
        if key.startswith('_'):
            return

        if hasattr(self, "_keyUpdateCallbacks"):
            for callback in self._keyUpdateCallbacks:
                callback(key, value)

    def addKeyUpdateCallback(self, callback: Callable):
        if not hasattr(self, "_keyUpdateCallbacks"):
            setattr(self, "_keyUpdateCallbacks", [])
        self._keyUpdateCallbacks.append(callback)

