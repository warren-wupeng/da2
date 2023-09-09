from enum import Enum


class JsonDictMixin:

    @classmethod
    def fromJsonDict(cls, data: dict):

        return cls(**data)

    def toJsonDict(self):
        result = self._traverseDict(self.__dict__)
        return result

    def _traverseDict(self, instance_dict):
        output = {}
        for key, value in instance_dict.items():
            if key.startswith('_'):
                continue
            if isinstance(value, type):
                continue
            output[key] = self._traverse(key, value)
        return output

    def _traverse(self, key, value):
        if isinstance(value, JsonDictMixin):
            return value.toJsonDict()
        elif isinstance(value, dict):
            return self._traverseDict(value)
        elif isinstance(value, list):
            return [self._traverse(key, i) for i in value]
        elif isinstance(value, Enum):
            return value.value
        elif hasattr(value, '__dict__'):
            return self._traverseDict(value.__dict__)
        else:
            return value


def jsonDictable(cls: type):
    result = type(cls.__name__, (cls, JsonDictMixin), dict(cls.__dict__))
    return result


class JsonDictDescriptor:

    def __init__(self, cls: type):
        self.cls = cls

    def __set__(self, instance, value):
        pass
        # if issubclass(self.cls, value):
        # setattr(instance, self.internalName, )

    def __get__(self, instance, owner):
        pass

    def __set_name__(self, owner, name):
        self.name = name
        self.internalName = '_'+name


if __name__ == "__main__":
    @jsonDictable
    class MyClass:

        def __init__(self):
            self._events = []
            self.f = 1

        @property
        def events(self):
            return self._events

        def foo(self):
            pass


    myClass = MyClass()
    print(myClass.toJsonDict())
