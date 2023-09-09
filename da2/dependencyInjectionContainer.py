import inspect
from typing import TypeVar

T = TypeVar('T')


class Container:
    dependencies = {}

    @classmethod
    def register(cls, key: T):
        def wrapper(dependency):
            cls.dependencies[key] = dependency
            return dependency
        return wrapper

    @classmethod
    def resolve(cls, key: T) -> T:
        dependency = cls.dependencies.get(key)
        if not dependency:
            raise Exception(f"No dependency registered for {key}")

        fullArgSpec = inspect.getfullargspec(dependency)
        subDepends = dict()
        if fullArgSpec.args and fullArgSpec.args[0] == 'self':

            for arg, argClass in fullArgSpec.annotations.items():
                subDepends[arg] = cls.resolve(argClass)

        if callable(dependency):
            return dependency(**subDepends)
        return dependency
