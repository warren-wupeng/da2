import inspect


class Container:

    def __init__(self):
        self._dependencies: dict = {}

    def register(self, key, factory_or_value):
        self._dependencies[key] = factory_or_value

    def resolve(self, key):
        dependency = self._dependencies.get(key)
        if dependency is None:
            raise KeyError(f"No dependency registered for {key}")

        if not callable(dependency):
            return dependency

        spec = inspect.getfullargspec(dependency)
        sub_deps = {}
        if spec.args and spec.args[0] == "self":
            for arg, arg_type in spec.annotations.items():
                if arg in self._dependencies:
                    sub_deps[arg] = self.resolve(arg_type)

        return dependency(**sub_deps)
