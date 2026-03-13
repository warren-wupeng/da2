from __future__ import annotations

import inspect
from typing import Any


class Container:
    """Simple dependency injection container with recursive resolution.

    Register factories or values by key, then resolve them. If a factory's
    constructor has type-annotated parameters matching registered keys,
    they are resolved recursively.

    Example::

        from da2 import Container

        container = Container()
        container.register("db_url", "sqlite:///app.db")
        container.register("pool_size", 5)

        assert container.resolve("db_url") == "sqlite:///app.db"
        assert container.resolve("pool_size") == 5
    """

    def __init__(self) -> None:
        self._dependencies: dict[Any, Any] = {}

    def register(self, key: Any, factory_or_value: Any) -> None:
        """Register a dependency by key. Can be a value or a callable factory."""
        self._dependencies[key] = factory_or_value

    def resolve(self, key: Any) -> Any:
        """Resolve a dependency by key. Callables are invoked; values returned as-is."""
        dependency = self._dependencies.get(key)
        if dependency is None:
            registered = list(self._dependencies.keys())
            raise KeyError(
                f"No dependency registered for {key!r}. "
                f"Registered keys: {registered}"
            )

        if not callable(dependency):
            return dependency

        spec = inspect.getfullargspec(dependency)
        sub_deps: dict[str, Any] = {}
        if spec.args and spec.args[0] == "self":
            for arg, arg_type in spec.annotations.items():
                if arg_type in self._dependencies:
                    sub_deps[arg] = self.resolve(arg_type)

        return dependency(**sub_deps)
