from abc import abstractmethod
from typing import Generic, overload, Iterable, Any, TypeVar

from .entity import Entity


E = TypeVar("E", bound=Entity[Any, Any])


class Many(Generic[E]):

    @overload
    @abstractmethod
    def __getitem__(self, i: int) -> E: ...

    @overload
    @abstractmethod
    def __getitem__(self, s: slice) -> Iterable[E]: ...

    def __getitem__(self, i: int) -> E:
        pass

    @abstractmethod
    def __len__(self) -> int:
        pass

    # @overload
    # @abstractmethod
    # def __setitem__(self, i: int, o: E) -> None: ...
    #
    # @overload
    # @abstractmethod
    # def __setitem__(self, s: slice, o: Iterable[E]) -> None: ...
    #
    # def __setitem__(self, i: int, o: E) -> None:
    #     pass

    # @overload
    # @abstractmethod
    # def __delitem__(self, i: int) -> None: ...
    #
    # @overload
    # @abstractmethod
    # def __delitem__(self, i: slice) -> None: ...
    #
    # def __delitem__(self, i: int) -> None:
    #     pass






