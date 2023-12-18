from typing import Any, Generic, Optional, Protocol, Sequence, TypeVar

from typing_extensions import Self

_Parameters = TypeVar("_Parameters", contravariant=True)


class CursorProtocol(Generic[_Parameters], Protocol):
    def execute(self, operation: str, parameters: _Parameters = ..., /) -> Self:
        ...

    def executemany(
        self, operation: str, seq_of_parameters: Sequence[_Parameters], /
    ) -> Self:
        ...

    def fetchone(self) -> Optional[Any]:
        ...

    def fetchmany(self, size: int = 1) -> Sequence[Any]:
        ...

    def fetchall(self) -> Sequence[Any]:
        ...

    def __iter__(self) -> Self:
        ...

    def __next__(self) -> Any:
        ...
