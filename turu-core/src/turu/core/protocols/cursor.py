from typing import Generic, List, Protocol, Sequence, TypeVar

from typing_extensions import Self

_Parameters = TypeVar("_Parameters", contravariant=True)


class CursorProtocol(Generic[_Parameters], Protocol):
    def execute(self, operation: str, parameters: _Parameters = ..., /) -> Self:
        ...

    def executemany(
        self, operation: str, seq_of_parameters: Sequence[_Parameters], /
    ) -> Self:
        ...

    def fetchone(self) -> Self:
        ...

    def fetchmany(self, size: int = 1) -> List[Self]:
        ...

    def fetchall(self) -> List[Self]:
        ...

    def __iter__(self) -> Self:
        ...

    def __next__(self) -> Self:
        ...
