from typing import Any, Generic, List, Optional, Protocol, Sequence, TypeVar

from typing_extensions import Self

Parameters = TypeVar("Parameters", contravariant=True)


class CursorProtocol(Generic[Parameters], Protocol):
    def execute(self, operation: str, parameters: Parameters = ..., /) -> Self:
        ...

    def executemany(
        self, operation: str, seq_of_parameters: Sequence[Parameters], /
    ) -> Self:
        ...

    def fetchone(self) -> Optional[Any]:
        ...

    def fetchmany(self, size: int = -1) -> List[Any]:
        ...

    def fetchall(self) -> List[Any]:
        ...

    def __iter__(self) -> Self:
        ...

    def __next__(self) -> Any:
        ...
