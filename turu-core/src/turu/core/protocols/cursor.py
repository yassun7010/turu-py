from typing import Any, Generic, List, Optional, Protocol, Sequence, TypeVar

from typing_extensions import Self

Parameters = TypeVar("Parameters", contravariant=True)


class CursorProtocol(Generic[Parameters], Protocol):
    @property
    def rowcount(self) -> int:
        """
        the number of rows that the last .execute*() produced (for DQL statements like )
        or affected (for DML statements like or ).

        The attribute is -1 in case no .execute*() has been performed
        on the cursor or the rowcount of the last operation is cannot be determined by the interface.
        """
        ...

    @property
    def arraysize(self) -> int:
        ...

    @arraysize.setter
    def arraysize(self, size: int) -> None:
        ...

    def close(self) -> None:
        ...

    def execute(
        self, operation: str, parameters: Optional[Parameters] = None, /
    ) -> Self:
        ...

    def executemany(
        self, operation: str, seq_of_parameters: Sequence[Parameters], /
    ) -> Self:
        ...

    def fetchone(self) -> Optional[Any]:
        ...

    def fetchmany(self, size: Optional[int] = None) -> List[Any]:
        ...

    def fetchall(self) -> List[Any]:
        ...

    def __iter__(self) -> Self:
        ...

    def __next__(self) -> Any:
        ...

    def __enter__(self) -> Self:
        ...

    def __exit__(self, exc_type, exc_value, traceback):
        ...
