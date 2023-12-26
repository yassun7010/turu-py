# NOTE: It's a draft. It's NOT ready for production use.

from typing import Any, Generic, List, Optional, Protocol, Sequence

from turu.core.protocols.cursor import Parameters
from typing_extensions import Self


class AsyncCursorProtocol(Generic[Parameters], Protocol):
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
        """
        This read/write attribute specifies the number of rows to fetch at a time with `.fetchmany()`.
        It defaults to 1 meaning to fetch a single row at a time.

        Implementations must observe this value with respect to the `.fetchmany()` method,
        but are free to interact with the database a single row at a time.
        It may also be used in the implementation of `.executemany()`.
        """
        ...

    @arraysize.setter
    def arraysize(self, size: int) -> None:
        ...

    async def close(self) -> None:
        ...

    async def execute(
        self, operation: str, parameters: Optional[Parameters] = None, /
    ) -> Self:
        ...

    async def executemany(
        self, operation: str, seq_of_parameters: Sequence[Parameters], /
    ) -> Self:
        ...

    async def fetchone(self) -> Optional[Any]:
        ...

    async def fetchmany(self, size: Optional[int] = None) -> List[Any]:
        ...

    async def fetchall(self) -> List[Any]:
        ...

    def __aiter__(self) -> Self:
        ...

    async def __anext__(self) -> Any:
        ...

    async def __aenter__(self) -> Self:
        ...

    async def __aexit__(self, exc_type, exc_value, traceback):
        ...
