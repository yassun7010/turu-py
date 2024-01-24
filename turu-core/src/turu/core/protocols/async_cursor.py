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
        """
        Prepare and execute a database operation (query or command).

        Parameters:
            operation: A database operation (query or command).
            parameters: Parameters may be provided as sequence or mapping and will be bound to variables in the operation.

        Returns:
            A cursor that holds a reference to an operation.
        """
        ...

    async def executemany(
        self, operation: str, seq_of_parameters: Sequence[Parameters], /
    ) -> Self:
        """Prepare a database operation (query or command)
        and then execute it against all parameter sequences or mappings.

        Parameters:
            operation: A database operation (query or command).
            seq_of_parameters: Parameters may be provided as sequence or mapping and will be bound to variables in the operation.

        Returns:
            A cursor that holds a reference to an operation.
        """
        ...

    async def fetchone(self) -> Optional[Any]:
        """Fetch the next row of a query result set."""
        ...

    async def fetchmany(self, size: Optional[int] = None) -> List[Any]:
        """Fetch the next set of rows of a query result.

        An empty sequence is returned when no more rows are available.

        Parameters:
            size: The number of rows to fetch per call.
                    If this parameter is not used, it is usually refer to use the `.arraysize` attribute (Default is `1`).
                    If this parameter is used, then it is best for it to retain the same value from one `.fetchmany()` call to the next.
        """
        ...

    async def fetchall(self) -> List[Any]:
        """Fetch all (remaining) rows of a query result"""
        ...

    def __aiter__(self) -> Self:
        ...

    async def __anext__(self) -> Any:
        ...

    async def __aenter__(self) -> Self:
        ...

    async def __aexit__(self, exc_type, exc_value, traceback):
        ...
