from typing import Any, Generic, List, Optional, Protocol, Sequence, TypeVar

from typing_extensions import Self

Parameters = TypeVar("Parameters", contravariant=True)


class CursorProtocol(Generic[Parameters], Protocol):
    @property
    def rowcount(self) -> int:
        """The number of rows that the last `.execute*()` produced (for DQL statements like )
        or affected (for DML statements like or ).

        The attribute is `-1` in case no `.execute*()` has been performed
        on the cursor or the rowcount of the last operation is cannot be determined by the interface.
        """
        ...

    @property
    def arraysize(self) -> int:
        """The number of rows to fetch at a time with `.fetchmany()`.

        It defaults to 1 meaning to fetch a single row at a time.
        """
        ...

    @arraysize.setter
    def arraysize(self, size: int) -> None:
        ...

    def close(self) -> None:
        ...

    def execute(
        self, operation: str, parameters: Optional[Parameters] = None, /
    ) -> Self:
        """Prepare and execute a database operation (query or command).

        Parameters:
            operation: A database operation (query or command).
            parameters: Parameters may be provided as sequence or mapping and will be bound to variables in the operation.

        Returns:
            A cursor that holds a reference to an operation.
        """
        ...

    def executemany(
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

    def fetchone(self) -> Optional[Any]:
        """Fetch the next row of a query result set."""
        ...

    def fetchmany(self, size: Optional[int] = None) -> List[Any]:
        """Fetch the next set of rows of a query result.

        An empty sequence is returned when no more rows are available.

        Parameters:
            size: The number of rows to fetch per call.
                    If this parameter is not used, it is usually refer to use the `.arraysize` attribute (Default is `1`).
                    If this parameter is used, then it is best for it to retain the same value from one `.fetchmany()` call to the next.
        """
        ...

    def fetchall(self) -> List[Any]:
        """Fetch all (remaining) rows of a query result"""
        ...

    def __iter__(self) -> Self:
        ...

    def __next__(self) -> Any:
        ...

    def __enter__(self) -> Self:
        ...

    def __exit__(self, exc_type, exc_value, traceback):
        ...
