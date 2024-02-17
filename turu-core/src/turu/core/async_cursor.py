from abc import abstractmethod
from typing import (
    Generic,
    List,
    Optional,
    Sequence,
    Type,
    TypeVar,
)

from turu.core.cursor import GenericNewRowType as GenericNewRowType
from turu.core.cursor import GenericRowType as GenericRowType
from turu.core.protocols.async_cursor import AsyncCursorProtocol
from turu.core.protocols.async_cursor import Parameters as Parameters
from typing_extensions import Self, override


class AsyncCursor(Generic[GenericRowType, Parameters], AsyncCursorProtocol[Parameters]):
    @property
    @abstractmethod
    def rowcount(self) -> int:
        ...

    @property
    @abstractmethod
    def arraysize(self) -> int:
        ...

    @arraysize.setter
    @abstractmethod
    def arraysize(self, size: int) -> None:
        ...

    @abstractmethod
    async def close(self) -> None:
        ...

    @abstractmethod
    async def execute(
        self, operation: str, parameters: Optional[Parameters] = None, /
    ) -> "AsyncCursor":
        ...

    @abstractmethod
    async def executemany(
        self, operation: str, seq_of_parameters: Sequence[Parameters], /
    ) -> "AsyncCursor":
        ...

    @abstractmethod
    async def execute_map(
        self,
        row_type: Type[GenericNewRowType],
        operation: str,
        parameters: Optional[Parameters] = None,
        /,
    ) -> "AsyncCursor[GenericNewRowType, Parameters]":
        """
        Execute a database operation (query or command) and map each row to a `row_type`.

        Parameters:
            row_type: The type of the row that will be returned.
            operation: A database operation (query or command).
            parameters: Parameters may be provided as sequence or mapping and will be bound to variables in the operation.

        Returns:
            A cursor that holds a reference to an operation.
        """
        ...

    @abstractmethod
    async def executemany_map(
        self,
        row_type: Type[GenericNewRowType],
        operation: str,
        seq_of_parameters: Sequence[Parameters],
        /,
    ) -> "AsyncCursor[GenericNewRowType, Parameters]":
        """Execute a database operation (query or command) against all parameter sequences or mappings.

        Parameters:
            row_type: The type of the row that will be returned.
            operation: A database operation (query or command).
            seq_of_parameters: Parameters may be provided as sequence or mapping and will be bound to variables in the operation.

        Returns:
            A cursor that holds a reference to an operation.
        """
        ...

    @abstractmethod
    async def fetchone(self) -> Optional[GenericRowType]:
        ...

    @abstractmethod
    async def fetchmany(self, size: Optional[int] = None) -> List[GenericRowType]:
        ...

    @abstractmethod
    async def fetchall(self) -> List[GenericRowType]:
        ...

    @override
    def __aiter__(self) -> Self:
        return self

    @abstractmethod
    async def __anext__(self) -> GenericRowType:
        ...

    @override
    async def __aenter__(self):
        return self

    @override
    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.close()


GenericAsyncCursor = TypeVar("GenericAsyncCursor", bound=AsyncCursor)
