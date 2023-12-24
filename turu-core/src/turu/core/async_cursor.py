from abc import abstractmethod
from typing import (
    Generic,
    List,
    Optional,
    Sequence,
    Type,
)

from turu.core.cursor import GenericNewRowType, GenericRowType
from turu.core.protocols.async_cursor import AsyncCursorProtocol, Parameters
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
        ...

    @abstractmethod
    async def executemany_map(
        self,
        row_type: Type[GenericNewRowType],
        operation: str,
        seq_of_parameters: Sequence[Parameters],
        /,
    ) -> "AsyncCursor[GenericNewRowType, Parameters]":
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
