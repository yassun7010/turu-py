from abc import abstractmethod
from typing import Any, Optional, Sequence, Tuple, Type

import turu.core.async_cursor
from turu.core.protocols.async_connection import AsyncConnectionProtocol
from turu.core.protocols.async_cursor import Parameters
from typing_extensions import Never


class AsyncConnection(AsyncConnectionProtocol):
    @abstractmethod
    async def close(self) -> None:
        ...

    @abstractmethod
    async def commit(self) -> None:
        ...

    @abstractmethod
    async def rollback(self) -> None:
        ...

    @abstractmethod
    async def cursor(self) -> turu.core.async_cursor.AsyncCursor[Never, Any]:
        ...

    async def execute(
        self,
        operation: str,
        parameters: Optional[Parameters] = None,
        /,
    ) -> turu.core.async_cursor.AsyncCursor[Tuple[Any], Parameters]:
        return await (await self.cursor()).execute(operation, parameters)

    async def executemany(
        self,
        operation: str,
        seq_of_parameters: Sequence[Parameters],
        /,
    ) -> turu.core.async_cursor.AsyncCursor[Tuple[Any], Parameters]:
        return await (await self.cursor()).executemany(operation, seq_of_parameters)

    async def execute_map(
        self,
        row_type: Type[turu.core.async_cursor.GenericNewRowType],
        operation: str,
        parameters: Optional[Parameters] = None,
        /,
    ) -> turu.core.async_cursor.AsyncCursor[
        turu.core.async_cursor.GenericNewRowType, Parameters
    ]:
        return await (await self.cursor()).execute_map(row_type, operation, parameters)

    async def executemany_map(
        self,
        row_type: Type[turu.core.async_cursor.GenericNewRowType],
        operation: str,
        seq_of_parameters: Sequence[Parameters],
        /,
    ) -> turu.core.async_cursor.AsyncCursor[
        turu.core.async_cursor.GenericNewRowType, Parameters
    ]:
        return await (await self.cursor()).executemany_map(
            row_type, operation, seq_of_parameters
        )
