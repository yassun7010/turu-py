from typing import Any, Optional, Sequence, Tuple, Type, cast

import turu.core.async_cursor
import turu.core.mock
from typing_extensions import Self, Unpack, override

from .async_cursor import AsyncCursor, ExecuteOptions


class MockAsyncCursor(  # type: ignore
    turu.core.mock.MockAsyncCursor[turu.core.async_cursor.GenericRowType, Any],  # type: ignore
    AsyncCursor[turu.core.async_cursor.GenericRowType],  # type: ignore
):
    @override
    async def execute(
        self,
        operation: str,
        parameters: Optional[Any] = None,
        /,
        **options: Unpack[ExecuteOptions],
    ) -> "MockAsyncCursor[Tuple[Any]]":
        return cast(MockAsyncCursor, await super().execute(operation, parameters))

    @override
    async def executemany(
        self,
        operation: str,
        seq_of_parameters: Sequence[Any],
        /,
        **options: Unpack[ExecuteOptions],
    ) -> "MockAsyncCursor[Tuple[Any]]":
        return cast(
            MockAsyncCursor, await super().executemany(operation, seq_of_parameters)
        )

    @override
    async def execute_map(
        self,
        row_type: Type[turu.core.async_cursor.GenericNewRowType],
        operation: str,
        parameters: Optional[Any] = None,
        /,
        **options: Unpack[ExecuteOptions],
    ) -> "MockAsyncCursor[turu.core.async_cursor.GenericNewRowType]":
        return cast(
            MockAsyncCursor, await super().execute_map(row_type, operation, parameters)
        )

    @override
    async def executemany_map(
        self,
        row_type: Type[turu.core.async_cursor.GenericNewRowType],
        operation: str,
        seq_of_parameters: Sequence[Any],
        /,
        **options: Unpack[ExecuteOptions],
    ) -> "MockAsyncCursor[turu.core.async_cursor.GenericNewRowType]":
        return cast(
            MockAsyncCursor,
            await super().executemany_map(row_type, operation, seq_of_parameters),
        )

    def use_warehouse(self, warehouse: str, /) -> Self:
        return self

    def use_database(self, database: str, /) -> Self:
        return self

    def use_schema(self, schema: str, /) -> Self:
        return self

    def use_role(self, role: str, /) -> Self:
        return self
