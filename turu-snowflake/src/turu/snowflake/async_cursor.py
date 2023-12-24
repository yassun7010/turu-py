import asyncio
from typing import Any, List, Optional, Sequence, Tuple, Type, TypedDict, cast

import turu.core.async_cursor
import turu.core.mock
from turu.core.cursor import map_row
from typing_extensions import Self, Unpack, override

import snowflake.connector


class ExecuteOptions(TypedDict, total=False):
    timeout: int
    """timeout[sec]"""

    num_statements: int


class AsyncCursor(
    turu.core.async_cursor.AsyncCursor[turu.core.async_cursor.GenericRowType, Any],
):
    def __init__(
        self,
        cursor: snowflake.connector.cursor.SnowflakeCursor,
        *,
        row_type: Optional[Type[turu.core.async_cursor.GenericRowType]] = None,
    ) -> None:
        self._raw_cursor = cursor
        self._row_type: Optional[Type[turu.core.async_cursor.GenericRowType]] = row_type

    @property
    def rowcount(self) -> int:
        return self._raw_cursor.rowcount or -1

    @property
    def arraysize(self) -> int:
        return self._raw_cursor.arraysize

    @arraysize.setter
    def arraysize(self, size: int) -> None:
        self._raw_cursor.arraysize = size

    @override
    async def close(self) -> None:
        self._raw_cursor.close()

    @override
    async def execute(
        self,
        operation: str,
        parameters: Optional[Any] = None,
        /,
        **options: Unpack[ExecuteOptions],
    ) -> "AsyncCursor[Tuple[Any]]":
        await self._execute_async(operation, parameters, **options)
        self._row_type = None

        return cast(AsyncCursor, self)

    @override
    async def executemany(
        self,
        operation: str,
        seq_of_parameters: Sequence[Any],
        /,
        **options: Unpack[ExecuteOptions],
    ) -> "AsyncCursor[Tuple[Any]]":
        """CAUTION: executemany does not support async. Actually, this is sync."""
        self._raw_cursor.executemany(operation, seq_of_parameters, **options)
        self._row_type = None

        return cast(AsyncCursor, self)

    @override
    async def execute_map(
        self,
        row_type: Type[turu.core.async_cursor.GenericNewRowType],
        operation: str,
        parameters: "Optional[Any]" = None,
        /,
        **options: Unpack[ExecuteOptions],
    ) -> "AsyncCursor[turu.core.async_cursor.GenericNewRowType]":
        self._raw_cursor.execute(operation, parameters, **options)
        self._row_type = cast(turu.core.async_cursor.GenericRowType, row_type)

        return cast(AsyncCursor, self)

    @override
    async def executemany_map(
        self,
        row_type: Type[turu.core.async_cursor.GenericNewRowType],
        operation: str,
        seq_of_parameters: "Sequence[Any]",
        /,
        **options: Unpack[ExecuteOptions],
    ) -> "AsyncCursor[turu.core.async_cursor.GenericNewRowType]":
        """CAUTION: executemany does not support async. Actually, this is sync."""
        self._raw_cursor.executemany(operation, seq_of_parameters, **options)
        self._row_type = cast(turu.core.async_cursor.GenericRowType, row_type)

        return cast(AsyncCursor, self)

    @override
    async def fetchone(self) -> Optional[turu.core.async_cursor.GenericRowType]:
        row = self._raw_cursor.fetchone()
        if row is None:
            return None

        elif self._row_type is not None:
            return map_row(self._row_type, row)

        else:
            return row  # type: ignore[return-value]

    @override
    async def fetchmany(
        self, size: Optional[int] = None
    ) -> List[turu.core.async_cursor.GenericRowType]:
        return [
            map_row(self._row_type, row)
            for row in self._raw_cursor.fetchmany(
                size if size is not None else self.arraysize
            )
        ]

    @override
    async def fetchall(self) -> List[turu.core.async_cursor.GenericRowType]:
        return [map_row(self._row_type, row) for row in self._raw_cursor.fetchall()]

    @override
    async def __anext__(self) -> turu.core.async_cursor.GenericRowType:
        next_row = self._raw_cursor.fetchone()

        if next_row is None:
            raise StopAsyncIteration()

        if self._row_type is not None:
            return map_row(self._row_type, next_row)

        else:
            return next_row  # type: ignore[return-value]

    def use_warehouse(self, warehouse: str, /) -> Self:
        self._raw_cursor.execute(f"use warehouse {warehouse}")

        return self

    def use_database(self, database: str, /) -> Self:
        self._raw_cursor.execute(f"use database {database}")

        return self

    def use_schema(self, schema: str, /) -> Self:
        self._raw_cursor.execute(f"use schema {schema}")

        return self

    def use_role(self, role: str, /) -> Self:
        self._raw_cursor.execute(f"use role {role}")

        return self

    async def _execute_async(
        self,
        operation: str,
        parameters: Optional[Any] = None,
        /,
        **options: Unpack[ExecuteOptions],
    ) -> None:
        cur = self._raw_cursor
        cur.execute_async(operation, parameters, **options)
        conn = cur.connection
        query_id = cur.sfqid

        if query_id:
            while conn.is_still_running(conn.get_query_status(query_id)):
                await asyncio.sleep(0.01)

            cur.get_results_from_sfqid(query_id)


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
