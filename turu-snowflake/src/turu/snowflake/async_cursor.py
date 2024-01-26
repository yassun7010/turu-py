import asyncio
from typing import Any, Iterator, List, Optional, Sequence, Tuple, Type, TypedDict, cast

import turu.core.async_cursor
import turu.core.mock
import turu.snowflake.record.async_record_cursor
from turu.core.cursor import map_row
from turu.snowflake.features import PandasDataFlame
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
        """Prepare and execute a database operation (query or command).

        Parameters:
            operation: A database operation (query or command).
            parameters: Parameters may be provided as sequence or mapping and will be bound to variables in the operation.
            options: snowflake connector options

        Returns:
            A cursor that holds a reference to an operation.
        """

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
        """Prepare a database operation (query or command)
        and then execute it against all parameter sequences or mappings.

        Caution:
            executemany does not support async. Actually, this is sync.

        Parameters:
            operation: A database operation (query or command).
            seq_of_parameters: Parameters may be provided as sequence or mapping and will be bound to variables in the operation.
            options: snowflake connector options

        Returns:
            A cursor that holds a reference to an operation.
        """

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
        """
        Execute a database operation (query or command) and map each row to a `row_type`.

        Parameters:
            row_type: The type of the row that will be returned.
            operation: A database operation (query or command).
            parameters: Parameters may be provided as sequence or mapping and will be bound to variables in the operation.
            options: snowflake connector options

        Returns:
            A cursor that holds a reference to an operation.
        """

        self._raw_cursor.execute(operation, parameters, **options)
        self._row_type = cast(Type[turu.core.async_cursor.GenericRowType], row_type)

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
        """Execute a database operation (query or command) against all parameter sequences or mappings.

        Caution:
            executemany does not support async. Actually, this is sync.

        Parameters:
            operation: A database operation (query or command).
            seq_of_parameters: Parameters may be provided as sequence or mapping and will be bound to variables in the operation.
            options: snowflake connector options

        Returns:
            A cursor that holds a reference to an operation.
        """

        self._raw_cursor.executemany(operation, seq_of_parameters, **options)
        self._row_type = cast(Type[turu.core.async_cursor.GenericRowType], row_type)

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

    async def fetch_arrow_all(self):
        """Fetches a single Arrow Table."""

        return self._raw_cursor.fetch_arrow_all()

    async def fetch_arrow_batches(self):
        """Fetches Arrow Tables in batches, where 'batch' refers to Snowflake Chunk."""

        return self._raw_cursor.fetch_arrow_batches()

    async def fetch_pandas_all(self, **kwargs) -> PandasDataFlame:
        """Fetch Pandas dataframes."""

        return self._raw_cursor.fetch_pandas_all(**kwargs)

    async def fetch_pandas_batches(self, **kwargs) -> Iterator[PandasDataFlame]:
        """Fetch Pandas dataframes in batches, where 'batch' refers to Snowflake Chunk."""

        return self._raw_cursor.fetch_pandas_batches(**kwargs)

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
        """Use a warehouse in cursor."""

        self._raw_cursor.execute(f"use warehouse {warehouse}")

        return self

    def use_database(self, database: str, /) -> Self:
        """Use a database in cursor."""

        self._raw_cursor.execute(f"use database {database}")

        return self

    def use_schema(self, schema: str, /) -> Self:
        """Use a schema in cursor."""

        self._raw_cursor.execute(f"use schema {schema}")

        return self

    def use_role(self, role: str, /) -> Self:
        """Use a role in cursor."""

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

    @property
    def _AsyncRecordCursor(
        self,
    ):
        return turu.snowflake.record.async_record_cursor.AsyncRecordCursor
