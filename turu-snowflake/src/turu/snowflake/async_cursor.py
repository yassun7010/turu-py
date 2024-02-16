import asyncio
from typing import (
    Any,
    AsyncIterator,
    Generic,
    List,
    Optional,
    Sequence,
    Tuple,
    Type,
    TypedDict,
    Union,
    cast,
    overload,
)

import turu.core.async_cursor
import turu.core.cursor
import turu.core.mock
from turu.core.cursor import map_row
from turu.snowflake.cursor import (
    GenericNewRowType,
    GenericRowType,
)
from turu.snowflake.features import (
    GenericNewPandasDataFrame,
    GenericNewPanderaDataFrameModel,
    GenericNewPyArrowTable,
    GenericPandasDataFrame,
    GenericPyArrowTable,
    PandasDataFrame,
    PanderaDataFrame,
    PanderaDataFrameModel,
    PyArrowTable,
)
from typing_extensions import Never, Self, Unpack, override

import snowflake.connector


class ExecuteOptions(TypedDict, total=False):
    timeout: int
    """timeout[sec]"""

    num_statements: int


class AsyncCursor(
    Generic[GenericRowType, GenericPandasDataFrame, GenericPyArrowTable],
    turu.core.async_cursor.AsyncCursor[GenericRowType, Any],
):
    def __init__(
        self,
        cursor: snowflake.connector.cursor.SnowflakeCursor,
        *,
        row_type: Optional[Type[GenericRowType]] = None,
    ) -> None:
        self._raw_cursor = cursor
        self._row_type: Optional[Type[GenericRowType]] = row_type

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
    ) -> "AsyncCursor[Tuple[Any], PandasDataFrame, PyArrowTable]":
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
    ) -> "AsyncCursor[Tuple[Any], PandasDataFrame, PyArrowTable]":
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

    @overload
    async def execute_map(
        self,
        row_type: Type[GenericNewRowType],
        operation: str,
        parameters: "Optional[Any]" = None,
        /,
        **options: Unpack[ExecuteOptions],
    ) -> "AsyncCursor[GenericNewRowType, Never, Never]":
        ...

    @overload
    async def execute_map(
        self,
        row_type: Type[GenericNewPandasDataFrame],
        operation: str,
        parameters: "Optional[Any]" = None,
        /,
        **options: Unpack[ExecuteOptions],
    ) -> "AsyncCursor[Never, GenericNewPandasDataFrame, Never]":
        ...

    @overload
    async def execute_map(
        self,
        row_type: Type[GenericNewPyArrowTable],
        operation: str,
        parameters: "Optional[Any]" = None,
        /,
        **options: Unpack[ExecuteOptions],
    ) -> "AsyncCursor[Never, Never, GenericNewPyArrowTable]":
        ...

    @overload
    async def execute_map(
        self,
        row_type: Type[GenericNewPanderaDataFrameModel],
        operation: str,
        parameters: "Optional[Any]" = None,
        /,
        **options: Unpack[ExecuteOptions],
    ) -> "AsyncCursor[Never, PanderaDataFrame[GenericNewPanderaDataFrameModel], Never]":
        ...

    @override
    async def execute_map(
        self,
        row_type: Union[
            Type[GenericNewRowType],
            Type[GenericNewPandasDataFrame],
            Type[GenericNewPyArrowTable],
            Type[GenericNewPanderaDataFrameModel],
        ],
        operation: str,
        parameters: "Optional[Any]" = None,
        /,
        **options: Unpack[ExecuteOptions],
    ) -> "AsyncCursor":
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
        self._row_type = cast(Type[GenericRowType], row_type)

        return cast(AsyncCursor, self)

    @overload
    async def executemany_map(
        self,
        row_type: Type[GenericNewRowType],
        operation: str,
        seq_of_parameters: "Sequence[Any]",
        /,
        **options: Unpack[ExecuteOptions],
    ) -> "AsyncCursor[GenericNewRowType, Never, Never]":
        ...

    @overload
    async def executemany_map(
        self,
        row_type: Type[GenericNewPandasDataFrame],
        operation: str,
        seq_of_parameters: "Sequence[Any]",
        /,
        **options: Unpack[ExecuteOptions],
    ) -> "AsyncCursor[Never, GenericNewPandasDataFrame, Never]":
        ...

    @overload
    async def executemany_map(
        self,
        row_type: Type[GenericNewPyArrowTable],
        operation: str,
        seq_of_parameters: "Sequence[Any]",
        /,
        **options: Unpack[ExecuteOptions],
    ) -> "AsyncCursor[Never, Never, GenericNewPyArrowTable]":
        ...

    @overload
    async def executemany_map(
        self,
        row_type: Type[GenericNewPanderaDataFrameModel],
        operation: str,
        seq_of_parameters: "Sequence[Any]",
        /,
        **options: Unpack[ExecuteOptions],
    ) -> "AsyncCursor[Never, PanderaDataFrame[GenericNewPanderaDataFrameModel], Never]":
        ...

    @override
    async def executemany_map(
        self,
        row_type: Union[
            Type[GenericNewRowType],
            Type[GenericNewPandasDataFrame],
            Type[GenericNewPyArrowTable],
            Type[GenericNewPanderaDataFrameModel],
        ],
        operation: str,
        seq_of_parameters: "Sequence[Any]",
        /,
        **options: Unpack[ExecuteOptions],
    ) -> "AsyncCursor":
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
        self._row_type = cast(Type[GenericRowType], row_type)

        return cast(AsyncCursor, self)

    @override
    async def fetchone(self) -> Optional[GenericRowType]:
        row = self._raw_cursor.fetchone()
        if row is None:
            return None

        elif self._row_type is not None:
            return map_row(self._row_type, row)

        else:
            return row  # type: ignore[return-value]

    @override
    async def fetchmany(self, size: Optional[int] = None) -> List[GenericRowType]:
        return [
            map_row(self._row_type, row)
            for row in self._raw_cursor.fetchmany(
                size if size is not None else self.arraysize
            )
        ]

    @override
    async def fetchall(self) -> List[GenericRowType]:
        return [map_row(self._row_type, row) for row in self._raw_cursor.fetchall()]

    async def fetch_arrow_all(self) -> GenericPyArrowTable:
        """Fetches a single Arrow Table."""

        return cast(
            GenericPyArrowTable,
            self._raw_cursor.fetch_arrow_all(force_return_table=True),
        )

    async def fetch_arrow_batches(self) -> AsyncIterator[GenericPyArrowTable]:
        """Fetches Arrow Tables in batches, where 'batch' refers to Snowflake Chunk."""

        for batch in self._raw_cursor.fetch_arrow_batches():
            yield cast(GenericPyArrowTable, batch)

    async def fetch_pandas_all(self, **kwargs: Any) -> GenericPandasDataFrame:
        """Fetch Pandas dataframes."""

        df = self._raw_cursor.fetch_pandas_all(**kwargs)

        if self._row_type and issubclass(self._row_type, PanderaDataFrameModel):
            df = self._row_type.validate(df)  # type: ignore[union-attr]

        return cast(GenericPandasDataFrame, df)

    async def fetch_pandas_batches(
        self, **kwargs: Any
    ) -> AsyncIterator[GenericPandasDataFrame]:
        """Fetch Pandas dataframes in batches, where 'batch' refers to Snowflake Chunk."""

        for batch in self._raw_cursor.fetch_pandas_batches(**kwargs):
            yield cast(
                GenericPandasDataFrame,
                batch,
            )

    @override
    async def __anext__(self) -> GenericRowType:
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
        import turu.snowflake.record.async_record_cursor

        return turu.snowflake.record.async_record_cursor.AsyncRecordCursor
