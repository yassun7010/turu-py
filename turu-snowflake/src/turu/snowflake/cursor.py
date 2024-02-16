from typing import (
    Any,
    Generic,
    Iterator,
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

import turu.core.cursor
import turu.core.mock
from turu.core.cursor import GenericNewRowType, GenericRowType
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
    """number of statements"""


class Cursor(
    Generic[GenericRowType, GenericPandasDataFrame, GenericPyArrowTable],
    turu.core.cursor.Cursor[GenericRowType, Any],
):
    """
    A cursor is a database object that is used to manage the context of a fetch operation.

    This class is a wrapper around the `snowflake.connector.cursor.SnowflakeCursor` class.
    """

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
    def close(self) -> None:
        self._raw_cursor.close()

    @override
    def execute(
        self,
        operation: str,
        parameters: Optional[Any] = None,
        /,
        **options: Unpack[ExecuteOptions],
    ) -> "Cursor[Tuple[Any], PandasDataFrame, PyArrowTable]":
        """Prepare and execute a database operation (query or command).

        Parameters:
            operation: A database operation (query or command).
            parameters: Parameters may be provided as sequence or mapping and will be bound to variables in the operation.
            options: snowflake connector options

        Returns:
            A cursor that holds a reference to an operation.
        """

        self._raw_cursor.execute(operation, parameters, **options)
        self._row_type = None

        return cast(Cursor, self)

    @override
    def executemany(
        self,
        operation: str,
        seq_of_parameters: Sequence[Any],
        /,
        **options: Unpack[ExecuteOptions],
    ) -> "Cursor[Tuple[Any], PandasDataFrame, PyArrowTable]":
        """Prepare a database operation (query or command)
        and then execute it against all parameter sequences or mappings.

        Parameters:
            operation: A database operation (query or command).
            seq_of_parameters: Parameters may be provided as sequence or mapping and will be bound to variables in the operation.
            options: snowflake connector options

        Returns:
            A cursor that holds a reference to an operation.
        """

        self._raw_cursor.executemany(operation, seq_of_parameters, **options)
        self._row_type = None

        return cast(Cursor, self)

    @overload
    def execute_map(
        self,
        row_type: Type[GenericNewRowType],
        operation: str,
        parameters: "Optional[Any]" = None,
        /,
        **options: Unpack[ExecuteOptions],
    ) -> "Cursor[GenericNewRowType, Never, Never]":
        ...

    @overload
    def execute_map(
        self,
        row_type: Type[GenericNewPandasDataFrame],
        operation: str,
        parameters: "Optional[Any]" = None,
        /,
        **options: Unpack[ExecuteOptions],
    ) -> "Cursor[Never, GenericNewPandasDataFrame, Never]":
        ...

    @overload
    def execute_map(
        self,
        row_type: Type[GenericNewPyArrowTable],
        operation: str,
        parameters: "Optional[Any]" = None,
        /,
        **options: Unpack[ExecuteOptions],
    ) -> "Cursor[Never,  Never, GenericNewPyArrowTable]":
        ...

    @overload
    def execute_map(
        self,
        row_type: Type[GenericNewPanderaDataFrameModel],
        operation: str,
        parameters: "Optional[Any]" = None,
        /,
        **options: Unpack[ExecuteOptions],
    ) -> "Cursor[Never, PanderaDataFrame[GenericNewPanderaDataFrameModel], Never]":
        ...

    @override
    def execute_map(
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
    ) -> "Cursor":
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

        return cast(Cursor, self)

    @overload
    def executemany_map(
        self,
        row_type: Type[GenericNewRowType],
        operation: str,
        seq_of_parameters: "Sequence[Any]",
        /,
        **options: Unpack[ExecuteOptions],
    ) -> "Cursor[GenericNewRowType, Never, Never]":
        pass

    @overload
    def executemany_map(
        self,
        row_type: Type[GenericNewPandasDataFrame],
        operation: str,
        seq_of_parameters: "Sequence[Any]",
        /,
        **options: Unpack[ExecuteOptions],
    ) -> "Cursor[Never, GenericNewPandasDataFrame, Never]":
        pass

    @overload
    def executemany_map(
        self,
        row_type: Type[GenericNewPyArrowTable],
        operation: str,
        seq_of_parameters: "Sequence[Any]",
        /,
        **options: Unpack[ExecuteOptions],
    ) -> "Cursor[Never, Never, GenericNewPyArrowTable]":
        pass

    @overload
    def executemany_map(
        self,
        row_type: Type[GenericNewPanderaDataFrameModel],
        operation: str,
        seq_of_parameters: Sequence[Any],
        /,
        **options: Unpack[ExecuteOptions],
    ) -> "Cursor[Never, PanderaDataFrame[GenericNewPanderaDataFrameModel], Never]":
        ...

    @override
    def executemany_map(
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
    ) -> "Cursor":
        """Execute a database operation (query or command) against all parameter sequences or mappings.

        Parameters:
            row_type: The type of the row that will be returned.
            operation: A database operation (query or command).
            seq_of_parameters: Parameters may be provided as sequence or mapping and will be bound to variables in the operation.
            options: snowflake connector options

        Returns:
            A cursor that holds a reference to an operation.
        """

        self._raw_cursor.executemany(operation, seq_of_parameters, **options)
        self._row_type = cast(Type[GenericRowType], row_type)

        return cast(Cursor, self)

    @override
    def fetchone(self) -> Optional[GenericRowType]:
        row = self._raw_cursor.fetchone()
        if row is None:
            return None

        elif self._row_type is not None:
            return turu.core.cursor.map_row(self._row_type, row)

        else:
            return row  # type: ignore[return-value]

    @override
    def fetchmany(self, size: Optional[int] = None) -> List[GenericRowType]:
        return [
            turu.core.cursor.map_row(self._row_type, row)
            for row in self._raw_cursor.fetchmany(
                size if size is not None else self.arraysize
            )
        ]

    @override
    def fetchall(self) -> List[GenericRowType]:
        return [
            turu.core.cursor.map_row(self._row_type, row)
            for row in self._raw_cursor.fetchall()
        ]

    @override
    def __next__(self) -> GenericRowType:
        next_row = self._raw_cursor.fetchone()

        if next_row is None:
            raise StopIteration()

        if self._row_type is not None:
            return turu.core.cursor.map_row(self._row_type, next_row)

        else:
            return next_row  # type: ignore[return-value]

    def fetch_arrow_all(self) -> GenericPyArrowTable:
        """Fetches a single Arrow Table."""

        return cast(
            GenericPyArrowTable,
            self._raw_cursor.fetch_arrow_all(force_return_table=True),
        )

    def fetch_arrow_batches(self) -> "Iterator[GenericPyArrowTable]":
        """Fetches Arrow Tables in batches, where 'batch' refers to Snowflake Chunk."""

        return cast(
            Iterator[GenericPyArrowTable], self._raw_cursor.fetch_arrow_batches()
        )

    def fetch_pandas_all(self, **kwargs: Any) -> "GenericPandasDataFrame":
        """Fetch a single Pandas dataframe."""
        df = self._raw_cursor.fetch_pandas_all(**kwargs)

        if self._row_type and issubclass(self._row_type, PanderaDataFrameModel):
            df = self._row_type.validate(df)  # type: ignore[assignment]

        return cast(GenericPandasDataFrame, df)

    def fetch_pandas_batches(self, **kwargs: Any) -> "Iterator[GenericPandasDataFrame]":
        """Fetch Pandas dataframes in batches, where 'batch' refers to Snowflake Chunk."""

        return cast(
            Iterator[GenericPandasDataFrame],
            self._raw_cursor.fetch_pandas_batches(**kwargs),
        )

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

    @property
    def _RecordCursor(self):
        import turu.snowflake.record.record_cursor

        return turu.snowflake.record.record_cursor.RecordCursor
