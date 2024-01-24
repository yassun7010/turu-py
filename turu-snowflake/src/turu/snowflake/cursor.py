from typing import (
    Any,
    Iterator,
    List,
    Optional,
    Sequence,
    Tuple,
    Type,
    TypedDict,
    cast,
)

import turu.core.cursor
import turu.core.mock
from turu.snowflake.features import PandasDataFlame
from typing_extensions import Self, Unpack, override

import snowflake.connector


class ExecuteOptions(TypedDict, total=False):
    timeout: int
    """timeout[sec]"""

    num_statements: int
    """number of statements"""


class Cursor(
    turu.core.cursor.Cursor[turu.core.cursor.GenericRowType, Any],
):
    def __init__(
        self,
        cursor: snowflake.connector.cursor.SnowflakeCursor,
        *,
        row_type: Optional[Type[turu.core.cursor.GenericRowType]] = None,
    ) -> None:
        self._raw_cursor = cursor
        self._row_type: Optional[Type[turu.core.cursor.GenericRowType]] = row_type

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
    ) -> "Cursor[Tuple[Any]]":
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
    ) -> "Cursor[Tuple[Any]]":
        self._raw_cursor.executemany(operation, seq_of_parameters, **options)
        self._row_type = None

        return cast(Cursor, self)

    @override
    def execute_map(
        self,
        row_type: Type[turu.core.cursor.GenericNewRowType],
        operation: str,
        parameters: "Optional[Any]" = None,
        /,
        **options: Unpack[ExecuteOptions],
    ) -> "Cursor[turu.core.cursor.GenericNewRowType]":
        self._raw_cursor.execute(operation, parameters, **options)
        self._row_type = cast(Type[turu.core.cursor.GenericRowType], row_type)

        return cast(Cursor, self)

    @override
    def executemany_map(
        self,
        row_type: Type[turu.core.cursor.GenericNewRowType],
        operation: str,
        seq_of_parameters: "Sequence[Any]",
        /,
        **options: Unpack[ExecuteOptions],
    ) -> "Cursor[turu.core.cursor.GenericNewRowType]":
        self._raw_cursor.executemany(operation, seq_of_parameters, **options)
        self._row_type = cast(Type[turu.core.cursor.GenericRowType], row_type)

        return cast(Cursor, self)

    @override
    def fetchone(self) -> Optional[turu.core.cursor.GenericRowType]:
        row = self._raw_cursor.fetchone()
        if row is None:
            return None

        elif self._row_type is not None:
            return turu.core.cursor.map_row(self._row_type, row)

        else:
            return row  # type: ignore[return-value]

    @override
    def fetchmany(
        self, size: Optional[int] = None
    ) -> List[turu.core.cursor.GenericRowType]:
        return [
            turu.core.cursor.map_row(self._row_type, row)
            for row in self._raw_cursor.fetchmany(
                size if size is not None else self.arraysize
            )
        ]

    @override
    def fetchall(self) -> List[turu.core.cursor.GenericRowType]:
        return [
            turu.core.cursor.map_row(self._row_type, row)
            for row in self._raw_cursor.fetchall()
        ]

    @override
    def __next__(self) -> turu.core.cursor.GenericRowType:
        next_row = self._raw_cursor.fetchone()

        if next_row is None:
            raise StopIteration()

        if self._row_type is not None:
            return turu.core.cursor.map_row(self._row_type, next_row)

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

    def fetch_arrow_all(self):
        return self._raw_cursor.fetch_arrow_all()

    def fetch_arrow_batches(self):
        return self._raw_cursor.fetch_arrow_batches()

    def fetch_pandas_all(self, **kwargs) -> "PandasDataFlame":
        """Fetch Pandas dataframes in batches, where 'batch' refers to Snowflake Chunk."""

        return self._raw_cursor.fetch_pandas_all(**kwargs)

    def fetch_pandas_batches(self, **kwargs) -> "Iterator[PandasDataFlame]":
        """Fetches a single Arrow Table."""

        return self._raw_cursor.fetch_pandas_batches(**kwargs)

    @property
    def _RecordCursor(self):
        import turu.snowflake.record.record_cursor

        return turu.snowflake.record.record_cursor.RecordCursor
