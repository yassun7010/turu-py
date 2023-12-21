from typing import Any, List, Optional, Sequence, Tuple, Type, TypedDict, cast

import turu.core.cursor
import turu.core.mock
from typing_extensions import Unpack, override

import snowflake.connector


class ExecuteOptions(TypedDict, total=False):
    timeout: int
    """timeout[sec]"""

    num_statements: int


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
        self._row_type = cast(turu.core.cursor.GenericRowType, row_type)

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
        self._row_type = cast(turu.core.cursor.GenericRowType, row_type)

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


class MockCursor(  # type: ignore
    turu.core.mock.MockCursor[turu.core.cursor.GenericRowType, Any],  # type: ignore
    Cursor[turu.core.cursor.GenericRowType],  # type: ignore
):
    @override
    def execute(
        self,
        operation: str,
        parameters: Optional[Any] = None,
        /,
        **options: Unpack[ExecuteOptions],
    ) -> "MockCursor[Tuple[Any]]":
        return cast(MockCursor, super().execute(operation, parameters))

    @override
    def executemany(
        self,
        operation: str,
        seq_of_parameters: Sequence[Any],
        /,
        **options: Unpack[ExecuteOptions],
    ) -> "MockCursor[Tuple[Any]]":
        return cast(MockCursor, super().executemany(operation, seq_of_parameters))

    @override
    def execute_map(
        self,
        row_type: Type[turu.core.cursor.GenericNewRowType],
        operation: str,
        parameters: Optional[Any] = None,
        /,
        **options: Unpack[ExecuteOptions],
    ) -> "MockCursor[turu.core.cursor.GenericNewRowType]":
        return cast(MockCursor, super().execute_map(row_type, operation, parameters))

    @override
    def executemany_map(
        self,
        row_type: Type[turu.core.cursor.GenericNewRowType],
        operation: str,
        seq_of_parameters: Sequence[Any],
        /,
        **options: Unpack[ExecuteOptions],
    ) -> "MockCursor[turu.core.cursor.GenericNewRowType]":
        return cast(
            MockCursor, super().executemany_map(row_type, operation, seq_of_parameters)
        )
