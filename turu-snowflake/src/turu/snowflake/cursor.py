from typing import Any, Generic, List, Optional, Sequence, Type, cast

import turu.core.cursor
from turu.core.protocols.cursor import Parameters
from typing_extensions import override

import snowflake.connector


class Cursor(
    Generic[turu.core.cursor.RowType, Parameters],
    turu.core.cursor.Cursor[turu.core.cursor.RowType, Any],
):
    def __init__(
        self,
        raw_cursor: snowflake.connector.cursor.SnowflakeCursor,
        *,
        row_type: Any = None,
    ) -> None:
        self._raw_cursor = raw_cursor
        self._row_type = row_type

    @override
    def execute(
        self, operation: str, parameters: "Optional[Any]" = None
    ) -> "Cursor[Any, Parameters]":
        return Cursor(
            cast(
                snowflake.connector.cursor.SnowflakeCursor,
                self._raw_cursor.execute(operation, parameters),
            )
        )

    @override
    def executemany(
        self,
        operation: str,
        seq_of_parameters: "Sequence[Any]",
    ) -> "Cursor[Any, Parameters]":
        return Cursor(self._raw_cursor.executemany(operation, seq_of_parameters))

    @override
    def execute_map(
        self,
        row_type: Type[turu.core.cursor.NewRowType],
        operation: str,
        parameters: "Optional[Any]" = None,
    ) -> "Cursor[turu.core.cursor.NewRowType, Parameters]":
        return Cursor(
            cast(
                snowflake.connector.cursor.SnowflakeCursor,
                self._raw_cursor.execute(operation, parameters),
            ),
            row_type=row_type,
        )

    @override
    def executemany_map(
        self,
        row_type: Type[turu.core.cursor.NewRowType],
        operation: str,
        seq_of_parameters: "Sequence[Parameters]",
    ) -> "Cursor[turu.core.cursor.NewRowType, Parameters]":
        return Cursor(
            cast(
                snowflake.connector.cursor.SnowflakeCursor,
                self._raw_cursor.executemany(operation, seq_of_parameters),
            ),
            row_type=row_type,
        )

    @override
    def fetchone(self) -> Optional[turu.core.cursor.RowType]:
        row = self._raw_cursor.fetchone()
        if row is None:
            return None

        elif self._row_type is not None:
            return turu.core.cursor.map_row(self._row_type, row)

        else:
            return row  # type: ignore[return-value]

    @override
    def fetchmany(self, size: int = 1) -> List[turu.core.cursor.RowType]:
        return [
            turu.core.cursor.map_row(self._row_type, row)
            for row in self._raw_cursor.fetchmany(size)
        ]

    @override
    def fetchall(self) -> List[turu.core.cursor.RowType]:
        return [
            turu.core.cursor.map_row(self._row_type, row)
            for row in self._raw_cursor.fetchall()
        ]

    @override
    def __next__(self) -> turu.core.cursor.RowType:
        next_row = self._raw_cursor.fetchone()
        if self._row_type is not None and next_row is not None:
            return turu.core.cursor.map_row(self._row_type, next_row)

        else:
            return next_row  # type: ignore[return-value]


try:
    import turu.mock  # type: ignore

    class MockCursor(  # type: ignore
        Generic[turu.core.cursor.RowType],  # type: ignore
        turu.mock.MockCursor[turu.core.cursor.RowType, Any],  # type: ignore
        Cursor[turu.core.cursor.RowType, Any],  # type: ignore
    ):
        pass

except ImportError:
    pass
