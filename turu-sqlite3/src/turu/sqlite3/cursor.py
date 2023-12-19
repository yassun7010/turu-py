import sqlite3
from typing import TYPE_CHECKING, Any, Generic, List, Optional, Sequence, Type

import turu.core.cursor
from typing_extensions import override

if TYPE_CHECKING:
    from sqlite3.dbapi2 import _Parameters


class Cursor(
    Generic[turu.core.cursor.RowType],
    turu.core.cursor.Cursor[turu.core.cursor.RowType, "_Parameters"],
):
    def __init__(
        self,
        raw_cursor: sqlite3.Cursor,
        *,
        row_type: Optional[Type[turu.core.cursor.RowType]] = None,
    ):
        self._raw_cursor = raw_cursor
        self._row_type = row_type

    @property
    def rowcount(self) -> int:
        return self._raw_cursor.rowcount

    @override
    def close(self) -> None:
        self._raw_cursor.close()

    @override
    def execute(
        self, operation: str, parameters: "Optional[_Parameters]" = None
    ) -> "Cursor[Any]":
        return Cursor(self._raw_cursor.execute(operation, parameters or ()))

    @override
    def executemany(
        self,
        operation: str,
        seq_of_parameters: "Sequence[_Parameters]",
    ) -> "Cursor[Any]":
        return Cursor(self._raw_cursor.executemany(operation, seq_of_parameters))

    @override
    def execute_map(
        self,
        row_type: Type[turu.core.cursor.NewRowType],
        operation: str,
        parameters: "Optional[_Parameters]" = None,
    ) -> "Cursor[turu.core.cursor.NewRowType]":
        return Cursor(
            self._raw_cursor.execute(operation, parameters or ()),
            row_type=row_type,
        )

    @override
    def executemany_map(
        self,
        row_type: Type[turu.core.cursor.NewRowType],
        operation: str,
        seq_of_parameters: "Sequence[_Parameters]",
    ) -> "Cursor[turu.core.cursor.NewRowType]":
        return Cursor(
            self._raw_cursor.executemany(operation, seq_of_parameters),
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
            return row

    @override
    def fetchmany(self, size: int = 1) -> List[turu.core.cursor.RowType]:
        return [
            turu.core.cursor.map_row(self._row_type, row)
            for row in (
                self._raw_cursor.fetchmany(size)
                if size is not None
                else self._raw_cursor.fetchmany()
            )
        ]

    @override
    def fetchall(self) -> List[turu.core.cursor.RowType]:
        return [
            turu.core.cursor.map_row(self._row_type, row)
            for row in self._raw_cursor.fetchall()
        ]

    @override
    def __next__(self) -> turu.core.cursor.RowType:
        next_row = next(self._raw_cursor)
        if self._row_type is not None and next_row is not None:
            return turu.core.cursor.map_row(self._row_type, next_row)

        else:
            return next_row


try:
    import turu.mock

    class MockCursor(  # type: ignore
        Generic[turu.core.cursor.RowType],
        turu.mock.MockCursor[turu.core.cursor.RowType, "_Parameters"],
        Cursor[turu.core.cursor.RowType],
    ):
        pass

except ImportError:
    pass
