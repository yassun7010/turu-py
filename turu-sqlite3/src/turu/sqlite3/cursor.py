import sqlite3
from collections.abc import Iterable
from typing import TYPE_CHECKING, Generic, Optional, Sequence, Type

import turu.core.cursor
from turu.core.cursor import RowType, map_row
from typing_extensions import Never, Self, override

if TYPE_CHECKING:
    from sqlite3.dbapi2 import _Parameters


class Cursor(
    Generic[RowType],
    turu.core.cursor.Cursor[RowType, "_Parameters"],
):
    def __init__(self, raw_cursor: sqlite3.Cursor, *, row_type: Optional[type] = None):
        self._raw_cursor = raw_cursor
        self._row_type = row_type

    @override
    def execute(
        self, operation: str, parameters: "_Parameters" = ()
    ) -> "Cursor[Never]":
        return Cursor(self._raw_cursor.execute(operation, parameters))

    @override
    def executemany(
        self,
        __sql: str,
        __seq_of_parameters: "Iterable[_Parameters]",
    ) -> "Cursor[Never]":
        return Cursor(self._raw_cursor.executemany(__sql, __seq_of_parameters))

    @override
    def execute_typing(
        self,
        row_type: Type[RowType],
        operation: str,
        parameters: "_Parameters" = (),
    ) -> "Cursor[RowType]":
        return Cursor(
            self._raw_cursor.execute(operation, parameters),
            row_type=row_type,
        )

    @override
    def executemany_typing(
        self,
        row_type: Type[RowType],
        __sql: str,
        __seq_of_parameters: "Iterable[_Parameters]",
    ) -> "Cursor[RowType]":
        return Cursor(
            self._raw_cursor.executemany(__sql, __seq_of_parameters),
            row_type=row_type,
        )

    @override
    def fetchone(self) -> Optional[RowType]:
        row = self._raw_cursor.fetchone()
        if row is None:
            return None
        elif self._row_type is not None:
            return map_row(self._row_type, row)
        else:
            return row

    @override
    def fetchmany(self, size: Optional[int] = 1) -> Sequence[Self]:
        return [
            map_row(self._row_type, row) if self._row_type is not None else row
            for row in self._raw_cursor.fetchmany(size)
        ]

    @override
    def fetchall(self) -> Sequence[Self]:
        return [
            map_row(self._row_type, row) if self._row_type is not None else row
            for row in self._raw_cursor.fetchall()
        ]

    @override
    def __next__(self) -> RowType:
        next_row = next(self._raw_cursor)
        if self._row_type is not None and next_row is not None:
            return map_row(self._row_type, next_row)

        else:
            return next_row


try:
    import turu.mock

    class MockCursor(turu.mock.MockCursor, Cursor):
        pass

except ImportError:
    pass
