import sqlite3
from collections.abc import Iterable
from typing import TYPE_CHECKING, Any, Generic, Optional, Sequence, Type

import turu.core.cursor
from turu.core.cursor import NewRowType, RowType, map_row
from typing_extensions import override

if TYPE_CHECKING:
    from sqlite3.dbapi2 import _Parameters


class Cursor(
    Generic[RowType],
    turu.core.cursor.Cursor[RowType, "_Parameters"],
):
    def __init__(
        self,
        raw_cursor: sqlite3.Cursor,
        *,
        row_type: Optional[Type[RowType]] = None,
    ):
        self._raw_cursor = raw_cursor
        self._row_type = row_type

    @override
    def execute(
        self, operation: str, parameters: "Optional[_Parameters]" = None
    ) -> "Cursor[Any]":
        return Cursor(self._raw_cursor.execute(operation, parameters or ()))

    @override
    def executemany(
        self,
        operation: str,
        seq_of_parameters: "Iterable[_Parameters]",
    ) -> "Cursor[Any]":
        return Cursor(self._raw_cursor.executemany(operation, seq_of_parameters))

    @override
    def execute_map(
        self,
        row_type: Type[NewRowType],
        operation: str,
        parameters: "Optional[_Parameters]" = None,
    ) -> "Cursor[NewRowType]":
        return Cursor(
            self._raw_cursor.execute(operation, parameters or ()),
            row_type=row_type,
        )

    @override
    def executemany_map(
        self,
        row_type: Type[NewRowType],
        operation: str,
        seq_of_parameters: "Iterable[_Parameters]",
    ) -> "Cursor[NewRowType]":
        return Cursor(
            self._raw_cursor.executemany(operation, seq_of_parameters),
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
    def fetchmany(self, size: Optional[int] = 1) -> Sequence[RowType]:
        return [
            map_row(self._row_type, row) for row in self._raw_cursor.fetchmany(size)
        ]

    @override
    def fetchall(self) -> Sequence[RowType]:
        return [map_row(self._row_type, row) for row in self._raw_cursor.fetchall()]

    @override
    def __next__(self) -> RowType:
        next_row = next(self._raw_cursor)
        if self._row_type is not None and next_row is not None:
            return map_row(self._row_type, next_row)

        else:
            return next_row


try:
    import turu.mock

    class MockCursor(
        Generic[RowType], turu.mock.MockCursor[RowType, "_Parameters"], Cursor[RowType]
    ):
        pass

except ImportError:
    pass
