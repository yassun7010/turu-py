import sqlite3
from typing import TYPE_CHECKING, Any, List, Optional, Sequence, Type, cast

import turu.core.cursor
from typing_extensions import override

if TYPE_CHECKING:
    from sqlite3.dbapi2 import _Parameters


class Cursor(
    turu.core.cursor.Cursor[turu.core.cursor.RowType, "_Parameters"],
):
    def __init__(
        self,
        raw_cursor: sqlite3.Cursor,
        *,
        row_type: Optional[Type[turu.core.cursor.RowType]] = None,
    ):
        self._raw_cursor = raw_cursor
        self._row_type: Optional[Type[turu.core.cursor.RowType]] = row_type

    @property
    def rowcount(self) -> int:
        return self._raw_cursor.rowcount

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
        self, operation: str, parameters: Optional["_Parameters"] = None, /
    ) -> "Cursor[Any]":
        self._raw_cursor.execute(operation, parameters or ())
        self._row_type = None

        return cast(Cursor, self)

    @override
    def executemany(
        self, operation: str, seq_of_parameters: "Sequence[_Parameters]", /
    ) -> "Cursor[Any]":
        self._raw_cursor.executemany(operation, seq_of_parameters)
        self._row_type = None

        return cast(Cursor, self)

    @override
    def execute_map(
        self,
        row_type: Type[turu.core.cursor.NewRowType],
        operation: str,
        parameters: "Optional[_Parameters]" = None,
        /,
    ) -> "Cursor[turu.core.cursor.NewRowType]":
        self._raw_cursor.execute(operation, parameters or ())
        self._row_type = cast(turu.core.cursor.RowType, row_type)

        return cast(Cursor, self)

    @override
    def executemany_map(
        self,
        row_type: Type[turu.core.cursor.NewRowType],
        operation: str,
        seq_of_parameters: "Sequence[_Parameters]",
        /,
    ) -> "Cursor[turu.core.cursor.NewRowType]":
        self._raw_cursor.executemany(operation, seq_of_parameters)
        self._row_type = cast(turu.core.cursor.RowType, row_type)

        return cast(Cursor, self)

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
    def fetchmany(self, size: Optional[int] = None) -> List[turu.core.cursor.RowType]:
        return [
            turu.core.cursor.map_row(self._row_type, row)
            for row in (
                self._raw_cursor.fetchmany(size if size is not None else self.arraysize)
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
        turu.mock.MockCursor[turu.core.cursor.RowType, "_Parameters"],
        Cursor[turu.core.cursor.RowType],
    ):
        pass

except ImportError:
    pass
