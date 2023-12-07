import sqlite3
from collections.abc import Iterable
from typing import TYPE_CHECKING, List, Optional

from typing_extensions import Self, override

import typingsql.cursor

if TYPE_CHECKING:
    from sqlite3.dbapi2 import _Parameters


class Cursor(sqlite3.Cursor, typingsql.cursor.Cursor["_Parameters"]):
    @override
    def execute(self, operation: str, parameters: "_Parameters" = ()) -> "Self":
        return super().execute(operation, parameters)

    @override
    def executemany(
        self, __sql: str, __seq_of_parameters: "Iterable[_Parameters]"
    ) -> Self:
        return super().executemany(__sql, __seq_of_parameters)

    @override
    def fetchone(self) -> Self:
        return super().fetchone()

    @override
    def fetchmany(self, size: Optional[int] = 1) -> List[Self]:
        if size is None:
            return super().fetchmany()
        else:
            return super().fetchmany(size)

    @override
    def fetchall(self) -> List[Self]:
        return super().fetchall()
