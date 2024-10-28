from typing import Any, List, Mapping, Optional, Sequence, Type, Union, cast

import psycopg
import psycopg.cursor
import turu.core.cursor
import turu.core.mock
import turu.core.tag
from typing_extensions import LiteralString, Never, override

Parameters = Union[Sequence[Any], Mapping[str, Any]]


class Cursor(
    turu.core.cursor.Cursor[turu.core.cursor.GenericRowType, Parameters],
):
    def __init__(
        self,
        cursor: psycopg.Cursor,
        *,
        row_type: Optional[Type[turu.core.cursor.GenericRowType]] = None,
    ):
        self._raw_cursor = cursor
        self._row_type: Optional[Type[turu.core.cursor.GenericRowType]] = row_type
        self._iter = None

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
        self,
        operation: str,
        parameters: Optional[Parameters] = None,
        /,
    ) -> "Cursor[turu.core.cursor.GenericRowType]":
        self._raw_cursor.execute(cast(LiteralString, operation), parameters)
        self._row_type = None

        return self

    @override
    def executemany(
        self,
        operation: str,
        seq_of_parameters: Sequence[Parameters],
        /,
    ) -> "Cursor[turu.core.cursor.GenericRowType]":
        self._raw_cursor.executemany(cast(LiteralString, operation), seq_of_parameters)
        self._row_type = None

        return self

    @override
    def execute_map(
        self,
        row_type: Type[turu.core.cursor.GenericNewRowType],
        operation: str,
        parameters: Optional[Parameters] = None,
        /,
    ) -> "Cursor[turu.core.cursor.GenericNewRowType]":
        self._raw_cursor.execute(cast(LiteralString, operation), parameters)
        self._row_type = cast(Type[turu.core.cursor.GenericRowType], row_type)

        return cast(Cursor, self)

    @override
    def executemany_map(
        self,
        row_type: Type[turu.core.cursor.GenericNewRowType],
        operation: str,
        seq_of_parameters: Sequence[Parameters],
        /,
    ) -> "Cursor[turu.core.cursor.GenericNewRowType]":
        self._raw_cursor.executemany(cast(LiteralString, operation), seq_of_parameters)
        self._row_type = cast(Type[turu.core.cursor.GenericRowType], row_type)

        return cast(Cursor, self)

    @override
    def execute_with_tag(
        self,
        tag: Type[turu.core.tag.Tag],
        operation: str,
        parameters: "Optional[Parameters]" = None,
    ) -> "Cursor[Never]":
        return cast(Cursor, self.execute(operation, parameters))

    @override
    def executemany_with_tag(
        self,
        tag: Type[turu.core.tag.Tag],
        operation: str,
        seq_of_parameters: "Sequence[Parameters]",
    ) -> "Cursor[Never]":
        return cast(Cursor, self.executemany(operation, seq_of_parameters))

    @override
    def fetchone(self) -> Optional[turu.core.cursor.GenericRowType]:
        row = self._raw_cursor.fetchone()
        if row is None:
            return None

        elif self._row_type is not None:
            return turu.core.cursor.map_row(self._row_type, row)

        else:
            return row  # type: ignore

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
    def __iter__(self) -> "Cursor[turu.core.cursor.GenericRowType]":
        self._iter = self._raw_cursor.__iter__()
        return self

    @override
    def __next__(self) -> turu.core.cursor.GenericRowType:
        if self._iter is None:
            self._iter = self._raw_cursor.__iter__()

        next_row = next(self._iter)
        if self._row_type is not None and next_row is not None:
            return turu.core.cursor.map_row(self._row_type, next_row)

        else:
            return next_row  # type: ignore
