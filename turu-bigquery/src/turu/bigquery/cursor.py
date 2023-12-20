from typing import Any, List, Mapping, Optional, Sequence, Type, Union, cast

import google.cloud.bigquery
import google.cloud.bigquery.dbapi
import turu.core.cursor
from typing_extensions import Never, deprecated, override


class Cursor(turu.core.cursor.Cursor[turu.core.cursor.GenericRowType, Any]):
    def __init__(
        self,
        cursor: google.cloud.bigquery.dbapi.Cursor,
        *,
        row_type: Optional[Type[turu.core.cursor.GenericRowType]] = None,
    ):
        self._raw_cursor = cursor
        self._row_type: Optional[Type[turu.core.cursor.GenericRowType]] = row_type

    @property
    def rowcount(self) -> int:
        return self._raw_cursor.rowcount

    @property
    @deprecated("arraysize is not supported in BigQuery")
    def arraysize(self) -> Never:
        raise NotImplementedError()

    @arraysize.setter
    @deprecated("arraysize is not supported in BigQuery")
    def arraysize(self, size: int) -> None:
        raise NotImplementedError()

    @override
    def close(self) -> None:
        self._raw_cursor.close()

    @override
    def execute(
        self,
        operation: str,
        parameters: Optional[Union[Mapping[str, Any], Sequence[Any]]] = None,
        /,
    ) -> "Cursor[Any]":
        self._raw_cursor.execute(operation, parameters)
        self._row_type = None

        return self

    @override
    def executemany(
        self,
        operation: str,
        seq_of_parameters: Union[Sequence[Mapping[str, Any]], Sequence[Any]],
        /,
    ) -> "Cursor[Any]":
        self._raw_cursor.executemany(operation, seq_of_parameters)
        self._row_type = None

        return self

    @override
    def execute_map(
        self,
        row_type: Type[turu.core.cursor.GenericNewRowType],
        operation: str,
        parameters: "Optional[Union[Mapping[str, Any], Sequence[Any]]]" = None,
        /,
    ) -> "Cursor[turu.core.cursor.GenericNewRowType]":
        self._raw_cursor.execute(operation, parameters)
        self._row_type = cast(turu.core.cursor.GenericRowType, row_type)

        return cast(Cursor, self)

    @override
    def executemany_map(
        self,
        row_type: Type[turu.core.cursor.GenericNewRowType],
        operation: str,
        seq_of_parameters: Union[Sequence[Mapping[str, Any]], Sequence[Any]],
        /,
    ) -> "Cursor[turu.core.cursor.GenericNewRowType]":
        self._raw_cursor.executemany(operation, seq_of_parameters)
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
        if self._row_type is not None and next_row is not None:
            return turu.core.cursor.map_row(self._row_type, next_row)

        else:
            return next_row  # type: ignore[return-value]
