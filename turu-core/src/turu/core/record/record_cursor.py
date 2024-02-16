from typing import (
    List,
    Optional,
    Sequence,
    Type,
    cast,
)

import turu.core.cursor
from turu.core.protocols.cursor import Parameters
from turu.core.record.recorder_protcol import RecorderProtcol


class RecordCursor(
    turu.core.cursor.Cursor[turu.core.cursor.GenericRowType, Parameters]
):
    def __init__(
        self,
        recorder: RecorderProtcol,
        cursor: turu.core.cursor.Cursor[turu.core.cursor.GenericRowType, Parameters],
    ):
        self._recorder = recorder
        self.__record_taregt_cursor: turu.core.cursor.Cursor = cursor

    @property
    def rowcount(self) -> int:
        return self.__record_taregt_cursor.rowcount

    @property
    def arraysize(self) -> int:
        return self.__record_taregt_cursor.arraysize

    @arraysize.setter
    def arraysize(self, size: int) -> None:
        self.__record_taregt_cursor.arraysize = size

    def close(self) -> None:
        self.__record_taregt_cursor.close()
        self._recorder.close()

    def execute(
        self, operation: str, parameters: Optional[Parameters] = None, /
    ) -> "RecordCursor[turu.core.cursor.GenericRowType, Parameters]":
        self.__record_taregt_cursor = self.__record_taregt_cursor.execute(
            operation, parameters
        )

        return self

    def executemany(
        self, operation: str, seq_of_parameters: "Sequence[Parameters]", /
    ) -> "RecordCursor[turu.core.cursor.GenericRowType, Parameters]":
        self.__record_taregt_cursor = self.__record_taregt_cursor.executemany(
            operation, seq_of_parameters
        )

        return self

    def execute_map(
        self,
        row_type: Type[turu.core.cursor.GenericNewRowType],
        operation: str,
        parameters: Optional[Parameters] = None,
        /,
    ) -> "RecordCursor[turu.core.cursor.GenericNewRowType, Parameters]":
        self.__record_taregt_cursor = self.__record_taregt_cursor.execute_map(
            row_type, operation, parameters
        )

        return cast(RecordCursor, self)

    def executemany_map(
        self,
        row_type: Type[turu.core.cursor.GenericNewRowType],
        operation: str,
        seq_of_parameters: Sequence[Parameters],
        /,
    ) -> "RecordCursor[turu.core.cursor.GenericNewRowType, Parameters]":
        self.__record_taregt_cursor = self.__record_taregt_cursor.executemany_map(
            row_type, operation, seq_of_parameters
        )

        return cast(RecordCursor, self)

    def fetchone(self) -> Optional[turu.core.cursor.GenericRowType]:
        row = self.__record_taregt_cursor.fetchone()
        if row is not None:
            self._recorder.record([row])

        return row

    def fetchmany(
        self, size: Optional[int] = None
    ) -> List[turu.core.cursor.GenericRowType]:
        rows = self.__record_taregt_cursor.fetchmany(size)

        self._recorder.record(rows)

        return rows

    def fetchall(self) -> List[turu.core.cursor.GenericRowType]:
        rows = self.__record_taregt_cursor.fetchall()

        self._recorder.record(rows)

        return rows

    def __iter__(self) -> "RecordCursor[turu.core.cursor.GenericRowType, Parameters]":
        return self

    def __next__(self) -> turu.core.cursor.GenericRowType:
        row = next(self.__record_taregt_cursor)

        self._recorder.record([row])

        return row

    def __getattr__(self, name):
        def _method_missing(*args):
            return args

        return getattr(self.__record_taregt_cursor, name, _method_missing)
