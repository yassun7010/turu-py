import contextlib
from pathlib import Path
from typing import (
    Generator,
    List,
    Optional,
    Sequence,
    Type,
    TypeVar,
    Union,
    cast,
)

import turu.core.cursor
from turu.core.protocols.cursor import Parameters
from turu.core.recorders.csv_recorder import CSVRecorder
from turu.core.recorders.recorder_protcol import RecorderProtcol
from typing_extensions import NotRequired, TypedDict, Unpack

GenericCursor = TypeVar("GenericCursor", bound=turu.core.cursor.Cursor)


class RecordCsvOptions(TypedDict):
    has_header: NotRequired[bool]
    enable: NotRequired[bool]
    rowsize: NotRequired[int]


class _RecordCursor(turu.core.cursor.Cursor[turu.core.cursor.RowType, Parameters]):
    def __init__(
        self,
        recorder: RecorderProtcol,
        cursor: turu.core.cursor.Cursor[turu.core.cursor.RowType, Parameters],
        **options: Unpack[RecordCsvOptions],
    ):
        self._recorder = recorder
        self._cursor = cursor
        self._options = options

    @property
    def rowcount(self) -> int:
        return self._cursor.rowcount

    @property
    def arraysize(self) -> int:
        return self._cursor.arraysize

    @arraysize.setter
    def arraysize(self, size: int) -> None:
        self._cursor.arraysize = size

    def close(self) -> None:
        self._cursor.close()
        self._recorder.close()

    def execute(
        self, operation: str, parameters: Optional[Parameters] = None, /
    ) -> "_RecordCursor[turu.core.cursor.RowType, Parameters]":
        self._cursor = self._cursor.execute(operation, parameters)

        return self

    def executemany(
        self, operation: str, seq_of_parameters: "Sequence[Parameters]", /
    ) -> "_RecordCursor[turu.core.cursor.RowType, Parameters]":
        self._cursor = self._cursor.executemany(operation, seq_of_parameters)

        return self

    def execute_map(
        self,
        row_type: Type[turu.core.cursor.NewRowType],
        operation: str,
        parameters: Optional[Parameters] = None,
        /,
    ) -> "_RecordCursor[turu.core.cursor.NewRowType, Parameters]":
        self._cursor = cast(
            turu.core.cursor.Cursor,
            self._cursor.execute_map(row_type, operation, parameters),
        )

        return cast(_RecordCursor, self)

    def executemany_map(
        self,
        row_type: Type[turu.core.cursor.NewRowType],
        operation: str,
        seq_of_parameters: Sequence[Parameters],
        /,
    ) -> "_RecordCursor[turu.core.cursor.NewRowType, Parameters]":
        self._cursor = cast(
            turu.core.cursor.Cursor,
            self._cursor.executemany_map(row_type, operation, seq_of_parameters),
        )

        return cast(_RecordCursor, self)

    def fetchone(self) -> Optional[turu.core.cursor.RowType]:
        row = self._cursor.fetchone()
        if row is not None:
            self._recorder.write_row(row)

        return row

    def fetchmany(self, size: Optional[int] = None) -> List[turu.core.cursor.RowType]:
        rows = self._cursor.fetchmany(size)

        for row in rows:
            self._recorder.write_row(row)

        return rows

    def fetchall(self) -> List[turu.core.cursor.RowType]:
        rows = self._cursor.fetchall()

        for row in rows:
            self._recorder.write_row(row)

        return rows

    def __iter__(self) -> "_RecordCursor[turu.core.cursor.RowType, Parameters]":
        return self

    def __next__(self) -> turu.core.cursor.RowType:
        row = next(self._cursor)

        self._recorder.write_row(row)

        return row


@contextlib.contextmanager
def record_as_csv(
    record_filepath: Union[str, Path],
    cursor: GenericCursor,
    **options: Unpack[RecordCsvOptions],
) -> Generator[GenericCursor, None, None]:
    cursor = cast(
        GenericCursor,
        _RecordCursor(CSVRecorder(record_filepath), cursor, **options),
    )

    try:
        yield cursor

    finally:
        cursor.close()
