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
from turu.core.record.csv_recorder import CsvRecorder, CsvRecorderOptions
from turu.core.record.recorder_protcol import RecorderProtcol
from typing_extensions import Unpack

GenericCursor = TypeVar("GenericCursor", bound=turu.core.cursor.Cursor)


class _RecordCursor(
    turu.core.cursor.Cursor[turu.core.cursor.GenericRowType, Parameters]
):
    def __init__(
        self,
        recorder: RecorderProtcol,
        cursor: turu.core.cursor.Cursor[turu.core.cursor.GenericRowType, Parameters],
    ):
        self._recorder = recorder
        self._cursor = cursor

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
    ) -> "_RecordCursor[turu.core.cursor.GenericRowType, Parameters]":
        self._cursor = self._cursor.execute(operation, parameters)

        return self

    def executemany(
        self, operation: str, seq_of_parameters: "Sequence[Parameters]", /
    ) -> "_RecordCursor[turu.core.cursor.GenericRowType, Parameters]":
        self._cursor = self._cursor.executemany(operation, seq_of_parameters)

        return self

    def execute_map(
        self,
        row_type: Type[turu.core.cursor.GenericNewRowType],
        operation: str,
        parameters: Optional[Parameters] = None,
        /,
    ) -> "_RecordCursor[turu.core.cursor.GenericNewRowType, Parameters]":
        self._cursor = cast(
            turu.core.cursor.Cursor,
            self._cursor.execute_map(row_type, operation, parameters),
        )

        return cast(_RecordCursor, self)

    def executemany_map(
        self,
        row_type: Type[turu.core.cursor.GenericNewRowType],
        operation: str,
        seq_of_parameters: Sequence[Parameters],
        /,
    ) -> "_RecordCursor[turu.core.cursor.GenericNewRowType, Parameters]":
        self._cursor = cast(
            turu.core.cursor.Cursor,
            self._cursor.executemany_map(row_type, operation, seq_of_parameters),
        )

        return cast(_RecordCursor, self)

    def fetchone(self) -> Optional[turu.core.cursor.GenericRowType]:
        row = self._cursor.fetchone()
        if row is not None:
            self._recorder.write_row(row)

        return row

    def fetchmany(
        self, size: Optional[int] = None
    ) -> List[turu.core.cursor.GenericRowType]:
        rows = self._cursor.fetchmany(size)

        for row in rows:
            self._recorder.write_row(row)

        return rows

    def fetchall(self) -> List[turu.core.cursor.GenericRowType]:
        rows = self._cursor.fetchall()

        for row in rows:
            self._recorder.write_row(row)

        return rows

    def __iter__(self) -> "_RecordCursor[turu.core.cursor.GenericRowType, Parameters]":
        return self

    def __next__(self) -> turu.core.cursor.GenericRowType:
        row = next(self._cursor)

        self._recorder.write_row(row)

        return row

    def __getattr__(self, name):
        def _method_missing(*args):
            return args

        return getattr(self._cursor, name, _method_missing)


@contextlib.contextmanager
def record_as_csv(
    record_filepath: Union[str, Path],
    cursor: GenericCursor,
    *,
    enable: Union[str, bool, None] = True,
    **options: Unpack[CsvRecorderOptions],
) -> Generator[GenericCursor, None, None]:
    if isinstance(enable, str):
        enable = enable.lower() == "true"

    if enable:
        # NOTE: hack to get original cursor type hint.
        cursor = cast(
            GenericCursor,
            _RecordCursor(
                CsvRecorder(record_filepath, **options),
                cursor,
            ),
        )

    try:
        yield cursor

    finally:
        cursor.close()
