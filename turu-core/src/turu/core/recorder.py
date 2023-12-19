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
from turu.core.exception import TuruUnexpectedFetchError
from turu.core.protocols.cursor import Parameters
from typing_extensions import NotRequired, TypedDict, Unpack, deprecated

GenericCursor = TypeVar("GenericCursor", bound=turu.core.cursor.Cursor)


class RecordOptions(TypedDict):
    has_header: NotRequired[bool]
    enable: NotRequired[bool]
    rowsize: NotRequired[int]


class RecordCursor(turu.core.cursor.Cursor[turu.core.cursor.RowType, Parameters]):
    def __init__(
        self,
        record_filepath: Union[str, Path],
        cursor: turu.core.cursor.Cursor[turu.core.cursor.RowType, Parameters],
        **options: Unpack[RecordOptions],
    ):
        record_filepath = Path(record_filepath)
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

    def execute(
        self, operation: str, parameters: Optional[Parameters] = None, /
    ) -> "RecordCursor[turu.core.cursor.RowType, Parameters]":
        return cast(
            RecordCursor,
            self._cursor.execute(operation, parameters),
        )

    def executemany(
        self, operation: str, seq_of_parameters: "Sequence[Parameters]", /
    ) -> "RecordCursor[turu.core.cursor.RowType, Parameters]":
        return cast(
            RecordCursor,
            self._cursor.executemany(operation, seq_of_parameters),
        )

    def execute_map(
        self,
        row_type: Type[turu.core.cursor.NewRowType],
        operation: str,
        parameters: Optional[Parameters] = None,
        /,
    ) -> "turu.core.cursor.Cursor[turu.core.cursor.NewRowType, Parameters]":
        return self._cursor.execute_map(row_type, operation, parameters)

    def executemany_map(
        self,
        row_type: Type[turu.core.cursor.NewRowType],
        operation: str,
        seq_of_parameters: Sequence[Parameters],
        /,
    ) -> "turu.core.cursor.Cursor[turu.core.cursor.NewRowType, Parameters]":
        return self._cursor.executemany_map(row_type, operation, seq_of_parameters)

    @deprecated("Use execute_*() first.")
    def fetchone(self) -> Optional[turu.core.cursor.RowType]:
        raise TuruUnexpectedFetchError()

    @deprecated("Use execute_*() first.")
    def fetchmany(self, size: Optional[int] = None) -> List[turu.core.cursor.RowType]:
        raise TuruUnexpectedFetchError()

    @deprecated("Use execute_*() first.")
    def fetchall(self) -> List[turu.core.cursor.RowType]:
        raise TuruUnexpectedFetchError()

    @deprecated("Use execute_*() first.")
    def __iter__(self) -> "RecordCursor[turu.core.cursor.RowType, Parameters]":
        raise TuruUnexpectedFetchError()

    @deprecated("Use execute_*() first.")
    def __next__(self) -> turu.core.cursor.RowType:
        raise TuruUnexpectedFetchError()


@contextlib.contextmanager
def record(
    record_filepath: Union[str, Path],
    cursor: GenericCursor,
    **options: Unpack[RecordOptions],
) -> Generator[GenericCursor, None, None]:
    yield cast(GenericCursor, RecordCursor(record_filepath, cursor, **options))
