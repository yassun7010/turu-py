from contextlib import _AsyncGeneratorContextManager, _GeneratorContextManager
from pathlib import Path
from typing import (
    AsyncGenerator,
    Generator,
    Union,
    cast,
    overload,
)

import turu.core.async_cursor
import turu.core.cursor
from turu.core.record.async_record_cursor import AsyncRecordCursor as AsyncRecordCursor
from turu.core.record.csv_recorder import CsvRecorder as CsvRecorder
from turu.core.record.csv_recorder import CsvRecorderOptions as CsvRecorderOptions
from turu.core.record.record_cursor import RecordCursor as RecordCursor
from typing_extensions import Unpack, deprecated


@overload
def record_to_csv(
    record_filepath: Union[str, Path],
    cursor: turu.core.cursor.GenericCursor,
    *,
    enable: Union[str, bool, None] = True,
    **options: Unpack[CsvRecorderOptions],
) -> "_GeneratorContextManager[turu.core.cursor.GenericCursor]":
    ...


@overload
def record_to_csv(
    record_filepath: Union[str, Path],
    cursor: turu.core.async_cursor.GenericAsyncCursor,
    *,
    enable: Union[str, bool, None] = True,
    **options: Unpack[CsvRecorderOptions],
) -> _AsyncGeneratorContextManager[turu.core.async_cursor.GenericAsyncCursor]:
    ...


def record_to_csv(  # type: ignore
    record_filepath: Union[str, Path],
    cursor: Union[
        turu.core.cursor.GenericCursor, turu.core.async_cursor.GenericAsyncCursor
    ],
    *,
    enable: Union[str, bool, None] = True,
    **options: Unpack[CsvRecorderOptions],
):
    """Records cursor's fetched data to CSV.

    Parameters:
        record_filepath: Path to record file.
        cursor: Cursor to record.
        enable: Enable recording.
        options: Options for CSV recorder.
    """
    if isinstance(cursor, turu.core.cursor.Cursor):
        return _GeneratorContextManager(
            _record_to_csv, (record_filepath, cursor), dict(enable=enable, **options)
        )

    elif isinstance(cursor, turu.core.async_cursor.AsyncCursor):
        return _AsyncGeneratorContextManager(
            _record_to_csv_async,
            (record_filepath, cursor),
            dict(enable=enable, **options),
        )

    raise NotImplementedError(f"cursor type {type(cursor)} is not supported")


def _record_to_csv(
    record_filepath: Union[str, Path],
    cursor: turu.core.cursor.GenericCursor,
    *,
    enable: Union[str, bool, None] = True,
    **options: Unpack[CsvRecorderOptions],
) -> Generator[turu.core.cursor.GenericCursor, None, None]:
    if isinstance(enable, str):
        enable = enable.lower() == "true"

    if enable:
        # NOTE: hack to get original cursor type hint.
        cursor = cast(
            turu.core.cursor.GenericCursor,
            getattr(cursor, "_RecordCursor", RecordCursor)(
                CsvRecorder(record_filepath, **options),
                cursor,
            ),
        )

    try:
        yield cursor

    finally:
        cursor.close()


async def _record_to_csv_async(
    record_filepath: Union[str, Path],
    cursor: turu.core.async_cursor.GenericAsyncCursor,
    *,
    enable: Union[str, bool, None] = True,
    **options: Unpack[CsvRecorderOptions],
) -> AsyncGenerator[turu.core.async_cursor.GenericAsyncCursor, None]:
    if isinstance(enable, str):
        enable = enable.lower() == "true"

    if enable:
        # NOTE: hack to get original cursor type hint.
        cursor = cast(
            turu.core.async_cursor.GenericAsyncCursor,
            getattr(cursor, "_AsyncRecordCursor", AsyncRecordCursor)(
                CsvRecorder(record_filepath, **options),
                cursor,
            ),
        )

    try:
        yield cursor

    finally:
        await cursor.close()


@overload
def record_as_csv(
    record_filepath: Union[str, Path],
    cursor: turu.core.cursor.GenericCursor,
    *,
    enable: Union[str, bool, None] = True,
    **options: Unpack[CsvRecorderOptions],
) -> "_GeneratorContextManager[turu.core.cursor.GenericCursor]":
    ...


@overload
def record_as_csv(
    record_filepath: Union[str, Path],
    cursor: turu.core.async_cursor.GenericAsyncCursor,
    *,
    enable: Union[str, bool, None] = True,
    **options: Unpack[CsvRecorderOptions],
) -> _AsyncGeneratorContextManager[turu.core.async_cursor.GenericAsyncCursor]:
    ...


@deprecated(
    "This function is deprecated. Use `record_to_csv` instead.",
)
def record_as_csv(  # type: ignore
    record_filepath: Union[str, Path],
    cursor: Union[
        turu.core.cursor.GenericCursor, turu.core.async_cursor.GenericAsyncCursor
    ],
    *,
    enable: Union[str, bool, None] = True,
    **options: Unpack[CsvRecorderOptions],
):
    return record_to_csv(record_filepath, cursor, enable=enable, **options)
