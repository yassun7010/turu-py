import csv
from collections.abc import Callable
from dataclasses import is_dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, Sequence, TextIO, Union

import turu.core.cursor
from turu.core.exception import TuruRowTypeNotSupportedError
from turu.core.features import USE_PYDANTIC, PydanticModel
from turu.core.record.recorder_protcol import RecorderProtcol
from typing_extensions import LiteralString, NotRequired, TypedDict, Unpack


def not_supported(row: Any):
    raise TuruRowTypeNotSupportedError(type(row))


RecordCheckMap: Dict[str, Callable[[Any], bool]] = {
    "dataclass": is_dataclass,
    "tuple": lambda row: isinstance(row, tuple),
    "pydantic": lambda row: USE_PYDANTIC and isinstance(row, PydanticModel),
}

RecordHeaderMap: Dict[str, Callable[[Any], Iterable[str]]] = {
    "dataclass": lambda row: row.__dict__.keys(),
    "tuple": not_supported,
    "pydantic": lambda row: row.model_dump().keys(),
}

RecordRowMap: Dict[str, Callable[[Any], Iterable[str]]] = {
    "dataclass": lambda row: row.__dict__.values(),
    "tuple": lambda row: row,
    "pydantic": lambda row: row.model_dump().values(),
}

RecordRowsMap: Dict[str, Callable[[Any], Iterable[Iterable[str]]]] = {
    "dataclass": not_supported,
    "tuple": not_supported,
    "pydantic": not_supported,
}


class CsvRecorderOptions(TypedDict):
    """Options for CSV recorder."""

    header: NotRequired[bool]
    """Whether to write header or not."""

    limit: NotRequired[int]
    """Limit of rows to write."""


class CsvRecorder(RecorderProtcol):
    def __init__(
        self,
        filename: Union[str, Path],
        **options: Unpack[CsvRecorderOptions],
    ) -> None:
        self._file = Path(filename).open("w")
        self._writer = csv.writer(self._file)
        self._options = options
        self._writed_rowsize = 0

    @property
    def file(self) -> TextIO:
        return self._file

    def record(
        self,
        rows: Union[
            turu.core.cursor.GenericRowType,
            Sequence[turu.core.cursor.GenericRowType],
        ],
    ) -> None:
        try:
            datatype = get_datatype(rows)

        except TuruRowTypeNotSupportedError:
            pass

        for row in rows:  # type: ignore
            datatype = get_datatype(row)

            if self._writed_rowsize == 0:
                if self._options.get("header", True):
                    self._writer.writerow(RecordHeaderMap[datatype](row))

            if (limit := self._options.get("limit")) is not None:
                if self._writed_rowsize >= limit:
                    continue

            self._writed_rowsize += 1

            self._writer.writerow(RecordRowMap[datatype](row))

        return

    def close(self) -> None:
        if not self._file.closed:
            self._file.close()


def get_datatype(row: Any) -> str:
    for datatype, check in RecordCheckMap.items():
        if check(row):
            return datatype
    raise TuruRowTypeNotSupportedError(type(row))


def add_record_map(
    datatype: LiteralString,
    check_map: Callable[[Any], bool],
    header_map: Callable[[Any], Iterable[Any]],
    row_map: Callable[[Any], Iterable[Any]],
    rows_map: Callable[[Any], Iterable[Any]],
):
    global RecordHeaderMap, RecordRowMap

    RecordHeaderMap[datatype] = header_map
    RecordCheckMap[datatype] = check_map
    RecordRowMap[datatype] = row_map
