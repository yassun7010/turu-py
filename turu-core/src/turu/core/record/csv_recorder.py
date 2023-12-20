import csv
from dataclasses import is_dataclass
from pathlib import Path
from typing import Union, cast

import turu.core.cursor
from turu.core._feature_flags import USE_PYDANTIC, PydanticModel
from turu.core.exception import TuruRowTypeNotSupportedError
from turu.core.record.recorder_protcol import RecorderProtcol
from typing_extensions import NotRequired, TypedDict, Unpack


class CsvRecorderOptions(TypedDict):
    header: NotRequired[bool]
    limit: NotRequired[int]


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

    def write_row(self, row: turu.core.cursor.RowType) -> None:
        if self._writed_rowsize == 0:
            if self._options.get("header", True):
                self._write_header(row)

        if (limit := self._options.get("limit")) is not None:
            if self._writed_rowsize >= limit:
                return

        self._writed_rowsize += 1

        if is_dataclass(row):
            self._writer.writerow(row.__dict__.values())
            return

        if isinstance(row, tuple):
            self._writer.writerow(cast(tuple, row))
            return

        if USE_PYDANTIC:
            if isinstance(row, PydanticModel):
                self._writer.writerow(row.model_dump().values())
                return

        raise TuruRowTypeNotSupportedError(type(row))

    def _write_header(self, row: turu.core.cursor.RowType) -> None:
        if is_dataclass(row):
            self._writer.writerow(row.__dict__.keys())
            return

        if USE_PYDANTIC:
            if isinstance(row, PydanticModel):
                self._writer.writerow(row.model_dump().keys())
                return

        raise TuruRowTypeNotSupportedError(type(row))

    def close(self) -> None:
        if not self._file.closed:
            self._file.close()
