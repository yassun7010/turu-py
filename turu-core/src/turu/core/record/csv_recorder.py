import csv
from dataclasses import is_dataclass
from pathlib import Path
from typing import Sequence, Union

import turu.core.cursor
from turu.core.exception import TuruRowTypeNotSupportedError
from turu.core.features import USE_PYDANTIC, PydanticModel
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

    def record(
        self, rows: Sequence[turu.core.cursor.GenericRowType]
    ) -> Sequence[turu.core.cursor.GenericRowType]:
        for row in rows:
            if self._writed_rowsize == 0:
                if self._options.get("header", True):
                    self._write_header(row)

            if (limit := self._options.get("limit")) is not None:
                if self._writed_rowsize >= limit:
                    continue

            self._writed_rowsize += 1

            if is_dataclass(row):
                self._writer.writerow(row.__dict__.values())
                continue

            if isinstance(row, tuple):
                self._writer.writerow(row)
                continue

            if USE_PYDANTIC:
                if isinstance(row, PydanticModel):
                    self._writer.writerow(row.model_dump().values())
                    continue

            raise TuruRowTypeNotSupportedError(type(row))

        return rows

    def _write_header(
        self, row: turu.core.cursor.GenericRowType
    ) -> turu.core.cursor.GenericRowType:
        if is_dataclass(row):
            self._writer.writerow(row.__dict__.keys())
            return row

        if USE_PYDANTIC:
            if isinstance(row, PydanticModel):
                self._writer.writerow(row.model_dump().keys())
                return row

        raise TuruRowTypeNotSupportedError(type(row))

    def close(self) -> None:
        if not self._file.closed:
            self._file.close()
