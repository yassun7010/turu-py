from typing import Iterator

import turu.core.record
import turu.snowflake.cursor
from turu.core.cursor import GenericRowType
from turu.snowflake.features import (
    GenericPandasDataFrame,
    GenericPyArrowTable,
)


class RecordCursor(  # type: ignore[override]
    turu.core.record.RecordCursor,
    turu.snowflake.cursor.Cursor[
        GenericRowType, GenericPandasDataFrame, GenericPyArrowTable
    ],
):
    def fetch_pandas_all(self, **kwargs) -> GenericPandasDataFrame:
        df = super().fetch_pandas_all(**kwargs)

        if isinstance(self._recorder, turu.core.record.CsvRecorder):
            if limit := self._recorder._options.get("limit"):
                df = df.head(limit)

            df.to_csv(
                self._recorder.file,
                index=False,
                header=self._recorder._options.get("header", True),
            )

        return df

    def fetch_pandas_batches(self, **kwargs) -> Iterator[GenericPandasDataFrame]:
        batches = super().fetch_pandas_batches(**kwargs)
        if isinstance(self._recorder, turu.core.record.CsvRecorder):
            if limit := self._recorder._options.get("limit"):
                for batch in batches:
                    yield batch

                    limit -= len(batch)
                    if limit <= 0:
                        return

        return batches
