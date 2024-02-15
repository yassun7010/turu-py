from typing import Generic, Iterator, cast

import turu.core.record
import turu.snowflake.cursor
from turu.core.cursor import GenericRowType
from turu.snowflake.features import (
    GenericPandasDataFrame,
    GenericPyArrowTable,
)


class RecordCursor(  # type: ignore[override]
    turu.core.record.RecordCursor,
    Generic[GenericRowType, GenericPandasDataFrame, GenericPyArrowTable],
):
    def fetch_pandas_all(self, **kwargs) -> GenericPandasDataFrame:
        df = cast(GenericPandasDataFrame, self._cursor.fetch_pandas_all(**kwargs))  # type: ignore[assignment]

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
        batches = cast(
            Iterator[GenericPandasDataFrame],
            self._cursor.fetch_pandas_batches(**kwargs),  # type: ignore[assignment]
        )
        if isinstance(self._recorder, turu.core.record.CsvRecorder):
            if limit := self._recorder._options.get("limit"):
                for batch in batches:
                    yield batch.head(limit)

                    limit -= len(batch)
                    if limit <= 0:
                        return

        return batches
