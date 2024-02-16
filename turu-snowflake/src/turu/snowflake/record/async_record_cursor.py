from typing import AsyncIterator, Generic, cast

import turu.core.record
import turu.snowflake.async_cursor
from turu.core.cursor import GenericRowType
from turu.snowflake.features import (
    GenericPandasDataFrame,
    GenericPyArrowTable,
)


class AsyncRecordCursor(  # type: ignore[override]
    turu.core.record.AsyncRecordCursor,
    Generic[
        GenericRowType,
        GenericPandasDataFrame,
        GenericPyArrowTable,
    ],
):
    async def fetch_pandas_all(self, **kwargs) -> GenericPandasDataFrame:
        df = cast(GenericPandasDataFrame, await self._cursor.fetch_pandas_all(**kwargs))  # type: ignore[assignment]

        if isinstance(self._recorder, turu.core.record.CsvRecorder):
            if limit := self._recorder._options.get("limit"):
                df = df.head(limit)

            df.to_csv(
                self._recorder.file,
                index=False,
                header=self._recorder._options.get("header", True),
            )

        return df

    async def fetch_pandas_batches(
        self, **kwargs
    ) -> AsyncIterator[GenericPandasDataFrame]:
        batches = cast(
            AsyncIterator[GenericPandasDataFrame],
            self._cursor.fetch_pandas_batches(**kwargs),  # type: ignore[assignment]
        )
        if isinstance(self._recorder, turu.core.record.CsvRecorder):
            if limit := self._recorder._options.get("limit"):
                async for batch in batches:
                    yield batch.head(limit)

                    limit -= len(batch)
                    if limit <= 0:
                        return

        async for batch in batches:
            yield batch

    async def fetch_arrow_all(self) -> GenericPyArrowTable:
        table = cast(GenericPyArrowTable, await self._cursor.fetch_arrow_all())  # type: ignore[assignment]

        if isinstance(self._recorder, turu.core.record.CsvRecorder):
            if limit := self._recorder._options.get("limit"):
                table = table.slice(0, limit)

            table.to_pandas().to_csv(
                self._recorder.file,
                index=False,
                header=self._recorder._options.get("header", True),
            )

        return table

    async def fetch_arrow_batches(self) -> AsyncIterator[GenericPyArrowTable]:
        batches = cast(
            AsyncIterator[GenericPyArrowTable],
            self._cursor.fetch_arrow_batches(),  # type: ignore[assignment]
        )
        if isinstance(self._recorder, turu.core.record.CsvRecorder):
            if limit := self._recorder._options.get("limit"):
                async for batch in batches:
                    yield batch.slice(0, limit)

                    limit -= len(batch)
                    if limit <= 0:
                        return

        async for batch in batches:
            yield batch
