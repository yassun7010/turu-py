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
        df = await self.__sf_cursor.fetch_pandas_all(**kwargs)

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
        batches = self.__sf_cursor.fetch_pandas_batches(**kwargs)

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
        table = await self.__sf_cursor.fetch_arrow_all()

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
        batches = self.__sf_cursor.fetch_arrow_batches()

        if isinstance(self._recorder, turu.core.record.CsvRecorder):
            if limit := self._recorder._options.get("limit"):
                async for batch in batches:
                    yield batch.slice(0, limit)

                    limit -= len(batch)
                    if limit <= 0:
                        return

        async for batch in batches:
            yield batch

    @property
    def __sf_cursor(
        self,
    ) -> turu.snowflake.async_cursor.AsyncCursor[
        GenericRowType, GenericPandasDataFrame, GenericPyArrowTable
    ]:
        return cast(
            turu.snowflake.async_cursor.AsyncCursor, self.__record_taregt_cursor
        )
