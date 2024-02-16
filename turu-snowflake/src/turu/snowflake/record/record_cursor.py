from typing import Generic, Iterator, cast

import turu.core.record
import turu.snowflake.cursor
from turu.core.cursor import GenericRowType
from turu.snowflake.features import (
    GenericPandasDataFrame,
    GenericPyArrowTable,
)
from typing_extensions import Self


class RecordCursor(  # type: ignore[override]
    turu.core.record.RecordCursor,
    Generic[GenericRowType, GenericPandasDataFrame, GenericPyArrowTable],
):
    def fetch_pandas_all(self, **kwargs) -> GenericPandasDataFrame:
        df = self._sf_cursor.fetch_pandas_all(**kwargs)

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
        batches = self._sf_cursor.fetch_pandas_batches(**kwargs)

        if isinstance(self._recorder, turu.core.record.CsvRecorder):
            if limit := self._recorder._options.get("limit"):
                for batch in batches:
                    yield batch.head(limit)

                    limit -= len(batch)
                    if limit <= 0:
                        return

        return batches

    def fetch_arrow_all(self) -> GenericPyArrowTable:
        table = self._sf_cursor.fetch_arrow_all()

        if isinstance(self._recorder, turu.core.record.CsvRecorder):
            if limit := self._recorder._options.get("limit"):
                table = table.slice(0, limit)

            table.to_pandas().to_csv(
                self._recorder.file,
                index=False,
                header=self._recorder._options.get("header", True),
            )

        return table

    def fetch_arrow_batches(self) -> Iterator[GenericPyArrowTable]:
        batches = self._sf_cursor.fetch_arrow_batches()

        if isinstance(self._recorder, turu.core.record.CsvRecorder):
            if limit := self._recorder._options.get("limit"):
                for batch in batches:
                    yield batch.slice(0, limit)

                    limit -= batch.num_rows
                    if limit <= 0:
                        return

        return batches

    def use_warehouse(self, warehouse: str, /) -> Self:
        """Use a warehouse in cursor."""

        self._sf_cursor.use_warehouse(warehouse)

        return self

    def use_database(self, database: str, /) -> Self:
        """Use a database in cursor."""

        self._sf_cursor.use_database(database)

        return self

    def use_schema(self, schema: str, /) -> Self:
        """Use a schema in cursor."""

        self._sf_cursor.use_schema(schema)

        return self

    def use_role(self, role: str, /) -> Self:
        """Use a role in cursor."""

        self._sf_cursor.use_role(role)

        return self

    @property
    def _sf_cursor(
        self,
    ) -> turu.snowflake.cursor.Cursor[
        GenericRowType, GenericPandasDataFrame, GenericPyArrowTable
    ]:
        return cast(turu.snowflake.cursor.Cursor, self._cursor)
