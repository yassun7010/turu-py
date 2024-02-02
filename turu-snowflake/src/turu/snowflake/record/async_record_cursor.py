import turu.core.record
from turu.snowflake.features import PandasDataFrame


class AsyncRecordCursor(turu.core.record.AsyncRecordCursor):
    async def fetch_pandas_all(self, **kwargs) -> "PandasDataFrame":
        df: PandasDataFrame = self._raw_cursor.fetch_pandas_all(**kwargs)  # type: ignore
        if isinstance(self._recorder, turu.core.record.CsvRecorder):
            df.to_csv(self._recorder.file, index=False)

        return df
