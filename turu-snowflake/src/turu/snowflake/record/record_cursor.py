import turu.core.record
from turu.snowflake.features import PandasDataFrame


class RecordCursor(turu.core.record.RecordCursor):
    def fetch_pandas_all(self, **kwargs) -> "PandasDataFrame":
        df: PandasDataFrame = self._raw_cursor.fetch_pandas_all(**kwargs)  # type: ignore

        if isinstance(self._recorder, turu.core.record.CsvRecorder):
            if limit := self._recorder._options.get("limit"):
                record_df = df.head(limit)

            else:
                record_df = df

            record_df.to_csv(
                self._recorder.file,
                index=False,
                header=self._recorder._options.get("header", True),
            )

        return df
