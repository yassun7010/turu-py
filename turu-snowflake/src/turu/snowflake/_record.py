import turu.core.record
from turu.core.record.csv_recorder import add_record_map
from turu.snowflake.features import USE_PANDAS, PandasDataFlame

if USE_PANDAS:
    add_record_map(
        "pandas",
        lambda row: USE_PANDAS and isinstance(row, PandasDataFlame),
        lambda row: row.keys(),
        lambda row: row.values,
        lambda row: row,
    )


class _RecordCursor(turu.core.record._RecordCursor):
    def fetch_pandas_all(self, **kwargs) -> "PandasDataFlame":
        df: PandasDataFlame = self._raw_cursor.fetch_pandas_all(**kwargs)  # type: ignore
        if isinstance(self._recorder, turu.core.record.CsvRecorder):
            df.to_csv(self._recorder.file, index=False)

        return df
