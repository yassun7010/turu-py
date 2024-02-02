from turu.core.record.csv_recorder import add_record_map
from turu.snowflake.features import USE_PANDAS, PandasDataFrame

if USE_PANDAS:
    add_record_map(
        "pandas",
        lambda row: USE_PANDAS and isinstance(row, PandasDataFrame),
        lambda row: row.keys(),
        lambda row: row.values,
        lambda row: row,
    )
