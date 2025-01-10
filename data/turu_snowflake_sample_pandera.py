import pandera as pa
import pytest
import turu.snowflake
from pandera.errors import SchemaError
from pandera.typing import DataFrame, Series


class User(pa.DataFrameModel):
    id: Series[pa.Int8] = pa.Field(ge=2, alias="ID")


connection = turu.snowflake.connect_from_env()

with pytest.raises(SchemaError):
    with connection.cursor() as cursor:
        df: DataFrame[User] = cursor.execute_map(
            User,
            """
            SELECT 1 AS id
            UNION ALL
            SELECT 2 AS id
            """,
        ).fetch_pandas_all()
