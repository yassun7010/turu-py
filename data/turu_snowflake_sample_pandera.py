import pandera as pa
import pytest
import turu.snowflake
from pandera.errors import SchemaError
from pandera.typing import DataFrame, Series


class User(pa.DataFrameModel):
    id: Series[pa.Int8] = pa.Field(ge=2, alias="ID")


connection = turu.snowflake.connect_from_env()

with pytest.raises(SchemaError):
    with connection.execute_map(
        User, "select 1 as id union all select 2 as id"
    ) as cursor:
        df: DataFrame[User] = cursor.fetch_pandas_all()
