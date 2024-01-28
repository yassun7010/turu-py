import pandera as pa
import pytest
import turu.snowflake
from pandera.errors import SchemaInitError
from pandera.typing import DataFrame
from typing_extensions import Annotated


class User(pa.DataFrameModel):
    id: Annotated[pa.Int64, pa.Field(ge=5)]


connection = turu.snowflake.connect_from_env()

with pytest.raises(SchemaInitError):
    with connection.execute_map(User, "select 1 as id union all select 2 id") as cursor:
        df: DataFrame[User] = cursor.fetch_pandas_all()
