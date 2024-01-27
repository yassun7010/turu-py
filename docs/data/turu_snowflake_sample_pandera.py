from typing import Annotated

import pytest
import turu.snowflake
from pandera import DataFrameModel, Field, Int64
from pandera.errors import SchemaInitError
from pandera.typing import DataFrame


class User(DataFrameModel):
    id: Annotated[Int64, Field(ge=5)]


connection = turu.snowflake.connect_from_env()

with pytest.raises(SchemaInitError):
    with connection.execute_map(User, "select 1 as id union all select 2 id") as cursor:
        df: DataFrame[User] = cursor.fetch_pandas_all()
