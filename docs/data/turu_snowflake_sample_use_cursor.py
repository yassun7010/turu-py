import os

import pydantic
import turu.snowflake


class User(pydantic.BaseModel):
    id: int
    name: str


connection = turu.snowflake.connect_from_env()

with (
    connection.cursor()
    .use_warehouse(os.environ["SNOWFLAKE_WAREHOUSE"])
    .use_database(os.environ["SNOWFLAKE_DATABASE"])
) as cursor:
    assert cursor.execute_map(User, "select 1, 'taro'").fetchone() == User(
        id=1, name="taro"
    )
