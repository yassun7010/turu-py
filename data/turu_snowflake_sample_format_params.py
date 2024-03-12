import pydantic
import turu.snowflake


class User(pydantic.BaseModel):
    id: int
    name: str


connection = turu.snowflake.connect_from_env()

with connection.execute_map(User, "select %s, %s", [1, "taro"]) as cursor:
    assert cursor.fetchone() == User(id=1, name="taro")
