import pydantic
import turu.snowflake


class User(pydantic.BaseModel):
    id: int
    name: str


connection = turu.snowflake.connect_from_env()

with connection.cursor() as cursor:
    user = cursor.execute_map(
        User,
        "SELECT %(id)s, %(name)s",
        {"id": 1, "name": "taro"},
    ).fetchone()

    assert user == User(id=1, name="taro")
