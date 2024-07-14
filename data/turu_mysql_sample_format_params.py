import pydantic
import turu.mysql


class User(pydantic.BaseModel):
    id: int
    name: str


connection = turu.mysql.connect_from_env()

with connection.cursor() as cursor:
    user = cursor.execute_map(
        User,
        "select %s, %s",
        (1, "taro"),
    ).fetchone()

    assert user == User(id=1, name="taro")
