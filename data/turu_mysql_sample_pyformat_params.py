import pydantic
import turu.mysql


class User(pydantic.BaseModel):
    id: int
    name: str


connection = turu.mysql.connect_from_env()

with connection.execute_map(
    User, "select %(id)s, %(name)s", {"id": 1, "name": "taro"}
) as cursor:
    assert cursor.fetchone() == User(id=1, name="taro")
