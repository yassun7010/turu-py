import pydantic
import turu.postgres


class User(pydantic.BaseModel):
    id: int
    name: str


connection = turu.postgres.connect_from_env()

with connection.execute_map(User, "select %s, %s", [1, "taro"]) as cursor:
    assert cursor.fetchone() == User(id=1, name="taro")
