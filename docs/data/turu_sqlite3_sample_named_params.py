import pydantic
import turu.sqlite3


class User(pydantic.BaseModel):
    id: int
    name: str


connection = turu.sqlite3.connect(":memory:")

with connection.execute_map(
    User, "select :id, :name", {"id": 1, "name": "taro"}
) as cursor:
    assert cursor.fetchone() == User(id=1, name="taro")
