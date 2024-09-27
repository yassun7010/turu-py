import pydantic
import turu.sqlite3


class User(pydantic.BaseModel):
    id: int
    name: str


connection = turu.sqlite3.connect(":memory:")

with connection.cursor() as cursor:
    user = cursor.execute_map(
        User,
        "SELECT :id, :name",
        {"id": 1, "name": "taro"},
    ).fetchone()

    assert user == User(id=1, name="taro")
