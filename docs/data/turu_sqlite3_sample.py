import pydantic
import turu.sqlite3


class User(pydantic.BaseModel):
    id: int
    name: str


connection = turu.sqlite3.connect(":memory:")

with connection.cursor().execute_map(User, "select 1, 'taro'") as cursor:
    assert cursor.fetchone() == User(id=1, name="taro")
