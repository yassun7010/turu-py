import pydantic
import turu.bigquery


class User(pydantic.BaseModel):
    id: int
    name: str


connection = turu.bigquery.connect()

with connection.cursor() as cursor:
    user = cursor.execute_map(
        User,
        "SELECT 1, 'taro'",
    ).fetchone()

    assert user == User(id=1, name="taro")
