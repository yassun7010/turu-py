import pydantic
import turu.postgres


class User(pydantic.BaseModel):
    id: int
    name: str


connection = turu.postgres.connect_from_env()

with connection.cursor() as cursor:
    user = cursor.execute_map(User, "SELECT 1, 'taro'").fetchone()

    assert user == User(id=1, name="taro")
