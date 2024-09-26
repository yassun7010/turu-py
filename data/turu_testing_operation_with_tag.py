import pydantic
import turu.snowflake
from turu.core import tag


class User(pydantic.BaseModel):
    id: int
    name: str


def your_logic(connection: turu.snowflake.Connection):
    with connection.cursor() as cursor:
        cursor.execute_with_tag(
            tag.Update[User],
            "UPDATE users SET name = 'jiro' WHERE id = 1",
        ).fetchone()


def test_your_logic(connection: turu.snowflake.MockConnection):
    connection.inject_operation_with_tag(tag.Update[User])

    your_logic(connection)
