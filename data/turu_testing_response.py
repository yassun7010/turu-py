import pydantic
import turu.snowflake


class User(pydantic.BaseModel):
    id: int
    name: str


def your_logic(connection: turu.snowflake.Connection):
    with connection.cursor() as cursor:
        user = cursor.execute_map(
            User,
            "SELECT * FROM users WHERE id = 1",
        ).fetchone()

        print(user)


def test_your_logic(connection: turu.snowflake.MockConnection):
    connection.inject_response(User, [User(id=1, name="taro")])

    your_logic(connection)
