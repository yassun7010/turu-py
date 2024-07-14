from typing import Generic

from turu.core.cursor import GenericRowType


class Tag:
    """
    Base class for tags specified by `execute_with_tag`.
    """

    pass


class Insert(Tag, Generic[GenericRowType]):
    """
    Tag to specify when executing an INSERT query with `execute_with_tag`.

    It is assumed that a Pydantic model with table information is specified.

    ```python
    class User(pydantic.BaseModel):
        id: int
        name: str

    with database_conn.cursor() as cursor:
        cursor.execute_with_tag(
            tag.Insert[User],
            "INSERT INTO users (id, name) VALUES (1, 'Alice')"
        )
    ```
    """

    pass


class Update(Tag, Generic[GenericRowType]):
    """
    Tag to specify when executing an UPDATE query with `execute_with_tag`.

    It is assumed that a Pydantic model with table information is specified.

    ```python
    class User(pydantic.BaseModel):
        id: int
        name: str

    with database_conn.cursor() as cursor:
        cursor.execute_with_tag(
            tag.Update[User],
            "UPDATE users SET name = 'Bob' WHERE id = 1"
        )
    ```
    """

    pass


class Delete(Tag, Generic[GenericRowType]):
    """
    Tag to specify when executing a DELETE query with `execute_with_tag`.

    It is assumed that a Pydantic model with table information is specified.

    ```python
    class User(pydantic.BaseModel):
        id: int
        name: str

    with database_conn.cursor() as cursor:
        cursor.execute_with_tag(
            tag.Delete[User],
            "DELETE FROM users WHERE id = 1"
        )
    """

    pass


class Truncate(Tag, Generic[GenericRowType]):
    """
    Tag to specify when executing a TRUNCATE query with `execute_with_tag`.

    ```python
    with database_conn.cursor() as cursor:
        cursor.execute_with_tag(
            tag.Truncate,
            "TRUNCATE TABLE users"
        )
    """

    pass
