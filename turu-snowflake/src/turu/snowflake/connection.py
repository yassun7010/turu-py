from typing import Callable, Type, overload

from turu.core.protocols.connection import ConnectionProtocol
from typing_extensions import Self

import snowflake.connector

from .cursor import Cursor


class Connection(snowflake.connector.SnowflakeConnection, ConnectionProtocol):
    @overload
    def cursor(
        self, cursor_class: Callable[[Self], snowflake.connector.cursor.SnowflakeCursor]
    ) -> Cursor:
        ...

    @overload
    def cursor(
        self,
        cursor_class: Type[
            snowflake.connector.cursor.SnowflakeCursor
        ] = snowflake.connector.cursor.SnowflakeCursor,
    ) -> Cursor:
        ...

    def cursor(self, cursor_class=snowflake.connector.cursor.SnowflakeCursor):  # type: ignore[override]
        return super().cursor(cursor_class or Cursor)


def connect(**kwargs) -> Connection:
    return Connection(**kwargs)
