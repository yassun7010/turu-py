from pathlib import Path
from typing import Optional

import turu.core.connection
import turu.core.mock
import turu.snowflake.cursor
from typing_extensions import Never

import snowflake.connector

from .cursor import Cursor


class Connection(turu.core.connection.Connection):
    def __init__(self, connection: snowflake.connector.SnowflakeConnection):
        self._raw_connection = connection

    def close(self) -> None:
        self._raw_connection.close()

    def commit(self) -> None:
        self._raw_connection.commit()

    def rollback(self) -> None:
        self._raw_connection.rollback()

    def cursor(self) -> Cursor[Never]:
        return Cursor(self._raw_connection.cursor())


class MockConnection(Connection, turu.core.mock.MockConnection):
    def __init__(self, *args, **kwargs):
        turu.core.mock.MockConnection.__init__(self)

    def cursor(self) -> "turu.snowflake.cursor.MockCursor[Never]":
        return turu.snowflake.cursor.MockCursor(self._turu_mock_store)


def connect(
    connection_name: Optional[str] = None,
    connections_file_path: Optional[Path] = None,
    **kwargs,
) -> Connection:
    return Connection(
        snowflake.connector.SnowflakeConnection(
            connection_name,
            connections_file_path,
            **kwargs,
        )
    )
