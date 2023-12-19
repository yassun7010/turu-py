from pathlib import Path
from typing import Optional

from turu.core.protocols.connection import ConnectionProtocol
from typing_extensions import Never

import snowflake.connector

from .cursor import Cursor


class Connection(ConnectionProtocol):
    def __init__(self, raw_connection: snowflake.connector.SnowflakeConnection):
        self._raw_connection = raw_connection

    def close(self) -> None:
        self._raw_connection.close()

    def commit(self) -> None:
        self._raw_connection.commit()

    def rollback(self) -> None:
        self._raw_connection.rollback()

    def cursor(self) -> Cursor[Never]:
        return Cursor(self._raw_connection.cursor())


try:
    import turu.mock  # type: ignore
    import turu.snowflake.cursor

    class MockConnection(Connection, turu.mock.MockConnection):
        def __init__(self, **kwargs):
            turu.mock.MockConnection.__init__(self)

        def cursor(self) -> "turu.snowflake.cursor.MockCursor[Never]":
            return turu.snowflake.cursor.MockCursor(self._turu_mock_store)

except ImportError:
    pass


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
