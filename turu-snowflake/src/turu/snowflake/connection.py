from pathlib import Path
from typing import Any, Optional, Sequence, Tuple, Type

import turu.core.connection
import turu.core.cursor
import turu.core.mock
import turu.snowflake.cursor
from typing_extensions import Never, Unpack, override

import snowflake.connector

from .cursor import Cursor, ExecuteOptions


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

    def execute(
        self,
        operation: str,
        parameters: Optional[Any] = None,
        /,
        **options: Unpack[ExecuteOptions],
    ) -> Cursor[Tuple[Any]]:
        return self.cursor().execute(operation, parameters, **options)

    def executemany(
        self,
        operation: str,
        seq_of_parameters: Sequence[Any],
        /,
        **options: Unpack[ExecuteOptions],
    ) -> Cursor[Tuple[Any]]:
        return self.cursor().executemany(operation, seq_of_parameters, **options)

    def execute_map(
        self,
        row_type: Type[turu.core.cursor.GenericNewRowType],
        operation: str,
        parameters: Optional[Any] = None,
        /,
        **options: Unpack[ExecuteOptions],
    ) -> Cursor[turu.core.cursor.GenericNewRowType]:
        return self.cursor().execute_map(row_type, operation, parameters, **options)

    def executemany_map(
        self,
        row_type: Type[turu.core.cursor.GenericNewRowType],
        operation: str,
        seq_of_parameters: Sequence[Any],
        /,
        **options: Unpack[ExecuteOptions],
    ) -> Cursor[turu.core.cursor.GenericNewRowType]:
        return self.cursor().executemany_map(
            row_type, operation, seq_of_parameters, **options
        )


class MockConnection(Connection, turu.core.mock.MockConnection):
    def __init__(self, *args, **kwargs):
        turu.core.mock.MockConnection.__init__(self)

    @override
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
