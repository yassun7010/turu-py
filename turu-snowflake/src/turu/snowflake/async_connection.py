import os
from pathlib import Path
from typing import Any, Optional, Sequence, Tuple, Type

import turu.core.async_connection
import turu.core.cursor
import turu.core.mock
from typing_extensions import Never, Unpack, override

import snowflake.connector

from .async_cursor import AsyncCursor, ExecuteOptions, MockAsyncCursor


class AsyncConnection(turu.core.async_connection.AsyncConnection):
    def __init__(self, connection: snowflake.connector.SnowflakeConnection):
        self._raw_connection = connection

    async def close(self) -> None:
        self._raw_connection.close()

    async def commit(self) -> None:
        self._raw_connection.commit()

    async def rollback(self) -> None:
        self._raw_connection.rollback()

    async def cursor(self) -> AsyncCursor[Never]:
        return AsyncCursor(self._raw_connection.cursor())

    async def execute(
        self,
        operation: str,
        parameters: Optional[Any] = None,
        /,
        **options: Unpack[ExecuteOptions],
    ) -> AsyncCursor[Tuple[Any]]:
        return await (await self.cursor()).execute(operation, parameters, **options)

    async def executemany(
        self,
        operation: str,
        seq_of_parameters: Sequence[Any],
        /,
        **options: Unpack[ExecuteOptions],
    ) -> AsyncCursor[Tuple[Any]]:
        return await (await self.cursor()).executemany(
            operation, seq_of_parameters, **options
        )

    async def execute_map(
        self,
        row_type: Type[turu.core.cursor.GenericNewRowType],
        operation: str,
        parameters: Optional[Any] = None,
        /,
        **options: Unpack[ExecuteOptions],
    ) -> AsyncCursor[turu.core.cursor.GenericNewRowType]:
        return await (await self.cursor()).execute_map(
            row_type,
            operation,
            parameters,
            **options,
        )

    async def executemany_map(
        self,
        row_type: Type[turu.core.cursor.GenericNewRowType],
        operation: str,
        seq_of_parameters: Sequence[Any],
        /,
        **options: Unpack[ExecuteOptions],
    ) -> AsyncCursor[turu.core.cursor.GenericNewRowType]:
        return await (await self.cursor()).executemany_map(
            row_type,
            operation,
            seq_of_parameters,
            **options,
        )


class MockAsyncConnection(AsyncConnection, turu.core.mock.MockAsyncConnection):
    def __init__(self, *args, **kwargs):
        turu.core.mock.MockAsyncConnection.__init__(self)

    @override
    async def cursor(self) -> "MockAsyncCursor[Never]":
        return MockAsyncCursor(self._turu_mock_store)


async def connect_async(
    connection_name: Optional[str] = None,
    connections_file_path: Optional[Path] = None,
    **kwargs,
) -> AsyncConnection:
    return AsyncConnection(
        snowflake.connector.SnowflakeConnection(
            connection_name,
            connections_file_path,
            **kwargs,
        )
    )


async def connect_async_from_env(
    connection_name: Optional[str] = None,
    connections_file_path: Optional[Path] = None,
    user_envname: str = "SNOWFLAKE_USER",
    password_envname: str = "SNOWFLAKE_PASSWORD",
    account_envname: str = "SNOWFLAKE_ACCOUNT",
    database_envname: str = "SNOWFLAKE_DATABASE",
    schema_envname: str = "SNOWFLAKE_SCHEMA",
    warehouse_envname: str = "SNOWFLAKE_WAREHOUSE",
    role_envname: str = "SNOWFLAKE_ROLE",
    **kwargs,
) -> AsyncConnection:
    return await connect_async(
        connection_name,
        connections_file_path,
        user=os.environ.get(user_envname),
        password=os.environ.get(password_envname),
        account=os.environ.get(account_envname),
        database=os.environ.get(database_envname),
        schema=os.environ.get(schema_envname),
        warehouse=os.environ.get(warehouse_envname),
        role=os.environ.get(role_envname),
        **kwargs,
    )
