import os
from pathlib import Path
from typing import Any, Optional, Sequence, Tuple, Type

import turu.core.async_connection
import turu.core.cursor
import turu.core.mock
from typing_extensions import Never, Unpack, override

import snowflake.connector

from .async_cursor import AsyncCursor, ExecuteOptions


class AsyncConnection(turu.core.async_connection.AsyncConnection):
    def __init__(self, connection: snowflake.connector.SnowflakeConnection):
        self._raw_connection = connection

    @override
    async def close(self) -> None:
        self._raw_connection.close()

    @override
    async def commit(self) -> None:
        self._raw_connection.commit()

    @override
    async def rollback(self) -> None:
        self._raw_connection.rollback()

    @override
    async def cursor(self) -> AsyncCursor[Never]:
        return AsyncCursor(self._raw_connection.cursor())

    @override
    async def execute(
        self,
        operation: str,
        parameters: Optional[Any] = None,
        /,
        **options: Unpack[ExecuteOptions],
    ) -> AsyncCursor[Tuple[Any]]:
        return await (await self.cursor()).execute(operation, parameters, **options)

    @override
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

    @override
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

    @override
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
    if (
        authenticator := os.environ.get("SNOWFLAKE_AUTHENTICATOR")
    ) and "authenticator" not in kwargs:
        kwargs["authenticator"] = authenticator

    return await connect_async(
        connection_name,
        connections_file_path,
        user=os.environ.get(user_envname, kwargs.get("user")),
        password=os.environ.get(password_envname, kwargs.get("password")),
        account=os.environ.get(account_envname, kwargs.get("account")),
        database=os.environ.get(database_envname, kwargs.get("database")),
        schema=os.environ.get(schema_envname, kwargs.get("schema")),
        warehouse=os.environ.get(warehouse_envname, kwargs.get("warehouse")),
        role=os.environ.get(role_envname, kwargs.get("role")),
        **kwargs,
    )
