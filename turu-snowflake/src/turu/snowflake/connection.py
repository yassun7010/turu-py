import os
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

    @override
    def close(self) -> None:
        self._raw_connection.close()

    @override
    def commit(self) -> None:
        self._raw_connection.commit()

    @override
    def rollback(self) -> None:
        self._raw_connection.rollback()

    @override
    def cursor(self) -> Cursor[Never]:
        return Cursor(self._raw_connection.cursor())

    @override
    def execute(
        self,
        operation: str,
        parameters: Optional[Any] = None,
        /,
        **options: Unpack[ExecuteOptions],
    ) -> Cursor[Tuple[Any]]:
        return self.cursor().execute(operation, parameters, **options)

    @override
    def executemany(
        self,
        operation: str,
        seq_of_parameters: Sequence[Any],
        /,
        **options: Unpack[ExecuteOptions],
    ) -> Cursor[Tuple[Any]]:
        return self.cursor().executemany(operation, seq_of_parameters, **options)

    @override
    def execute_map(
        self,
        row_type: Type[turu.core.cursor.GenericNewRowType],
        operation: str,
        parameters: Optional[Any] = None,
        /,
        **options: Unpack[ExecuteOptions],
    ) -> Cursor[turu.core.cursor.GenericNewRowType]:
        return self.cursor().execute_map(
            row_type,
            operation,
            parameters,
            **options,
        )

    @override
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


def connect(
    connection_name: Optional[str] = None,
    connections_file_path: Optional[Path] = None,
    user: Optional[str] = None,
    password: Optional[str] = None,
    account: Optional[str] = None,
    database: Optional[str] = None,
    schema: Optional[str] = None,
    warehouse: Optional[str] = None,
    role: Optional[str] = None,
    **kwargs,
) -> Connection:
    return Connection(
        snowflake.connector.SnowflakeConnection(
            connection_name,
            connections_file_path,
            user=user,
            password=password,
            account=account,
            database=database,
            schema=schema,
            warehouse=warehouse,
            role=role,
            **kwargs,
        )
    )


def connect_from_env(
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
) -> Connection:
    if (
        authenticator := os.environ.get("SNOWFLAKE_AUTHENTICATOR")
    ) and "authenticator" not in kwargs:
        kwargs["authenticator"] = authenticator

    return connect(
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
