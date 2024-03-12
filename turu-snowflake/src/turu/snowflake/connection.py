import os
from pathlib import Path
from typing import Any, Optional, Sequence, Tuple, Type, Union, cast, overload

import turu.core.connection
import turu.core.cursor
import turu.core.mock
import turu.snowflake.cursor
from turu.core.cursor import GenericNewRowType
from turu.snowflake.features import (
    GenericNewPanderaDataFrameModel,
    PandasDataFrame,
    PanderaDataFrame,
    PyArrowTable,
)
from typing_extensions import Never, Unpack, override

import snowflake.connector

from .cursor import (
    Cursor,
    ExecuteOptions,
    GenericNewPandasDataFrame,
    GenericNewPyArrowTable,
    Self,
)


class Connection(turu.core.connection.Connection):
    """
    A connection to a Snowflake database.

    This class is a wrapper around the `snowflake.connector.SnowflakeConnection` class.
    """

    def __init__(self, connection: snowflake.connector.SnowflakeConnection):
        self._raw_connection = connection

    @override
    @classmethod
    def connect(  # type: ignore[override]
        cls,
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
    ) -> Self:
        return cls(
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

    @override
    @classmethod
    def connect_from_env(  # type: ignore[override]
        cls,
        connection_name: Optional[str] = None,
        connections_file_path: Optional[Path] = None,
        user_envname: str = "SNOWFLAKE_USER",
        password_envname: str = "SNOWFLAKE_PASSWORD",
        account_envname: str = "SNOWFLAKE_ACCOUNT",
        database_envname: str = "SNOWFLAKE_DATABASE",
        schema_envname: str = "SNOWFLAKE_SCHEMA",
        warehouse_envname: str = "SNOWFLAKE_WAREHOUSE",
        role_envname: str = "SNOWFLAKE_ROLE",
        authenticator_envname: str = "SNOWFLAKE_AUTHENTICATOR",
        **kwargs: Any,
    ) -> Self:
        if (
            authenticator := os.environ.get(
                authenticator_envname,
                kwargs.get("authenticator"),
            )
        ) and "authenticator" not in kwargs:
            kwargs["authenticator"] = authenticator

        return cls.connect(
            connection_name,
            connections_file_path,
            user=kwargs.pop("user", os.environ.get(user_envname)),
            password=kwargs.pop("password", os.environ.get(password_envname)),
            account=kwargs.get("account", os.environ.get(account_envname)),
            database=kwargs.get("database", os.environ.get(database_envname)),
            schema=kwargs.get("schema", os.environ.get(schema_envname)),
            warehouse=kwargs.get("warehouse", os.environ.get(warehouse_envname)),
            role=kwargs.get("role", os.environ.get(role_envname)),
            **kwargs,
        )

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
    def cursor(self) -> Cursor[Never, Never, Never]:
        return Cursor(self._raw_connection.cursor())

    @override
    def execute(
        self,
        operation: str,
        parameters: Optional[Any] = None,
        /,
        **options: Unpack[ExecuteOptions],
    ) -> Cursor[Tuple[Any], PandasDataFrame, PyArrowTable]:
        """Prepare and execute a database operation (query or command).

        This is not defined in [PEP 249](https://peps.python.org/pep-0249/),
        but is simply a convenient shortcut to `.cursor().execute()`.

        Parameters:
            operation: A database operation (query or command).
            parameters: Parameters may be provided as sequence or mapping and will be bound to variables in the operation.
            options: snowflake connector options

        Returns:
            A cursor that holds a reference to an operation.
        """

        return self.cursor().execute(operation, parameters, **options)

    @override
    def executemany(
        self,
        operation: str,
        seq_of_parameters: Sequence[Any],
        /,
        **options: Unpack[ExecuteOptions],
    ) -> Cursor[Tuple[Any], PandasDataFrame, PyArrowTable]:
        """Prepare a database operation (query or command)
        and then execute it against all parameter sequences or mappings.

        This is not defined in [PEP 249](https://peps.python.org/pep-0249/),
        but is simply a convenient shortcut to `.cursor().executemany()`.

        Parameters:
            operation: A database operation (query or command).
            seq_of_parameters: Parameters may be provided as sequence or mapping and will be bound to variables in the operation.
            options: snowflake connector options

        Returns:
            A cursor that holds a reference to an operation.
        """

        return self.cursor().executemany(operation, seq_of_parameters, **options)

    @overload
    def execute_map(
        self,
        row_type: Type[GenericNewRowType],
        operation: str,
        parameters: "Optional[Any]" = None,
        /,
        **options: Unpack[ExecuteOptions],
    ) -> Cursor[GenericNewRowType, Never, Never]:
        ...

    @overload
    def execute_map(
        self,
        row_type: Type[GenericNewPandasDataFrame],
        operation: str,
        parameters: "Optional[Any]" = None,
        /,
        **options: Unpack[ExecuteOptions],
    ) -> Cursor[Never, GenericNewPandasDataFrame, Never]:
        ...

    @overload
    def execute_map(
        self,
        row_type: Type[GenericNewPyArrowTable],
        operation: str,
        parameters: "Optional[Any]" = None,
        /,
        **options: Unpack[ExecuteOptions],
    ) -> Cursor[Never, Never, GenericNewPyArrowTable]:
        ...

    @overload
    def execute_map(
        self,
        row_type: Type[GenericNewPanderaDataFrameModel],
        operation: str,
        parameters: "Optional[Any]" = None,
        /,
        **options: Unpack[ExecuteOptions],
    ) -> Cursor[Never, PanderaDataFrame[GenericNewPanderaDataFrameModel], Never]:
        ...

    @override
    def execute_map(
        self,
        row_type: Union[
            Type[GenericNewRowType],
            Type[GenericNewPandasDataFrame],
            Type[GenericNewPyArrowTable],
            Type[GenericNewPanderaDataFrameModel],
        ],
        operation: str,
        parameters: "Optional[Any]" = None,
        /,
        **options: Unpack[ExecuteOptions],
    ) -> Cursor:
        """
        Execute a database operation (query or command) and map each row to a `row_type`.

        This is not defined in [PEP 249](https://peps.python.org/pep-0249/),
        but is simply a convenient shortcut to `.cursor().execute_map()`.

        Parameters:
            row_type: The type of the row that will be returned.
            operation: A database operation (query or command).
            parameters: Parameters may be provided as sequence or mapping and will be bound to variables in the operation.
            options: snowflake connector options

        Returns:
            A cursor that holds a reference to an operation.
        """

        return cast(
            Cursor,
            self.cursor().execute_map(
                row_type,
                operation,
                parameters,
                **options,
            ),
        )

    @overload
    def executemany_map(
        self,
        row_type: Type[GenericNewRowType],
        operation: str,
        seq_of_parameters: Sequence[Any],
        /,
        **options: Unpack[ExecuteOptions],
    ) -> Cursor[GenericNewRowType, Never, Never]:
        ...

    @overload
    def executemany_map(
        self,
        row_type: Type[GenericNewPandasDataFrame],
        operation: str,
        seq_of_parameters: Sequence[Any],
        /,
        **options: Unpack[ExecuteOptions],
    ) -> Cursor[Never, GenericNewPandasDataFrame, Never]:
        ...

    @overload
    def executemany_map(
        self,
        row_type: Type[GenericNewPyArrowTable],
        operation: str,
        seq_of_parameters: Sequence[Any],
        /,
        **options: Unpack[ExecuteOptions],
    ) -> Cursor[Never, Never, GenericNewPyArrowTable]:
        ...

    @overload
    def executemany_map(
        self,
        row_type: Type[GenericNewPanderaDataFrameModel],
        operation: str,
        seq_of_parameters: Sequence[Any],
        /,
        **options: Unpack[ExecuteOptions],
    ) -> Cursor[Never, PanderaDataFrame[GenericNewPanderaDataFrameModel], Never]:
        ...

    @override
    def executemany_map(
        self,
        row_type: Union[
            Type[GenericNewRowType],
            Type[GenericNewPandasDataFrame],
            Type[GenericNewPyArrowTable],
            Type[GenericNewPanderaDataFrameModel],
        ],
        operation: str,
        seq_of_parameters: Sequence[Any],
        /,
        **options: Unpack[ExecuteOptions],
    ) -> Cursor:
        """Execute a database operation (query or command) against all parameter sequences or mappings.

        This is not defined in [PEP 249](https://peps.python.org/pep-0249/),
        but is simply a convenient shortcut to `.cursor().executemany_map()`.

        Parameters:
            row_type: The type of the row that will be returned.
            operation: A database operation (query or command).
            seq_of_parameters: Parameters may be provided as sequence or mapping and will be bound to variables in the operation.
            options: snowflake connector options

        Returns:
            A cursor that holds a reference to an operation.
        """

        return cast(
            Cursor,
            self.cursor().executemany_map(
                row_type, operation, seq_of_parameters, **options
            ),
        )
