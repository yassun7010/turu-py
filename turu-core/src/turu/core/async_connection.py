from abc import abstractmethod
from typing import Any, Optional, Sequence, Tuple, Type

import turu.core.async_cursor
from turu.core.protocols.async_connection import AsyncConnectionProtocol
from turu.core.protocols.async_cursor import Parameters
from typing_extensions import Never, Self


class AsyncConnection(AsyncConnectionProtocol):
    @classmethod
    @abstractmethod
    async def connect(cls, *args: Any, **kwargs: Any) -> Self:
        """Connect to a database."""
        ...

    @classmethod
    @abstractmethod
    async def connect_from_env(cls, *args: Any, **kwargs: Any) -> Self:
        """Connect to a database using environment variables."""
        ...

    @abstractmethod
    async def close(self) -> None:
        ...

    @abstractmethod
    async def commit(self) -> None:
        ...

    @abstractmethod
    async def rollback(self) -> None:
        ...

    @abstractmethod
    async def cursor(self) -> turu.core.async_cursor.AsyncCursor[Never, Any]:
        ...

    async def execute(
        self,
        operation: str,
        parameters: Optional[Parameters] = None,
        /,
    ) -> turu.core.async_cursor.AsyncCursor[Tuple[Any], Parameters]:
        """Prepare and execute a database operation (query or command).

        This is not defined in [PEP 249](https://peps.python.org/pep-0249/),
        but is simply a convenient shortcut to `.cursor().execute()`.

        Parameters:
            operation: A database operation (query or command).
            parameters: Parameters may be provided as sequence or mapping and will be bound to variables in the operation.

        Returns:
            A cursor that holds a reference to an operation.
        """

        return await (await self.cursor()).execute(operation, parameters)

    async def executemany(
        self,
        operation: str,
        seq_of_parameters: Sequence[Parameters],
        /,
    ) -> turu.core.async_cursor.AsyncCursor[Tuple[Any], Parameters]:
        """Prepare a database operation (query or command)
        and then execute it against all parameter sequences or mappings.

        This is not defined in [PEP 249](https://peps.python.org/pep-0249/),
        but is simply a convenient shortcut to `.cursor().executemany()`.

        Parameters:
            operation: A database operation (query or command).
            seq_of_parameters: Parameters may be provided as sequence or mapping and will be bound to variables in the operation.

        Returns:
            A cursor that holds a reference to an operation.
        """

        return await (await self.cursor()).executemany(operation, seq_of_parameters)

    async def execute_map(
        self,
        row_type: Type[turu.core.async_cursor.GenericNewRowType],
        operation: str,
        parameters: Optional[Parameters] = None,
        /,
    ) -> turu.core.async_cursor.AsyncCursor[
        turu.core.async_cursor.GenericNewRowType, Parameters
    ]:
        """
        Execute a database operation (query or command) and map each row to a `row_type`.

        This is not defined in [PEP 249](https://peps.python.org/pep-0249/),
        but is simply a convenient shortcut to `.cursor().execute_map()`.

        Parameters:
            row_type: The type of the row that will be returned.
            operation: A database operation (query or command).
            parameters: Parameters may be provided as sequence or mapping and will be bound to variables in the operation.

        Returns:
            A cursor that holds a reference to an operation.
        """

        return await (await self.cursor()).execute_map(row_type, operation, parameters)

    async def executemany_map(
        self,
        row_type: Type[turu.core.async_cursor.GenericNewRowType],
        operation: str,
        seq_of_parameters: Sequence[Parameters],
        /,
    ) -> turu.core.async_cursor.AsyncCursor[
        turu.core.async_cursor.GenericNewRowType, Parameters
    ]:
        """Execute a database operation (query or command) against all parameter sequences or mappings.

        This is not defined in [PEP 249](https://peps.python.org/pep-0249/),
        but is simply a convenient shortcut to `.cursor().executemany_map()`.

        Parameters:
            row_type: The type of the row that will be returned.
            operation: A database operation (query or command).
            seq_of_parameters: Parameters may be provided as sequence or mapping and will be bound to variables in the operation.

        Returns:
            A cursor that holds a reference to an operation.
        """

        return await (await self.cursor()).executemany_map(
            row_type, operation, seq_of_parameters
        )
