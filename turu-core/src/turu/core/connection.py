from abc import abstractmethod
from typing import Any, Optional, Sequence, Tuple, Type

import turu.core.cursor
from turu.core.protocols.connection import ConnectionProtocol
from turu.core.protocols.cursor import Parameters
from typing_extensions import Never, Self, override


class Connection(ConnectionProtocol):
    @classmethod
    @abstractmethod
    def connect(cls, *args: Any, **kwargs: Any) -> Self:
        """Connect to a database."""
        ...

    @classmethod
    @abstractmethod
    def connect_from_env(cls, *args: Any, **kwargs: Any) -> Self:
        """Connect to a database using environment variables."""
        ...

    @abstractmethod
    @override
    def close(self) -> None:
        ...

    @abstractmethod
    @override
    def commit(self) -> None:
        ...

    @abstractmethod
    @override
    def rollback(self) -> None:
        ...

    @abstractmethod
    @override
    def cursor(self) -> turu.core.cursor.Cursor[Never, Any]:
        ...

    def execute(
        self,
        operation: str,
        parameters: Optional[Parameters] = None,
        /,
    ) -> turu.core.cursor.Cursor[Tuple[Any], Parameters]:
        """Prepare and execute a database operation (query or command).

        This is not defined in [PEP 249](https://peps.python.org/pep-0249/),
        but is simply a convenient shortcut to `.cursor().execute()`.

        Parameters:
            operation: A database operation (query or command).
            parameters: Parameters may be provided as sequence or mapping and will be bound to variables in the operation.

        Returns:
            A cursor that holds a reference to an operation.
        """

        return self.cursor().execute(operation, parameters)

    def executemany(
        self,
        operation: str,
        seq_of_parameters: Sequence[Parameters],
        /,
    ) -> turu.core.cursor.Cursor[Tuple[Any], Parameters]:
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

        return self.cursor().executemany(operation, seq_of_parameters)

    def execute_map(
        self,
        row_type: Type[turu.core.cursor.GenericNewRowType],
        operation: str,
        parameters: Optional[Parameters] = None,
        /,
    ) -> turu.core.cursor.Cursor[turu.core.cursor.GenericNewRowType, Parameters]:
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

        return self.cursor().execute_map(row_type, operation, parameters)

    def executemany_map(
        self,
        row_type: Type[turu.core.cursor.GenericNewRowType],
        operation: str,
        seq_of_parameters: Sequence[Parameters],
        /,
    ) -> turu.core.cursor.Cursor[turu.core.cursor.GenericNewRowType, Parameters]:
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

        return self.cursor().executemany_map(row_type, operation, seq_of_parameters)
