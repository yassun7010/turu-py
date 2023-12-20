# NOTE: Use ConnectionProtocol


from abc import abstractmethod
from typing import Any, Optional, Sequence, Type

from turu.core.cursor import Cursor, GenericNewRowType
from turu.core.protocols.connection import ConnectionProtocol
from turu.core.protocols.cursor import Parameters
from typing_extensions import Never


class Connection(ConnectionProtocol):
    @abstractmethod
    def close(self) -> None:
        ...

    @abstractmethod
    def commit(self) -> None:
        ...

    @abstractmethod
    def rollback(self) -> None:
        ...

    @abstractmethod
    def cursor(self) -> Cursor[Never, Any]:
        ...

    def execute(
        self,
        operation: str,
        parameters: Optional[Parameters] = None,
        /,
    ) -> Cursor[Never, Parameters]:
        return self.cursor().execute(operation, parameters)

    def executemany(
        self,
        operation: str,
        seq_of_parameters: Sequence[Parameters],
        /,
    ) -> Cursor[Never, Parameters]:
        return self.cursor().executemany(operation, seq_of_parameters)

    def execute_map(
        self,
        row_type: Type[GenericNewRowType],
        operation: str,
        parameters: Optional[Parameters] = None,
        /,
    ) -> Cursor[GenericNewRowType, Parameters]:
        return self.cursor().execute_map(row_type, operation, parameters)

    def executemany_map(
        self,
        row_type: Type[GenericNewRowType],
        operation: str,
        seq_of_parameters: Sequence[Parameters],
        /,
    ) -> Cursor[GenericNewRowType, Parameters]:
        return self.cursor().executemany_map(row_type, operation, seq_of_parameters)
