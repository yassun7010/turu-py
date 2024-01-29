from abc import abstractmethod
from dataclasses import is_dataclass
from typing import (
    Any,
    Generic,
    List,
    Optional,
    Sequence,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
)

from turu.core.exception import TuruRowTypeMismatchError
from turu.core.features import USE_PYDANTIC, PydanticModel
from turu.core.protocols.cursor import CursorProtocol, Parameters
from turu.core.protocols.dataclass import Dataclass
from typing_extensions import Self, override

RowType = Union[Tuple[Any], Dataclass, PydanticModel]
GenericRowType = TypeVar("GenericRowType", bound=RowType)
GenericNewRowType = TypeVar("GenericNewRowType", bound=RowType)


class Cursor(Generic[GenericRowType, Parameters], CursorProtocol[Parameters]):
    @property
    @abstractmethod
    def rowcount(self) -> int:
        ...

    @property
    @abstractmethod
    def arraysize(self) -> int:
        ...

    @arraysize.setter
    @abstractmethod
    def arraysize(self, size: int) -> None:
        ...

    @abstractmethod
    def close(self) -> None:
        ...

    @abstractmethod
    def execute(
        self, operation: str, parameters: Optional[Parameters] = None, /
    ) -> "Cursor":
        ...

    @abstractmethod
    def executemany(
        self, operation: str, seq_of_parameters: Sequence[Parameters], /
    ) -> "Cursor":
        ...

    @abstractmethod
    def execute_map(
        self,
        row_type: Type[GenericNewRowType],
        operation: str,
        parameters: Optional[Parameters] = None,
        /,
    ) -> "Cursor[GenericNewRowType, Parameters]":
        """
        Execute a database operation (query or command) and map each row to a `row_type`.

        Parameters:
            row_type: The type of the row that will be returned.
            operation: A database operation (query or command).
            parameters: Parameters may be provided as sequence or mapping and will be bound to variables in the operation.

        Returns:
            A cursor that holds a reference to an operation.
        """
        ...

    @abstractmethod
    def executemany_map(
        self,
        row_type: Type[GenericNewRowType],
        operation: str,
        seq_of_parameters: Sequence[Parameters],
        /,
    ) -> "Cursor[GenericNewRowType, Parameters]":
        """Execute a database operation (query or command) against all parameter sequences or mappings.

        Parameters:
            row_type: The type of the row that will be returned.
            operation: A database operation (query or command).
            seq_of_parameters: Parameters may be provided as sequence or mapping and will be bound to variables in the operation.

        Returns:
            A cursor that holds a reference to an operation.
        """
        ...

    @abstractmethod
    def fetchone(self) -> Optional[GenericRowType]:
        ...

    @abstractmethod
    def fetchmany(self, size: Optional[int] = None) -> List[GenericRowType]:
        ...

    @abstractmethod
    def fetchall(self) -> List[GenericRowType]:
        ...

    @override
    def __iter__(self) -> Self:
        return self

    @abstractmethod
    def __next__(self) -> GenericRowType:
        ...

    @override
    def __enter__(self):
        return self

    @override
    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


GenericCursor = TypeVar("GenericCursor", bound=Cursor)


def map_row(row_type: Optional[Type[GenericRowType]], row: Any) -> GenericRowType:
    if row_type is None:
        return row

    if is_dataclass(row_type):
        return row_type(
            **{
                key: data
                for key, data in zip(row_type.__dataclass_fields__.keys(), row)
            }
        )  # type: ignore

    elif issubclass(row_type, tuple):
        return row_type._make(row)  # type: ignore

    elif USE_PYDANTIC and issubclass(row_type, PydanticModel):
        return row_type(
            **{
                key: data
                for key, data in zip(
                    cast(PydanticModel, row_type).model_fields.keys(), row
                )
            }
        )  # type: ignore

    raise TuruRowTypeMismatchError(row_type, row.__class__)
