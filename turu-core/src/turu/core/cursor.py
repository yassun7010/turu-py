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
)

from turu.core._feature_flags import USE_PYDANTIC, PydanticModel
from turu.core.exception import TuruRowTypeMismatchError
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
        ...

    @abstractmethod
    def executemany_map(
        self,
        row_type: Type[GenericNewRowType],
        operation: str,
        seq_of_parameters: Sequence[Parameters],
        /,
    ) -> "Cursor[GenericNewRowType, Parameters]":
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

    if issubclass(row_type, tuple):
        return row_type._make(row)  # type: ignore

    if USE_PYDANTIC:
        if issubclass(row_type, PydanticModel):
            return row_type(
                **{key: data for key, data in zip(row_type.model_fields.keys(), row)}
            )

    raise TuruRowTypeMismatchError(row_type, row.__class__)
