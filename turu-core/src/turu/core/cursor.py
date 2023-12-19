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
from turu.core.exception import TuruRowTypeError
from turu.core.protocols.cursor import CursorProtocol, Parameters
from turu.core.protocols.dataclass import Dataclass
from typing_extensions import Self, override

RowType = TypeVar("RowType", bound=Union[Tuple[Any], Dataclass, PydanticModel])
NewRowType = TypeVar("NewRowType", bound=Union[Tuple[Any], Dataclass, PydanticModel])


class Cursor(Generic[RowType, Parameters], CursorProtocol[Parameters]):
    @property
    @abstractmethod
    def rowcount(self) -> int:
        ...

    @override
    def close(self) -> None:
        pass

    @override
    def execute(self, operation: str, parameters: Parameters = ..., /) -> Self:
        ...

    @override
    def executemany(
        self, operation: str, seq_of_parameters: Sequence[Parameters], /
    ) -> Self:
        ...

    def execute_map(
        self,
        row_type: Type[RowType],
        operation: str,
        parameters: Optional[Parameters] = None,
    ) -> "Cursor":
        ...

    def executemany_map(
        self,
        row_type: Type[RowType],
        operation: str,
        seq_of_parameters: Sequence[Parameters],
    ) -> "Cursor":
        ...

    @override
    def fetchone(self) -> Optional[RowType]:
        ...

    @override
    def fetchmany(self, size: int = 1) -> List[RowType]:
        ...

    @override
    def fetchall(self) -> List[RowType]:
        ...

    @override
    def __iter__(self) -> Self:
        return self

    @override
    def __next__(self) -> RowType:
        ...


def map_row(row_type: Optional[Type[RowType]], row: Any) -> RowType:
    if row_type is None:
        return row

    if issubclass(row_type, tuple):
        return row_type._make(row)  # type: ignore

    if is_dataclass(row_type):
        return row_type(
            **{
                key: data
                for key, data in zip(row_type.__dataclass_fields__.keys(), row)
            }
        )  # type: ignore

    if USE_PYDANTIC:
        if issubclass(row_type, PydanticModel):
            return row_type(
                **{key: data for key, data in zip(row_type.model_fields.keys(), row)}
            )

    raise TuruRowTypeError(row_type, row.__class__)
