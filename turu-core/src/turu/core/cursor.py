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
from turu.core.protocols.cursor import CursorProtocol, _Parameters
from turu.core.protocols.dataclass import Dataclass
from typing_extensions import Self, override

RowType = TypeVar("RowType", bound=Union[Tuple[Any], Dataclass, PydanticModel])


class Cursor(Generic[RowType, _Parameters], CursorProtocol[_Parameters]):
    @override
    def execute(self, operation: str, parameters: _Parameters = ..., /) -> Self:
        ...

    @override
    def executemany(
        self, operation: str, seq_of_parameters: Sequence[_Parameters], /
    ) -> Self:
        ...

    def execute_typing(
        self,
        row_type: Type[RowType],
        operation: str,
        parameters: Optional[_Parameters] = None,
        /,
    ) -> "Cursor[RowType, _Parameters]":
        ...

    def executemany_typing(
        self,
        row_type: Type[RowType],
        operation: str,
        seq_of_parameters: Sequence[_Parameters],
    ) -> "Cursor[RowType, _Parameters]":
        ...

    @override
    def fetchone(self) -> Optional[RowType]:
        ...

    @override
    def fetchmany(self, size: Optional[int] = None) -> List[RowType]:
        ...

    @override
    def fetchall(self) -> List[RowType]:
        ...

    @override
    def __iter__(self) -> Self:
        ...

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
