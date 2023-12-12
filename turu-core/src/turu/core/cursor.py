from dataclasses import is_dataclass
from typing import Iterator, List, Optional, Sequence, Type, TypeVar

from turu.core._feature_flags import USE_PYDANTIC, PydanticModel
from turu.core.exception import TuruRowTypeError
from turu.core.protocols.cursor import CursorProtocol, _Parameters
from typing_extensions import Self, override

RowType = TypeVar("RowType")


class Cursor(CursorProtocol[_Parameters]):
    @override
    def execute(self, operation: str, parameters: _Parameters = ..., /) -> Self:
        ...

    @override
    def executemany(
        self, operation: str, seq_of_parameters: Sequence[_Parameters], /
    ) -> Self:
        ...

    def fetchone(self) -> Self:
        ...

    def fetchmany(self, size: Optional[int] = None) -> List[Self]:
        ...

    def fetchall(self) -> List[Self]:
        ...

    @override
    def __iter__(self) -> Self:
        ...

    @override
    def __next__(self) -> Self:
        ...

    def execute_typing(
        self,
        row_type: Type[RowType],
        operation: str,
        parameters: Optional[_Parameters] = None,
        /,
    ) -> Iterator[RowType]:
        if parameters is None:
            return map(lambda row: _map_cursor(row_type, row), self.execute(operation))
        else:
            return map(
                lambda row: _map_cursor(row_type, row),
                self.execute(operation, parameters),
            )

    def executemany_typing(
        self,
        row_type: Type[RowType],
        operation: str,
        seq_of_parameters: Sequence[_Parameters],
    ) -> Iterator[RowType]:
        return map(
            lambda row: _map_cursor(row_type, row),
            self.executemany(operation, seq_of_parameters),
        )

    def fetchone_typing(self, row_type: Type[RowType]) -> RowType:
        return row_type(self.fetchone())

    def fetchmany_typing(
        self, row_type: Type[RowType], size: Optional[int] = None
    ) -> Iterator[RowType]:
        return map(lambda row: _map_cursor(row_type, row), self.fetchmany(size))

    def fetchall_typing(self, row_type: Type[RowType]) -> Iterator[RowType]:
        return map(lambda row: _map_cursor(row_type, row), self.fetchall())


def _map_cursor(row_type: Type[RowType], row: Cursor) -> RowType:
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
