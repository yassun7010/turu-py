from typing import Iterator, List, NamedTuple, Optional, Sequence, Type, TypeVar

from turu.core.protocols.cursor import CursorProtocol, _Parameters
from typing_extensions import Self, override

RowType = TypeVar("RowType", bound=NamedTuple)


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
            return map(row_type._make, self.execute(operation))
        else:
            return map(row_type._make, self.execute(operation, parameters))

    def executemany_typing(
        self,
        row_type: Type[RowType],
        operation: str,
        seq_of_parameters: Sequence[_Parameters],
    ) -> Iterator[RowType]:
        return map(row_type._make, self.executemany(operation, seq_of_parameters))

    def fetchone_typing(self, row_type: Type[RowType]) -> RowType:
        return row_type._make(self.fetchone())

    def fetchmany_typing(
        self, row_type: Type[RowType], size: Optional[int] = None
    ) -> Iterator[RowType]:
        return map(row_type._make, self.fetchmany(size))

    def fetchall_typing(self, row_type: Type[RowType]) -> Iterator[RowType]:
        return map(row_type._make, self.fetchall())
