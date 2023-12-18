from typing import (
    Any,
    Generic,
    Iterator,
    Optional,
    Sequence,
    Tuple,
    Type,
    TypeVar,
    Union,
)

from turu.core.cursor import (
    Cursor,
    Dataclass,
    PydanticModel,
    RowType,
    _Parameters,
    map_row,
)
from turu.mock.extension import (
    TuruMockUnexpectedFetchError,
)
from turu.mock.store import TuruMockStore
from typing_extensions import Self, override

NewRowType = TypeVar("NewRowType", bound=Union[Tuple[Any], Dataclass, PydanticModel])


class MockCursor(Generic[RowType, _Parameters], Cursor[RowType, _Parameters]):
    def __init__(
        self,
        store: TuruMockStore,
        *,
        row_count: Optional[int] = None,
        rows: Optional[Iterator] = None,
        row_type: Optional[Type[RowType]] = None,
    ) -> None:
        self._turu_mock_store = store
        self._rowcount = row_count
        self._turu_mock_rows = rows
        self._row_type = row_type

    @property
    def rowcount(self) -> int:
        return self._rowcount or -1

    @override
    def execute(
        self, operation: str, parameters: Optional[_Parameters] = None
    ) -> "MockCursor[Any, _Parameters]":
        return self._make_new_cursor(None)

    @override
    def executemany(
        self, operation: str, seq_of_parameters: Sequence, /
    ) -> "MockCursor[Any, _Parameters]":
        return self._make_new_cursor(None)

    @override
    def execute_typing(
        self,
        row_type: Type[NewRowType],
        operation: str,
        parameters: Optional[_Parameters] = None,
    ) -> "MockCursor[NewRowType, _Parameters]":
        return self._make_new_cursor(row_type)

    @override
    def executemany_typing(
        self,
        row_type: Type[NewRowType],
        operation: str,
        seq_of_parameters: Sequence[_Parameters],
        /,
    ) -> "MockCursor[NewRowType, _Parameters]":
        return self._make_new_cursor(row_type)

    @override
    def fetchone(self) -> Optional[RowType]:
        if self._turu_mock_rows is None:
            return None

        try:
            return next(self._turu_mock_rows)

        except StopIteration:
            return None

    @override
    def fetchmany(self, size: Optional[int] = None) -> Sequence[RowType]:
        if self._turu_mock_rows is None:
            raise TuruMockUnexpectedFetchError()

        return [
            map_row(self._row_type, row)
            for row, _ in zip(self._turu_mock_rows, range(size or self.rowcount))
        ]

    @override
    def fetchall(self) -> Sequence[RowType]:
        if self._turu_mock_rows is None:
            raise TuruMockUnexpectedFetchError()

        return [map_row(self._row_type, row) for row in self._turu_mock_rows]

    @override
    def __iter__(self) -> Self:
        if self._turu_mock_rows is None:
            raise TuruMockUnexpectedFetchError()
        return self

    @override
    def __next__(self) -> RowType:
        if self._turu_mock_rows is None:
            raise TuruMockUnexpectedFetchError()

        return next(self._turu_mock_rows)

    def _make_new_cursor(
        self, row_type: Optional[Type]
    ) -> Union[Tuple[int, list], Tuple[None, None]]:
        responses = self._turu_mock_store.provide_response(row_type)

        if responses is None:
            return MockCursor(responses, row_type=row_type)
        else:
            return MockCursor(
                responses,
                row_count=len(responses),
                rows=iter(responses),
                row_type=row_type,
            )
