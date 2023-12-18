from typing import Any, Generic, Iterator, List, Optional, Sequence, Type

from turu.core.cursor import (
    Cursor,
    NewRowType,
    Parameters,
    RowType,
    map_row,
)
from turu.mock.extension import (
    TuruMockUnexpectedFetchError,
)
from turu.mock.store import TuruMockStore
from typing_extensions import Self, override


class MockCursor(Generic[RowType, Parameters], Cursor[RowType, Parameters]):
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
        self, operation: str, parameters: Optional[Parameters] = None
    ) -> "MockCursor[Any, Parameters]":
        return self._make_new_cursor(None)

    @override
    def executemany(
        self, operation: str, seq_of_parameters: Sequence[Parameters], /
    ) -> "MockCursor[Any, Parameters]":
        return self._make_new_cursor(None)

    @override
    def execute_map(
        self,
        row_type: Type[NewRowType],
        operation: str,
        parameters: Optional[Parameters] = None,
    ) -> "MockCursor[NewRowType, Parameters]":
        return self._make_new_cursor(row_type)

    @override
    def executemany_map(
        self,
        row_type: Type[NewRowType],
        operation: str,
        seq_of_parameters: Sequence[Parameters],
    ) -> "MockCursor[NewRowType, Parameters]":
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
    def fetchmany(self, size: int = 1) -> List[RowType]:
        if self._turu_mock_rows is None:
            raise TuruMockUnexpectedFetchError()

        return [
            map_row(self._row_type, next(self._turu_mock_rows))
            for _ in range(size or self.rowcount)
        ]

    @override
    def fetchall(self) -> List[RowType]:
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

    def _make_new_cursor(self, row_type: Optional[Type]) -> "MockCursor":
        responses = self._turu_mock_store.provide_response(row_type)

        if responses is None:
            return MockCursor(responses, row_type=row_type)
        else:
            return MockCursor(
                self._turu_mock_store,
                row_count=len(responses),
                rows=iter(responses),
                row_type=row_type,
            )
