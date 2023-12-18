from typing import Any, Generic, Iterator, Optional, Sequence, Type

from turu.core.cursor import Cursor, RowType, _Parameters, map_row
from turu.mock.extension import (
    TuruMockStoreDataNotFoundError,
)
from turu.mock.store import TuruMockStore
from typing_extensions import Never, Self, override


class MockCursor(Generic[RowType, _Parameters], Cursor[RowType, _Parameters]):
    def __init__(
        self,
        store: TuruMockStore,
        cursor: Optional[Iterator] = None,
        row_type: Optional[Type[RowType]] = None,
    ) -> None:
        self._turu_mock_store = store
        self._turu_mock_cursor = cursor
        self._row_type = row_type
        self._rowcount = None

    @property
    def rowcount(self) -> int:
        return self._rowcount or -1

    @override
    def execute(
        self, operation: str, parameters: Optional[_Parameters] = None
    ) -> "MockCursor[Never, _Parameters]":
        self._update_response(None)

        return MockCursor(self._turu_mock_store, self._turu_mock_cursor)

    @override
    def executemany(
        self, operation: str, seq_of_parameters: Sequence, /
    ) -> "MockCursor[Never, _Parameters]":
        self._update_response(None)

        return MockCursor(self._turu_mock_store, self._turu_mock_cursor)

    @override
    def execute_typing(
        self, row_type: Type[RowType], operation: str, parameters: Optional[Any] = None
    ) -> "MockCursor[RowType, _Parameters]":
        self._update_response(row_type)

        return MockCursor(self._turu_mock_store, self._turu_mock_cursor, row_type)

    @override
    def executemany_typing(
        self, row_type: Type[RowType], operation: str, seq_of_parameters: Sequence, /
    ) -> "MockCursor[RowType, _Parameters]":
        self._update_response(row_type)

        return MockCursor(self._turu_mock_store, self._turu_mock_cursor, row_type)

    @override
    def fetchone(self) -> Optional[RowType]:
        if self._turu_mock_cursor is None:
            return None

        try:
            return next(self._turu_mock_cursor)

        except StopIteration:
            return None

    @override
    def fetchmany(self, size: Optional[int] = None) -> Sequence[RowType]:
        if self._turu_mock_cursor is None:
            raise TuruMockStoreDataNotFoundError()

        return map(
            lambda row: map_row(self._row_type, row),
            zip(self._turu_mock_cursor, range(size or self.rowcount)),
        )  # type: ignore

    @override
    def fetchall(self) -> Sequence[RowType]:
        if self._turu_mock_cursor is None:
            raise TuruMockStoreDataNotFoundError()

        return map(lambda row: map_row(self._row_type, row), self._turu_mock_cursor)  # type: ignore

    @override
    def __iter__(self) -> Self:
        if self._turu_mock_cursor is None:
            raise TuruMockStoreDataNotFoundError()
        return self

    @override
    def __next__(self) -> RowType:
        if self._turu_mock_cursor is None:
            raise TuruMockStoreDataNotFoundError()

        return next(self._turu_mock_cursor)

    def _update_response(self, row_type: Optional[Type]) -> None:
        responses = self._turu_mock_store.provide_response(row_type)

        if responses is None:
            return

        self._rowcount = len(responses)
        self._turu_mock_cursor = iter(responses)
