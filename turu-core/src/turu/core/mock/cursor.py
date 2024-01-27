from typing import Any, Iterator, List, Optional, Sequence, Tuple, Type

from turu.core.cursor import (
    Cursor,
    GenericNewRowType,
    GenericRowType,
    Parameters,
)
from turu.core.mock.exception import (
    TuruMockUnexpectedFetchError,
)
from turu.core.mock.store import TuruMockStore
from typing_extensions import Self, override


class MockCursor(Cursor[GenericRowType, Parameters]):
    def __init__(
        self,
        store: TuruMockStore,
        *,
        row_count: Optional[int] = None,
        rows_iter: Optional[Iterator] = None,
        row_type: Optional[Type[GenericRowType]] = None,
    ) -> None:
        self._turu_mock_store = store
        self._rowcount = row_count
        self._rows_iter = rows_iter
        self._row_type = row_type
        self._arraysize = 1

    @property
    def rowcount(self) -> int:
        return self._rowcount or -1

    @property
    def arraysize(self) -> int:
        return self._arraysize

    @arraysize.setter
    def arraysize(self, size: int) -> None:
        self._arraysize = size

    @override
    def close(self) -> None:
        pass

    @override
    def execute(
        self, operation: str, parameters: Optional[Parameters] = None, /
    ) -> "MockCursor[Tuple[Any], Parameters]":
        return self._make_new_mock_cursor(None)

    @override
    def executemany(
        self, operation: str, seq_of_parameters: Sequence[Parameters], /
    ) -> "MockCursor[Tuple[Any], Parameters]":
        return self._make_new_mock_cursor(None)

    @override
    def execute_map(
        self,
        row_type: Type[GenericNewRowType],
        operation: str,
        parameters: Optional[Parameters] = None,
        /,
    ) -> "MockCursor[GenericNewRowType, Parameters]":
        return self._make_new_mock_cursor(row_type)

    @override
    def executemany_map(
        self,
        row_type: Type[GenericNewRowType],
        operation: str,
        seq_of_parameters: Sequence[Parameters],
        /,
    ) -> "MockCursor[GenericNewRowType, Parameters]":
        return self._make_new_mock_cursor(row_type)

    @override
    def fetchone(self) -> Optional[GenericRowType]:
        if self._rows_iter is None:
            return None

        return next(self._rows_iter, None)

    @override
    def fetchmany(self, size: Optional[int] = None) -> List[GenericRowType]:
        if self._rows_iter is None:
            raise TuruMockUnexpectedFetchError()

        result = []
        for _ in range(size if size is not None else self.arraysize):
            try:
                result.append(next(self._rows_iter))
            except StopIteration:
                break

        return result

    @override
    def fetchall(self) -> List[GenericRowType]:
        if self._rows_iter is None:
            raise TuruMockUnexpectedFetchError()

        return list(self._rows_iter)

    @override
    def __iter__(self) -> Self:
        if self._rows_iter is None:
            raise TuruMockUnexpectedFetchError()
        return self

    @override
    def __next__(self) -> GenericRowType:
        if self._rows_iter is None:
            raise TuruMockUnexpectedFetchError()

        return next(self._rows_iter)

    def _make_new_mock_cursor(
        self, row_type: Optional[Type[GenericNewRowType]]
    ) -> "MockCursor":
        responses = self._turu_mock_store.provide_response(row_type)

        if responses is None:
            return self.__class__(
                self._turu_mock_store,
                row_type=row_type,
            )

        else:
            return self.__class__(
                self._turu_mock_store,
                row_count=len(responses),
                rows_iter=iter(responses),
                row_type=row_type,
            )
