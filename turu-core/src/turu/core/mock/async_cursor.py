from typing import Any, Iterator, List, Optional, Sequence, Tuple, Type

from turu.core.async_cursor import (
    AsyncCursor,
    GenericNewRowType,
    GenericRowType,
    Parameters,
)
from turu.core.mock.exception import (
    TuruMockUnexpectedFetchError,
)
from turu.core.mock.store import TuruMockStore
from typing_extensions import Self, override


class MockAsyncCursor(AsyncCursor[GenericRowType, Parameters]):
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
    async def close(self) -> None:
        pass

    @override
    async def execute(
        self, operation: str, parameters: Optional[Parameters] = None, /
    ) -> "MockAsyncCursor[Tuple[Any], Parameters]":
        return self._make_new_cursor(None)

    @override
    async def executemany(
        self, operation: str, seq_of_parameters: Sequence[Parameters], /
    ) -> "MockAsyncCursor[Tuple[Any], Parameters]":
        return self._make_new_cursor(None)

    @override
    async def execute_map(
        self,
        row_type: Type[GenericNewRowType],
        operation: str,
        parameters: Optional[Parameters] = None,
        /,
    ) -> "MockAsyncCursor[GenericNewRowType, Parameters]":
        return self._make_new_cursor(row_type)

    @override
    async def executemany_map(
        self,
        row_type: Type[GenericNewRowType],
        operation: str,
        seq_of_parameters: Sequence[Parameters],
        /,
    ) -> "MockAsyncCursor[GenericNewRowType, Parameters]":
        return self._make_new_cursor(row_type)

    @override
    async def fetchone(self) -> Optional[GenericRowType]:
        if self._rows_iter is None:
            return None

        try:
            return next(self._rows_iter)

        except (StopIteration, StopAsyncIteration):
            return None

    @override
    async def fetchmany(self, size: Optional[int] = None) -> List[GenericRowType]:
        if self._rows_iter is None:
            raise TuruMockUnexpectedFetchError()

        result = []
        for _ in range(size if size is not None else self.arraysize):
            try:
                result.append(self._rows_iter.__next__())
            except StopIteration:
                break

        return result

    @override
    async def fetchall(self) -> List[GenericRowType]:
        if self._rows_iter is None:
            raise TuruMockUnexpectedFetchError()

        return list(self._rows_iter)

    @override
    def __aiter__(self) -> Self:
        if self._rows_iter is None:
            raise TuruMockUnexpectedFetchError()
        return self

    @override
    async def __anext__(self) -> GenericRowType:
        if self._rows_iter is None:
            raise TuruMockUnexpectedFetchError()

        try:
            return next(self._rows_iter)
        except StopIteration as e:
            raise StopAsyncIteration from e

    def _make_new_cursor(
        self, row_type: Optional[Type[GenericNewRowType]]
    ) -> "MockAsyncCursor":
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
