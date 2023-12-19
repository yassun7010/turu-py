from abc import abstractmethod
from typing import Any, Optional, Sequence, Type, overload

from turu.core.cursor import RowType
from turu.core.protocols.connection import ConnectionProtocol
from turu.mock.store import TuruMockStore
from typing_extensions import Self

from .cursor import MockCursor


class MockConnection(ConnectionProtocol):
    def __init__(self, store: Optional[TuruMockStore] = None):
        self._turu_mock_store = store or TuruMockStore()

    @overload
    def inject_response(
        self,
        row_type: None,
        response: None = None,
    ) -> Self:
        ...

    @overload
    def inject_response(
        self,
        row_type: None,
        response: Sequence[Any],
    ) -> Self:
        ...

    @overload
    def inject_response(
        self,
        row_type: Type[RowType],
        response: Sequence[RowType],
    ) -> Self:
        ...

    @overload
    def inject_response(
        self,
        row_type: Type[RowType],
        response: Exception,
    ) -> Self:
        ...

    def inject_response(
        self,
        row_type,
        response=None,
    ):
        self._turu_mock_store.inject_response(row_type, response)
        return self

    def close(self) -> None:
        pass

    @abstractmethod
    def cursor(self) -> MockCursor:
        ...
