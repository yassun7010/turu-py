from typing import Optional, Sequence, Type, Union, overload

from turu.core.cursor import RowType
from turu.mock.extension import (
    TuruMockResponseTypeMismatchError,
    TuruMockStoreDataNotFoundError,
)


class TuruMockStore:
    def __init__(self):
        self._cursor = 0
        self._data = []

    @overload
    def inject_response(
        self,
        row_type: None,
        response: None = None,
    ):
        ...

    @overload
    def inject_response(
        self,
        row_type: Type[RowType],
        response: Union[Sequence[RowType], Exception],
    ):
        ...

    def inject_response(
        self,
        row_type: Optional[Type[RowType]],
        response: Union[Sequence[RowType], None, Exception] = None,
    ):
        self._data.append((row_type, response))

    def provide_response(
        self,
        row_type: Optional[Type],
    ) -> list:
        if len(self._data[self._cursor :]) == 0:
            raise TuruMockStoreDataNotFoundError()
        response = self._data[self._cursor]
        self._cursor += 1

        if response[0] is not row_type:
            raise TuruMockResponseTypeMismatchError(row_type, response[0])

        if isinstance(response[1], Exception):
            raise response[1]

        return response[1]
