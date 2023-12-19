from typing import Optional, Sequence, Type, Union, overload

from turu.core.cursor import GenericRowType
from turu.core.mock.exception import (
    TuruMockResponseTypeMismatchError,
    TuruMockStoreDataNotFoundError,
)


class TuruMockStore:
    def __init__(self):
        self._data = []

    @overload
    def inject_response(
        self,
        row_type: None,
        response: Union[Optional[Sequence[GenericRowType]], Exception] = None,
    ):
        ...

    @overload
    def inject_response(
        self,
        row_type: Type[GenericRowType],
        response: Union[Sequence[GenericRowType], Exception],
    ):
        ...

    def inject_response(
        self,
        row_type: Optional[Type[GenericRowType]],
        response: Union[Sequence[GenericRowType], None, Exception] = None,
    ):
        self._data.append((row_type, response))

    def provide_response(
        self,
        row_type: Optional[Type],
    ) -> list:
        if len(self._data) == 0:
            raise TuruMockStoreDataNotFoundError()

        _row_type, _response = self._data.pop()

        if _row_type is not row_type:
            raise TuruMockResponseTypeMismatchError(row_type, _row_type)

        if isinstance(_response, Exception):
            raise _response

        return _response
