from typing import Any, List, Optional, Sequence, Tuple, Type, Union, overload

from turu.core.cursor import GenericRowType
from turu.core.mock.exception import (
    TuruMockResponseTypeMismatchError,
    TuruMockStoreDataNotFoundError,
)


class TuruMockStore:
    def __init__(self):
        self._data: List[Tuple[Optional[Type], Sequence | None | Exception]] = []
        self._counter = 0

    @overload
    def inject_response(
        self,
        row_type: None,
        response: Union[Optional[Sequence[GenericRowType]], Exception],
    ):
        ...

    @overload
    def inject_response(
        self,
        row_type: Type[GenericRowType],
        response: Union[GenericRowType, Sequence[GenericRowType], Exception],
    ):
        ...

    def inject_response(
        self,
        row_type: Optional[Type[GenericRowType]],
        response: Union[Optional[Union[Sequence[GenericRowType]]], Exception],
    ):
        if row_type is not None and isinstance(response, row_type):
            response = (response,)

        self._data.append((row_type, response))

    def provide_response(
        self,
        row_type: Optional[Type],
    ) -> "Sequence[Any] | None":
        self._counter += 1

        if len(self._data) == 0:
            raise TuruMockStoreDataNotFoundError(self._counter)

        _row_type, _response = self._data.pop(0)

        if _row_type is not row_type:
            raise TuruMockResponseTypeMismatchError(row_type, _row_type, self._counter)

        if isinstance(_response, Exception):
            raise _response

        return _response
