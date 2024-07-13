from typing import Any, List, Optional, Sequence, Tuple, Type, Union, overload

from turu.core.cursor import GenericRowType
from turu.core.mock.exception import (
    TuruMockResponseTypeMismatchError,
    TuruMockStoreDataNotFoundError,
)
from turu.core.tag import Tag


class TuruMockStore:
    def __init__(self):
        self._data: List[Tuple[Optional[Type], Union[Sequence, None, Exception]]] = []
        self._counter = 0

    def inject_operation_with_tag(
        self, tag: Type[Tag], exception: Optional[Exception] = None
    ):
        self._data.append((tag, exception))

    @overload
    def inject_response(
        self,
        row_type: None,
        response: Union[GenericRowType, Sequence[GenericRowType], None, Exception],
    ): ...

    @overload
    def inject_response(
        self,
        row_type: Type[GenericRowType],
        response: Union[GenericRowType, Sequence[GenericRowType], Exception],
    ): ...

    def inject_response(self, row_type, response):
        if row_type is not None and isinstance(response, row_type):
            response = (response,)

        self._data.append((row_type, response))

    def provide_response(
        self,
        row_type: Optional[Type],
    ) -> "Optional[Sequence[Any]]":
        self._counter += 1

        if len(self._data) == 0:
            raise TuruMockStoreDataNotFoundError(self._counter)

        store_row_type, store_response = self._data.pop(0)

        if store_row_type is not row_type and (str(row_type) != str(store_row_type)):
            raise TuruMockResponseTypeMismatchError(
                row_type, store_row_type, self._counter
            )

        if isinstance(store_response, Exception):
            raise store_response

        return store_response
