import csv
import pathlib
from abc import abstractmethod
from typing import (
    Any,
    Optional,
    Sequence,
    Type,
    TypedDict,
    Union,
    overload,
)

import turu.core.connection
from turu.core.cursor import GenericRowType, map_row
from turu.core.mock.store import TuruMockStore
from typing_extensions import Never, NotRequired, Self, Unpack

from .cursor import MockCursor


class CSVOptions(TypedDict):
    header: NotRequired[bool]


class MockConnection(turu.core.connection.Connection):
    def __init__(self, store: Optional[TuruMockStore] = None):
        self._turu_mock_store = store or TuruMockStore()

    @classmethod
    def connect(cls, *args: Any, **kwargs: Any) -> Self:
        return cls()

    @classmethod
    def connect_from_env(cls, *args: Any, **kwargs: Any) -> Self:
        return cls()

    def chain(self) -> Self:
        """this method is just for code formatting by black."""

        return self

    @overload
    def inject_response(
        self,
        row_type: None,
        response: Union[Optional[Sequence[Any]], Exception] = None,
    ) -> Self:
        ...

    @overload
    def inject_response(
        self,
        row_type: Type[GenericRowType],
        response: Union[Sequence[GenericRowType], Exception],
    ) -> Self:
        ...

    def inject_response(
        self,
        row_type,
        response=None,
    ):
        self._turu_mock_store.inject_response(row_type, response)
        return self

    @overload
    def inject_response_from_csv(
        self,
        row_type: None,
        filepath: Union[str, pathlib.Path],
        **options: Unpack[CSVOptions],
    ) -> Self:
        ...

    @overload
    def inject_response_from_csv(
        self,
        row_type: Type[GenericRowType],
        filepath: Union[str, pathlib.Path],
        **options: Unpack[CSVOptions],
    ) -> Self:
        ...

    def inject_response_from_csv(
        self,
        row_type: Optional[Type[GenericRowType]],
        filepath: Union[str, pathlib.Path],
        **options: Unpack[CSVOptions],
    ):
        with open(filepath, "r") as file:
            reader = csv.reader(file)

            if options.get("header", True):
                next(reader)

            response = [map_row(row_type, row) for row in reader]

        self.inject_response(row_type, response)

        return self

    def close(self) -> None:
        pass

    def commit(self) -> None:
        pass

    def rollback(self) -> None:
        pass

    @abstractmethod
    def cursor(self) -> MockCursor[Never, Any]:
        ...
