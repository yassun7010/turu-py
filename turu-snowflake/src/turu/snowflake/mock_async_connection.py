import pathlib
from typing import Any, Optional, Sequence, Type, Union, cast, overload

import turu.core.async_connection
import turu.core.connection
import turu.core.cursor
import turu.core.mock
import turu.snowflake.cursor
import turu.snowflake.mock_cursor
from turu.core.cursor import GenericRowType
from turu.core.mock.connection import CSVOptions
from turu.core.mock.exception import TuruCsvHeaderOptionRequiredError
from turu.snowflake.cursor import GenericPandasDataFlame, GenericPyArrowTable
from turu.snowflake.features import (
    GenericPanderaDataFrameModel,
    PandasDataFlame,
    PanderaDataFrameModel,
    PyArrowTable,
)
from typing_extensions import Never, Self, Unpack, override

from .async_connection import AsyncConnection
from .mock_async_cursor import MockAsyncCursor


class MockAsyncConnection(turu.core.mock.MockAsyncConnection, AsyncConnection):
    def __init__(self, *args, **kwargs):
        turu.core.mock.MockAsyncConnection.__init__(self)

    @override
    async def cursor(self) -> "MockAsyncCursor[Never, Never, Never]":
        return MockAsyncCursor(self._turu_mock_store)

    @overload
    def inject_response(
        self,
        row_type: None,
        response: Union[
            Sequence[GenericRowType], Optional[GenericRowType], Exception
        ] = None,
    ) -> Self:
        ...

    @overload
    def inject_response(
        self,
        row_type: Type[GenericRowType],
        response: Union[Sequence[GenericRowType], GenericRowType, Exception],
    ) -> Self:
        ...

    @overload
    def inject_response(
        self,
        row_type: Type[GenericPanderaDataFrameModel],
        response: Union[
            Sequence[GenericPandasDataFlame], GenericPandasDataFlame, Exception
        ],
    ) -> Self:
        ...

    @overload
    def inject_response(
        self,
        row_type: Type[GenericPandasDataFlame],
        response: Union[
            Sequence[GenericPandasDataFlame], GenericPandasDataFlame, Exception
        ],
    ) -> Self:
        ...

    @overload
    def inject_response(
        self,
        row_type: Type[GenericPyArrowTable],
        response: Union[Sequence[GenericPyArrowTable], GenericPyArrowTable, Exception],
    ) -> Self:
        ...

    @override
    def inject_response(  # type: ignore[override]
        self,
        row_type: Union[
            Type[GenericRowType],
            Type[GenericPanderaDataFrameModel],
            Type[GenericPandasDataFlame],
            Type[GenericPyArrowTable],
            None,
        ],
        response: Union[Sequence[Any], Any, Exception] = None,
    ) -> Self:
        if row_type is not None and isinstance(response, PandasDataFlame):
            response = (response,)

        self._turu_mock_store.inject_response(
            row_type,
            response,
        )

        return self

    def inject_response_from_csv(  # type: ignore[override]
        self,
        row_type: Union[
            Type[GenericRowType],
            Type[GenericPanderaDataFrameModel],
            Type[GenericPandasDataFlame],
            Type[GenericPyArrowTable],
        ],
        filepath: Union[str, pathlib.Path],
        **options: Unpack[CSVOptions],
    ) -> Self:
        if row_type is not None:
            if issubclass(row_type, (PandasDataFlame, PanderaDataFrameModel)):
                import pandas

                if options.get("header", True) is False:
                    raise TuruCsvHeaderOptionRequiredError(row_type)

                self.inject_response(
                    row_type,
                    cast(Any, pandas.read_csv(filepath, **options)),
                )

            elif issubclass(row_type, PyArrowTable):
                import pyarrow.csv

                self.inject_response(
                    row_type,
                    pyarrow.csv.read_csv(filepath, **options),  # type: ignore
                )

            else:
                super().inject_response_from_csv(row_type, filepath, **options)

        return self
