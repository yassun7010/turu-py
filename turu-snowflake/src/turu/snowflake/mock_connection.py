import pathlib
from typing import Any, Optional, Sequence, Type, Union, cast, overload

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

from .connection import Connection


class MockConnection(turu.core.mock.MockConnection, Connection):
    def __init__(self, *args, **kwargs):
        turu.core.mock.MockConnection.__init__(self)

    @override
    def cursor(self) -> "turu.snowflake.mock_cursor.MockCursor[Never, Never, Never]":
        return turu.snowflake.mock_cursor.MockCursor(self._turu_mock_store)

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

                pd_options = {}
                if not options.get("header", True):
                    pd_options["header"] = None

                self.inject_response(
                    row_type,  # type: ignore
                    pandas.read_csv(filepath, **pd_options),
                )  # type: ignore

            elif issubclass(row_type, PyArrowTable):
                import pyarrow.csv

                if not options.get("header", True):
                    raise TuruCsvHeaderOptionRequiredError(row_type)

                self.inject_response(
                    row_type,
                    cast(Any, pyarrow.csv.read_csv(filepath)),
                )

            else:
                super().inject_response_from_csv(row_type, filepath, **options)

        return self
