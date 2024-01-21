import pathlib
from typing import Any, Optional, Sequence, Type, Union, cast, overload

import turu.core.connection
import turu.core.cursor
import turu.core.mock
import turu.snowflake.cursor
import turu.snowflake.mock_cursor
from turu.core.mock.connection import CSVOptions
from turu.core.mock.exception import TuruCsvHeaderOptionRequiredError
from turu.snowflake.features import PandasDataFlame, PyArrowTable
from typing_extensions import Never, Self, Unpack, override

from .connection import Connection


class MockConnection(Connection, turu.core.mock.MockConnection):
    def __init__(self, *args, **kwargs):
        turu.core.mock.MockConnection.__init__(self)

    @override
    def cursor(self) -> "turu.snowflake.mock_cursor.MockCursor[Never]":
        return turu.snowflake.mock_cursor.MockCursor(self._turu_mock_store)

    @overload
    def inject_response(
        self,
        row_type: None,
        response: Union[
            Sequence[turu.core.cursor.GenericRowType],
            Optional[turu.core.cursor.GenericRowType],
            Exception,
        ] = None,
    ) -> Self:
        ...

    @overload
    def inject_response(
        self,
        row_type: Type[turu.core.cursor.GenericRowType],
        response: Union[
            Sequence[turu.core.cursor.GenericRowType],
            turu.core.cursor.GenericRowType,
            Exception,
        ],
    ) -> Self:
        ...

    @override
    def inject_response(
        self,
        row_type: Optional[Type[turu.core.cursor.GenericRowType]],  # type: ignore
        response: Union[
            Sequence[Any],
            Any,
            Exception,
        ] = None,
    ) -> Self:
        if row_type is not None and isinstance(response, row_type):
            response = (response,)

        self._turu_mock_store.inject_response(
            row_type,
            response,
        )

        return self

    def inject_response_from_csv(
        self,
        row_type: Optional[Type[turu.core.cursor.GenericRowType]],  # type: ignore
        filepath: Union[str, pathlib.Path],
        **options: Unpack[CSVOptions],
    ) -> Self:
        if row_type is not None:
            if issubclass(row_type, PandasDataFlame):
                import pandas

                pd_options = {}
                if not options.get("header", True):
                    pd_options["header"] = None

                self._turu_mock_store.inject_response(
                    row_type,
                    pandas.read_csv(filepath, **pd_options),
                )

            elif issubclass(row_type, PyArrowTable):
                import pyarrow.csv

                if not options.get("header", True):
                    raise TuruCsvHeaderOptionRequiredError(row_type)

                self._turu_mock_store.inject_response(
                    row_type,
                    cast(Any, pyarrow.csv.read_csv(filepath)),
                )

            else:
                super().inject_response_from_csv(row_type, filepath, **options)

        return self
