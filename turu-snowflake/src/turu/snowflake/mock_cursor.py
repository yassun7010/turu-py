from typing import (
    Any,
    Iterator,
    Optional,
    Sequence,
    Tuple,
    Type,
    Union,
    cast,
    overload,
)

import turu.core.cursor
import turu.core.mock
from turu.core.cursor import GenericNewRowType
from turu.snowflake.features import (
    GenericNewPandasDataFrame,
    GenericNewPanderaDataFrameModel,
    GenericNewPyArrowTable,
    GenericPandasDataFrame,
    GenericPyArrowTable,
    PandasDataFrame,
    PanderaDataFrame,
    PanderaDataFrameModel,
    PyArrowTable,
)
from typing_extensions import Never, Self, Unpack, override

from .cursor import (
    Cursor,
    ExecuteOptions,
)


class MockCursor(  # type: ignore
    turu.core.mock.MockCursor[turu.core.cursor.GenericRowType, Any],  # type: ignore
    Cursor[
        turu.core.cursor.GenericRowType, GenericPandasDataFrame, GenericPyArrowTable
    ],  # type: ignore
):
    @override
    def execute(
        self,
        operation: str,
        parameters: Optional[Any] = None,
        /,
        **options: Unpack[ExecuteOptions],
    ) -> "MockCursor[Tuple[Any], PandasDataFrame, PyArrowTable]":
        return cast(MockCursor, super().execute(operation, parameters))

    @override
    def executemany(
        self,
        operation: str,
        seq_of_parameters: Sequence[Any],
        /,
        **options: Unpack[ExecuteOptions],
    ) -> "MockCursor[Tuple[Any], PandasDataFrame, PyArrowTable]":
        return cast(MockCursor, super().executemany(operation, seq_of_parameters))

    @overload
    def execute_map(
        self,
        row_type: Type[GenericNewRowType],
        operation: str,
        parameters: "Optional[Any]" = None,
        /,
        **options: Unpack[ExecuteOptions],
    ) -> "MockCursor[GenericNewRowType, Never, Never]":
        ...

    @overload
    def execute_map(
        self,
        row_type: Type[GenericNewPandasDataFrame],
        operation: str,
        parameters: "Optional[Any]" = None,
        /,
        **options: Unpack[ExecuteOptions],
    ) -> "MockCursor[Never, GenericNewPandasDataFrame, Never]":
        ...

    @overload
    def execute_map(
        self,
        row_type: Type[GenericNewPanderaDataFrameModel],
        operation: str,
        parameters: "Optional[Any]" = None,
        /,
        **options: Unpack[ExecuteOptions],
    ) -> "MockCursor[Never, PanderaDataFrame[GenericNewPanderaDataFrameModel], Never]":
        ...

    @overload
    def execute_map(
        self,
        row_type: Type[GenericNewPyArrowTable],
        operation: str,
        parameters: "Optional[Any]" = None,
        /,
        **options: Unpack[ExecuteOptions],
    ) -> "MockCursor[Never, Never, GenericNewPyArrowTable]":
        ...

    @override
    def execute_map(  # type: ignore[override]
        self,
        row_type: Union[
            Type[GenericNewRowType],
            Type[GenericNewPandasDataFrame],
            Type[GenericNewPyArrowTable],
            Type[GenericNewPanderaDataFrameModel],
        ],
        operation: str,
        parameters: Optional[Any] = None,
        /,
        **options: Unpack[ExecuteOptions],
    ) -> "MockCursor":
        return cast(
            MockCursor,
            super().execute_map(
                cast(Type[GenericNewRowType], row_type), operation, parameters
            ),
        )

    @overload
    def executemany_map(
        self,
        row_type: Type[GenericNewRowType],
        operation: str,
        seq_of_parameters: Sequence[Any],
        /,
        **options: Unpack[ExecuteOptions],
    ) -> "MockCursor[GenericNewRowType, Never, Never]":
        ...

    @overload
    def executemany_map(
        self,
        row_type: Type[GenericNewPandasDataFrame],
        operation: str,
        seq_of_parameters: Sequence[Any],
        /,
        **options: Unpack[ExecuteOptions],
    ) -> "MockCursor[Never, GenericNewPandasDataFrame, Never]":
        ...

    @overload
    def executemany_map(
        self,
        row_type: Type[GenericNewPanderaDataFrameModel],
        operation: str,
        seq_of_parameters: Sequence[Any],
        /,
        **options: Unpack[ExecuteOptions],
    ) -> "MockCursor[Never, PanderaDataFrame[GenericNewPanderaDataFrameModel], Never]":
        ...

    @overload
    def executemany_map(
        self,
        row_type: Type[GenericNewPyArrowTable],
        operation: str,
        seq_of_parameters: Sequence[Any],
        /,
        **options: Unpack[ExecuteOptions],
    ) -> "MockCursor[Never, Never, GenericNewPyArrowTable]":
        ...

    @override
    def executemany_map(  # type: ignore[override]
        self,
        row_type: Union[
            Type[GenericNewRowType],
            Type[GenericNewPandasDataFrame],
            Type[GenericNewPyArrowTable],
            Type[GenericNewPanderaDataFrameModel],
        ],
        operation: str,
        seq_of_parameters: Sequence[Any],
        /,
        **options: Unpack[ExecuteOptions],
    ):
        return cast(
            MockCursor,
            super().executemany_map(
                cast(Type[GenericNewRowType], row_type),
                operation,
                seq_of_parameters,
            ),
        )

    def use_warehouse(self, warehouse: str, /) -> Self:
        return self

    def use_database(self, database: str, /) -> Self:
        return self

    def use_schema(self, schema: str, /) -> Self:
        return self

    def use_role(self, role: str, /) -> Self:
        return self

    def fetch_arrow_all(self) -> GenericPyArrowTable:
        return cast(GenericPyArrowTable, self.fetchone())

    def fetch_arrow_batches(self) -> Iterator[GenericPyArrowTable]:
        return iter([self.fetch_arrow_all()])

    def fetch_pandas_all(self, **kwargs) -> GenericPandasDataFrame:
        df = self.fetchone()

        if (
            self._row_type
            and df is not None
            and issubclass(self._row_type, PanderaDataFrameModel)
        ):
            self._row_type.validate(df)  # type: ignore

        return cast(GenericPandasDataFrame, df)

    def fetch_pandas_batches(self, **kwargs) -> Iterator[GenericPandasDataFrame]:
        """Fetches a single Arrow Table."""

        return iter([self.fetch_pandas_all(**kwargs)])
