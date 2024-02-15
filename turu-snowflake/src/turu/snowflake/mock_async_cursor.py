from typing import (
    Any,
    AsyncIterator,
    Optional,
    Sequence,
    Tuple,
    Type,
    Union,
    cast,
    overload,
)

import turu.core.async_cursor
import turu.core.mock
from turu.core.cursor import GenericNewRowType, GenericRowType
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

from .async_cursor import AsyncCursor, ExecuteOptions


class MockAsyncCursor(  # type: ignore
    turu.core.mock.MockAsyncCursor[GenericRowType, Any],  # type: ignore
    AsyncCursor[GenericRowType, GenericPandasDataFrame, GenericPyArrowTable],  # type: ignore
):
    """
    A async cursor is a database object that is used to manage the context of a fetch operation.
    """

    @override
    async def execute(
        self,
        operation: str,
        parameters: Optional[Any] = None,
        /,
        **options: Unpack[ExecuteOptions],
    ) -> "MockAsyncCursor[Tuple[Any], PandasDataFrame, PyArrowTable]":
        return cast(MockAsyncCursor, await super().execute(operation, parameters))

    @override
    async def executemany(
        self,
        operation: str,
        seq_of_parameters: Sequence[Any],
        /,
        **options: Unpack[ExecuteOptions],
    ) -> "MockAsyncCursor[Tuple[Any], PandasDataFrame, PyArrowTable]":
        return cast(
            MockAsyncCursor, await super().executemany(operation, seq_of_parameters)
        )

    @overload
    async def execute_map(
        self,
        row_type: Type[GenericNewRowType],
        operation: str,
        parameters: "Optional[Any]" = None,
        /,
        **options: Unpack[ExecuteOptions],
    ) -> "MockAsyncCursor[GenericNewRowType, Never, Never]":
        ...

    @overload
    async def execute_map(
        self,
        row_type: Type[GenericNewPandasDataFrame],
        operation: str,
        parameters: "Optional[Any]" = None,
        /,
        **options: Unpack[ExecuteOptions],
    ) -> "MockAsyncCursor[Never, GenericNewPandasDataFrame, Never]":
        ...

    @overload
    async def execute_map(
        self,
        row_type: Type[GenericNewPanderaDataFrameModel],
        operation: str,
        parameters: "Optional[Any]" = None,
        /,
        **options: Unpack[ExecuteOptions],
    ) -> "MockAsyncCursor[Never, PanderaDataFrame[GenericNewPanderaDataFrameModel], Never]":
        ...

    @overload
    async def execute_map(
        self,
        row_type: Type[GenericNewPyArrowTable],
        operation: str,
        parameters: "Optional[Any]" = None,
        /,
        **options: Unpack[ExecuteOptions],
    ) -> "MockAsyncCursor[Never, Never, GenericNewPyArrowTable]":
        ...

    @override
    async def execute_map(  # type: ignore[override]
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
    ) -> "MockAsyncCursor":
        return cast(
            MockAsyncCursor,
            await super().execute_map(
                cast(Type[GenericNewRowType], row_type), operation, parameters
            ),
        )

    @overload
    async def executemany_map(
        self,
        row_type: Type[GenericNewRowType],
        operation: str,
        seq_of_parameters: Sequence[Any],
        /,
        **options: Unpack[ExecuteOptions],
    ) -> "MockAsyncCursor[GenericNewRowType, Never, Never]":
        ...

    @overload
    async def executemany_map(
        self,
        row_type: Type[GenericNewPandasDataFrame],
        operation: str,
        seq_of_parameters: Sequence[Any],
        /,
        **options: Unpack[ExecuteOptions],
    ) -> "MockAsyncCursor[Never, GenericNewPandasDataFrame, Never]":
        ...

    @overload
    async def executemany_map(
        self,
        row_type: Type[GenericNewPanderaDataFrameModel],
        operation: str,
        seq_of_parameters: Sequence[Any],
        /,
        **options: Unpack[ExecuteOptions],
    ) -> "MockAsyncCursor[Never, PanderaDataFrame[GenericNewPanderaDataFrameModel], Never]":
        ...

    @overload
    async def executemany_map(
        self,
        row_type: Type[GenericNewPyArrowTable],
        operation: str,
        seq_of_parameters: Sequence[Any],
        /,
        **options: Unpack[ExecuteOptions],
    ) -> "MockAsyncCursor[Never, Never, GenericNewPyArrowTable]":
        ...

    @override
    async def executemany_map(  # type: ignore[override]
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
    ) -> "MockAsyncCursor":
        return cast(
            MockAsyncCursor,
            await super().executemany_map(
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

    async def fetch_arrow_all(self) -> GenericPyArrowTable:
        return cast(GenericPyArrowTable, await self.fetchone())

    async def fetch_arrow_batches(self) -> AsyncIterator[GenericPyArrowTable]:
        yield await self.fetch_arrow_all()

    async def fetch_pandas_all(self, **kwargs) -> GenericPandasDataFrame:
        df = await self.fetchone()

        if (
            self._row_type
            and df is not None
            and issubclass(self._row_type, PanderaDataFrameModel)
        ):
            df = self._row_type.validate(df)  # type: ignore

        return cast(GenericPandasDataFrame, df)

    async def fetch_pandas_batches(
        self, **kwargs
    ) -> AsyncIterator[GenericPandasDataFrame]:
        """Fetches a single Arrow Table."""

        yield await self.fetch_pandas_all(**kwargs)
