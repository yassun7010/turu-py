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

import turu.core.async_cursor
import turu.core.mock
from turu.core.cursor import GenericNewRowType, GenericRowType
from turu.snowflake.cursor import (
    GenericNewPandasDataFlame,
    GenericNewPyArrowTable,
    GenericPandasDataFlame,
    GenericPyArrowTable,
)
from turu.snowflake.features import (
    GenericNewPanderaDataFrameModel,
    PanderaDataFrame,
    PanderaDataFrameModel,
)
from typing_extensions import Never, Self, Unpack, override

from .async_cursor import AsyncCursor, ExecuteOptions


class MockAsyncCursor(  # type: ignore
    turu.core.mock.MockAsyncCursor[GenericRowType, Any],  # type: ignore
    AsyncCursor[GenericRowType, GenericPandasDataFlame, GenericPyArrowTable],  # type: ignore
):
    @override
    async def execute(
        self,
        operation: str,
        parameters: Optional[Any] = None,
        /,
        **options: Unpack[ExecuteOptions],
    ) -> "MockAsyncCursor[Tuple[Any], Never, Never]":
        return cast(MockAsyncCursor, await super().execute(operation, parameters))

    @override
    async def executemany(
        self,
        operation: str,
        seq_of_parameters: Sequence[Any],
        /,
        **options: Unpack[ExecuteOptions],
    ) -> "MockAsyncCursor[Tuple[Any], Never, Never]":
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
        row_type: Type[GenericNewPandasDataFlame],
        operation: str,
        parameters: "Optional[Any]" = None,
        /,
        **options: Unpack[ExecuteOptions],
    ) -> "MockAsyncCursor[Never, GenericNewPandasDataFlame, Never]":
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
    async def execute_map(
        self,
        row_type: Union[
            Type[GenericNewRowType],
            Type[GenericNewPanderaDataFrameModel],
            Type[GenericNewPandasDataFlame],
            Type[GenericNewPyArrowTable],
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
        pass

    @overload
    async def executemany_map(
        self,
        row_type: Type[GenericNewPandasDataFlame],
        operation: str,
        seq_of_parameters: Sequence[Any],
        /,
        **options: Unpack[ExecuteOptions],
    ) -> "MockAsyncCursor[Never, GenericNewPandasDataFlame, Never]":
        pass

    @overload
    async def executemany_map(
        self,
        row_type: Type[GenericNewPyArrowTable],
        operation: str,
        seq_of_parameters: Sequence[Any],
        /,
        **options: Unpack[ExecuteOptions],
    ) -> "MockAsyncCursor[Never, Never, GenericNewPyArrowTable]":
        pass

    @override
    async def executemany_map(
        self,
        row_type,
        operation: str,
        seq_of_parameters: Sequence[Any],
        /,
        **options: Unpack[ExecuteOptions],
    ) -> "MockAsyncCursor":
        return cast(
            MockAsyncCursor,
            await super().executemany_map(row_type, operation, seq_of_parameters),
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

    async def fetch_arrow_batches(self) -> Iterator[GenericPyArrowTable]:
        return iter([await self.fetch_arrow_all()])

    async def fetch_pandas_all(self, **kwargs) -> GenericPandasDataFlame:
        df = await self.fetchone()

        if (
            self._row_type
            and df is not None
            and issubclass(self._row_type, PanderaDataFrameModel)
        ):
            df = self._row_type.validate(df)  # type: ignore

        return cast(GenericPandasDataFlame, df)

    async def fetch_pandas_batches(self, **kwargs) -> Iterator[GenericPandasDataFlame]:
        """Fetches a single Arrow Table."""

        return iter([await self.fetch_pandas_all(**kwargs)])
