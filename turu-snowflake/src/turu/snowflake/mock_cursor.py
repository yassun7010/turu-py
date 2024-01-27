from typing import (
    Any,
    Iterator,
    Optional,
    Sequence,
    Tuple,
    Type,
    cast,
    overload,
)

import turu.core.cursor
import turu.core.mock
from turu.core.cursor import GenericNewRowType
from turu.snowflake.features import PandasDataFlame, PyArrowTable
from typing_extensions import Never, Self, Unpack, override

from .cursor import Cursor, ExecuteOptions, GenericArrowTable, GenericPandasDataFlame


class MockCursor(  # type: ignore
    turu.core.mock.MockCursor[turu.core.cursor.GenericRowType, Any],  # type: ignore
    Cursor[turu.core.cursor.GenericRowType, GenericArrowTable, GenericPandasDataFlame],  # type: ignore
):
    @override
    def execute(
        self,
        operation: str,
        parameters: Optional[Any] = None,
        /,
        **options: Unpack[ExecuteOptions],
    ) -> "MockCursor[Tuple[Any], Never, Never]":
        return cast(MockCursor, super().execute(operation, parameters))

    @override
    def executemany(
        self,
        operation: str,
        seq_of_parameters: Sequence[Any],
        /,
        **options: Unpack[ExecuteOptions],
    ) -> "MockCursor[Tuple[Any], Never, Never]":
        return cast(MockCursor, super().executemany(operation, seq_of_parameters))

    @overload
    def execute_map(
        self,
        row_type: Type[PandasDataFlame],
        operation: str,
        parameters: "Optional[Any]" = None,
        /,
        **options: Unpack[ExecuteOptions],
    ) -> "MockCursor[Never, Never, PandasDataFlame]":
        ...

    @overload
    def execute_map(
        self,
        row_type: Type[PyArrowTable],
        operation: str,
        parameters: "Optional[Any]" = None,
        /,
        **options: Unpack[ExecuteOptions],
    ) -> "MockCursor[Never, PyArrowTable, Never]":
        ...

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

    @override
    def execute_map(
        self,
        row_type,
        operation: str,
        parameters: Optional[Any] = None,
        /,
        **options: Unpack[ExecuteOptions],
    ):
        return cast(MockCursor, super().execute_map(row_type, operation, parameters))

    @override
    def executemany_map(
        self,
        row_type: Type[GenericNewRowType],
        operation: str,
        seq_of_parameters: Sequence[Any],
        /,
        **options: Unpack[ExecuteOptions],
    ) -> "MockCursor[GenericNewRowType, Never, Never]":
        return cast(
            MockCursor, super().executemany_map(row_type, operation, seq_of_parameters)
        )

    def use_warehouse(self, warehouse: str, /) -> Self:
        return self

    def use_database(self, database: str, /) -> Self:
        return self

    def use_schema(self, schema: str, /) -> Self:
        return self

    def use_role(self, role: str, /) -> Self:
        return self

    def fetch_arrow_all(self) -> GenericArrowTable:
        return cast(GenericArrowTable, self.fetchone())

    def fetch_arrow_batches(self) -> Iterator[GenericArrowTable]:
        return iter([self.fetch_arrow_all()])

    def fetch_pandas_all(self, **kwargs) -> GenericPandasDataFlame:
        return cast(GenericPandasDataFlame, self.fetchone())

    def fetch_pandas_batches(self, **kwargs) -> Iterator[GenericPandasDataFlame]:
        """Fetches a single Arrow Table."""

        return iter([self.fetch_pandas_all(**kwargs)])
