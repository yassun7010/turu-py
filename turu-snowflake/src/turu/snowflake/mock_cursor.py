from typing import Any, Iterator, Optional, Sequence, Tuple, Type, cast

import turu.core.cursor
import turu.core.mock
from turu.snowflake.features import PandasDataFlame
from typing_extensions import Self, Unpack, override

from .cursor import Cursor, ExecuteOptions


class MockCursor(  # type: ignore
    turu.core.mock.MockCursor[turu.core.cursor.GenericRowType, Any],  # type: ignore
    Cursor[turu.core.cursor.GenericRowType],  # type: ignore
):
    @override
    def execute(
        self,
        operation: str,
        parameters: Optional[Any] = None,
        /,
        **options: Unpack[ExecuteOptions],
    ) -> "MockCursor[Tuple[Any]]":
        return cast(MockCursor, super().execute(operation, parameters))

    @override
    def executemany(
        self,
        operation: str,
        seq_of_parameters: Sequence[Any],
        /,
        **options: Unpack[ExecuteOptions],
    ) -> "MockCursor[Tuple[Any]]":
        return cast(MockCursor, super().executemany(operation, seq_of_parameters))

    @override
    def execute_map(
        self,
        row_type: Type[turu.core.cursor.GenericNewRowType],
        operation: str,
        parameters: Optional[Any] = None,
        /,
        **options: Unpack[ExecuteOptions],
    ) -> "MockCursor[turu.core.cursor.GenericNewRowType]":
        return cast(MockCursor, super().execute_map(row_type, operation, parameters))

    @override
    def executemany_map(
        self,
        row_type: Type[turu.core.cursor.GenericNewRowType],
        operation: str,
        seq_of_parameters: Sequence[Any],
        /,
        **options: Unpack[ExecuteOptions],
    ) -> "MockCursor[turu.core.cursor.GenericNewRowType]":
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

    def fetch_arrow_all(self):
        return self.fetchone()

    def fetch_arrow_batches(self):
        return iter([self.fetch_arrow_all()])

    def fetch_pandas_all(self, **kwargs) -> PandasDataFlame:
        return cast(PandasDataFlame, self.fetchone())

    def fetch_pandas_batches(self, **kwargs) -> Iterator[PandasDataFlame]:
        """Fetches a single Arrow Table."""

        return iter([self.fetch_pandas_all(**kwargs)])
