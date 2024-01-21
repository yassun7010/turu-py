import importlib.metadata

from turu.core.record.csv_recorder import add_record_map
from turu.snowflake.features import USE_PANDAS, PandasDataFlame

from .async_connection import (
    AsyncConnection,
    connect_async,
    connect_async_from_env,
)
from .async_cursor import AsyncCursor
from .connection import Connection, connect, connect_from_env
from .cursor import Cursor
from .mock_async_connection import MockAsyncConnection
from .mock_async_cursor import MockAsyncCursor
from .mock_connection import MockConnection
from .mock_cursor import MockCursor

__version__ = importlib.metadata.version("turu-snowflake")

__all__ = [
    "connect_async_from_env",
    "connect_from_env",
    "AsyncConnection",
    "AsyncCursor",
    "connect_async",
    "connect",
    "Connection",
    "Cursor",
    "MockAsyncConnection",
    "MockAsyncCursor",
    "MockConnection",
    "MockCursor",
]


if USE_PANDAS:
    add_record_map(
        "pandas",
        lambda row: USE_PANDAS and isinstance(row, PandasDataFlame),
        lambda row: row.keys(),
        lambda row: row.values,
        lambda row: row,
    )
