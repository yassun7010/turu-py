import importlib.metadata

from .async_connection import (
    AsyncConnection,
)
from .async_cursor import AsyncCursor
from .connection import (
    Connection,
)
from .cursor import Cursor
from .mock_async_connection import MockAsyncConnection
from .mock_async_cursor import MockAsyncCursor
from .mock_connection import MockConnection
from .mock_cursor import MockCursor

__version__ = importlib.metadata.version("turu-postgres")

__all__ = [
    "AsyncConnection",
    "AsyncCursor",
    "connect_async_from_env",
    "connect_async",
    "connect_from_env",
    "connect",
    "Connection",
    "Cursor",
    "MockAsyncConnection",
    "MockAsyncCursor",
    "MockConnection",
    "MockCursor",
]

connect = Connection.connect
connect_from_env = Connection.connect_from_env
connect_async = AsyncConnection.connect
connect_async_from_env = AsyncConnection.connect_from_env
