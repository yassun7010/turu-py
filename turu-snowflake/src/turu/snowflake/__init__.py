"""
Turu Snowflake Adapter

References:
    https://yassun7010.github.io/turu-py/adapters/snowflake/index.html
"""

import importlib.metadata

from .async_connection import AsyncConnection as AsyncConnection
from .async_cursor import AsyncCursor as AsyncCursor
from .connection import Connection as Connection
from .cursor import Cursor as Cursor
from .mock_async_connection import MockAsyncConnection as MockAsyncConnection
from .mock_async_cursor import MockAsyncCursor as MockAsyncCursor
from .mock_connection import MockConnection as MockConnection
from .mock_cursor import MockCursor as MockCursor

__version__ = importlib.metadata.version("turu-snowflake")


connect = Connection.connect
connect_from_env = Connection.connect_from_env
connect_async = AsyncConnection.connect
connect_async_from_env = AsyncConnection.connect_from_env
