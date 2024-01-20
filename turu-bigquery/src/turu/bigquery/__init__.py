import importlib.metadata

from .connection import Connection, connect
from .cursor import Cursor
from .mock_connection import MockConnection
from .mock_cursor import MockCursor

__version__ = importlib.metadata.version("turu-bigquery")

__all__ = [
    "connect",
    "Connection",
    "Cursor",
    "MockConnection",
    "MockCursor",
]
