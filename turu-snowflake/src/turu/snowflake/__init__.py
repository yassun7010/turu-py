import importlib.metadata

from .connection import Connection, MockConnection, connect
from .cursor import Cursor, MockCursor

__version__ = importlib.metadata.version("turu-snowflake")

__all__ = [
    "connect",
    "Connection",
    "Cursor",
    "MockConnection",
    "MockCursor",
]
