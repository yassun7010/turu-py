import importlib.metadata

from .connection import Connection, connect
from .cursor import Cursor

__version__ = importlib.metadata.version("turu-sqlite3")


from .connection import MockConnection
from .cursor import MockCursor

__all__ = [
    "connect",
    "Connection",
    "Cursor",
    "MockConnection",
    "MockCursor",
]
