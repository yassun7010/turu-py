import importlib.metadata
from importlib.util import find_spec

from .connection import Connection, connect
from .cursor import Cursor

__version__ = importlib.metadata.version("turu-sqlite3")


if find_spec("turu.core.mock"):
    from .connection import MockConnection
    from .cursor import MockCursor

    __all__ = [
        "connect",
        "Connection",
        "Cursor",
        "MockConnection",
        "MockCursor",
    ]
else:
    __all__ = [
        "connect",
        "Connection",
        "Cursor",
    ]
