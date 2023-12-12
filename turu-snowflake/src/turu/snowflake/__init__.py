import importlib.metadata

from .connection import Connection, connect
from .cursor import Cursor

__version__ = importlib.metadata.version("turu-snowflake")

__all__ = [
    "connect",
    "Connection",
    "Cursor",
]
