import importlib.metadata

from .connection import Connection, connect
from .cursor import Cursor

__version__ = importlib.metadata.version("turu-bigquery")

__all__ = [
    "connect",
    "Connection",
    "Cursor",
]
