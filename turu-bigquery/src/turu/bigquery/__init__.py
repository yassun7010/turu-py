import importlib.metadata

from .connection import Connection as Connection
from .cursor import Cursor as Cursor
from .mock_connection import MockConnection as MockConnection
from .mock_cursor import MockCursor as MockCursor

__version__ = importlib.metadata.version("turu-bigquery")

connect = Connection.connect
