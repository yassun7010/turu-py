import importlib.metadata

from .connection import MockConnection
from .cursor import MockCursor

__version__ = importlib.metadata.version("turu-mock")

__all__ = ["MockConnection", "MockCursor"]
