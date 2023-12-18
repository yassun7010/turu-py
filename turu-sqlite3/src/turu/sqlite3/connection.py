import importlib.metadata
import sqlite3
from typing import Optional, Type, Union

from turu.core.protocols.connection import ConnectionProtocol
from typing_extensions import Never, NotRequired, TypedDict, Unpack

from .cursor import Cursor

__version__ = importlib.metadata.version("turu-sqlite3")


class Connection(ConnectionProtocol):
    def __init__(self, raw_connection: sqlite3.Connection):
        self._raw_connection = raw_connection

    def cursor(self) -> Cursor:
        return Cursor(self._raw_connection.cursor())


try:
    import turu.mock
    import turu.sqlite3.cursor

    class MockConnection(Connection, turu.mock.MockConnection):
        def __init__(self, **kwargs):
            turu.mock.MockConnection.__init__(self)

        def cursor(self) -> "turu.sqlite3.cursor.MockCursor[Never]":
            return turu.sqlite3.cursor.MockCursor(self._turu_mock_store)

except ImportError:
    pass


class _ConnectArgs(TypedDict):
    timeout: NotRequired[float]
    detect_types: NotRequired[int]
    isolation_level: NotRequired[Optional[str]]
    check_same_thread: NotRequired[bool]
    factory: NotRequired[Optional[Type[sqlite3.Connection]]]
    cached_statements: NotRequired[int]
    uri: NotRequired[bool]


def connect(
    database: Union[str, bytes],
    **kwargs: Unpack[_ConnectArgs],
) -> Connection:
    return Connection(sqlite3.Connection(database, **kwargs))
