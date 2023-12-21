import importlib.metadata
import sqlite3
from typing import Optional, Type, Union

import turu.core.connection
import turu.core.mock
import turu.sqlite3.cursor
from typing_extensions import Never, NotRequired, TypedDict, Unpack

from .cursor import Cursor

__version__ = importlib.metadata.version("turu-sqlite3")


class Connection(turu.core.connection.Connection):
    def __init__(self, connection: sqlite3.Connection):
        self._raw_connection = connection

    def close(self) -> None:
        self._raw_connection.close()

    def commit(self) -> None:
        self._raw_connection.commit()

    def rollback(self) -> None:
        self._raw_connection.rollback()

    def cursor(self) -> Cursor[Never]:
        return Cursor(self._raw_connection.cursor())


class MockConnection(Connection, turu.core.mock.MockConnection):
    def __init__(self, *args, **kwargs):
        turu.core.mock.MockConnection.__init__(self)

    def cursor(self) -> "turu.sqlite3.cursor.MockCursor[Never]":
        return turu.sqlite3.cursor.MockCursor(self._turu_mock_store)


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
