import sqlite3
from typing import Callable, Optional, Type, Union, overload

from typing_extensions import NotRequired, Self, TypedDict, Unpack

from typingsql.protocols.connection import ConnectionProtocol

from .cursor import Cursor


class Connection(sqlite3.Connection, ConnectionProtocol):
    @overload
    def cursor(self, cursorClass: None = None) -> Cursor:
        ...

    @overload
    def cursor(self, cursorClass: Callable[[Self], sqlite3.Cursor]) -> Cursor:
        ...

    def cursor(self, cursorClass=None) -> Cursor:  # type: ignore[override]
        return super().cursor(cursorClass or Cursor)


class _ConnecArgs(TypedDict):
    timeout: NotRequired[float]
    detect_types: NotRequired[int]
    isolation_level: NotRequired[Optional[str]]
    check_same_thread: NotRequired[bool]
    factory: NotRequired[Optional[Type[sqlite3.Connection]]]
    cached_statements: NotRequired[int]
    uri: NotRequired[bool]


def connect(
    database: Union[str, bytes],
    **kwargs: Unpack[_ConnecArgs],
) -> Connection:
    return Connection(database, **kwargs)
