import sqlite3
from typing import Any, Optional, Type, Union

import turu.core.connection
import turu.core.mock
import turu.sqlite3.cursor
from typing_extensions import Never, NotRequired, Self, TypedDict, Unpack, override

from .cursor import Cursor


class Connection(turu.core.connection.Connection):
    def __init__(self, connection: sqlite3.Connection):
        self._raw_connection = connection

    @override
    @classmethod
    def connect(  # type: ignore[override]
        cls,
        database: Union[str, bytes],
        **kwargs: Unpack["_ConnectArgs"],
    ) -> Self:
        return cls(sqlite3.Connection(database, **kwargs))

    @override
    @classmethod
    def connect_from_env(cls, *args: Any, **kwargs: Any) -> Self:
        return cls.connect(*args, **kwargs)

    @override
    def close(self) -> None:
        self._raw_connection.close()

    @override
    def commit(self) -> None:
        self._raw_connection.commit()

    @override
    def rollback(self) -> None:
        self._raw_connection.rollback()

    @override
    def cursor(self) -> Cursor[Never]:
        return Cursor(self._raw_connection.cursor())


class _ConnectArgs(TypedDict):
    timeout: NotRequired[float]
    detect_types: NotRequired[int]
    isolation_level: NotRequired[Optional[str]]
    check_same_thread: NotRequired[bool]
    factory: NotRequired[Optional[Type[sqlite3.Connection]]]
    cached_statements: NotRequired[int]
    uri: NotRequired[bool]
