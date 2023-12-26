import psycopg
import turu.core.connection
import turu.core.cursor
from typing_extensions import Never, override

from .cursor import Cursor


class Connection(turu.core.connection.Connection):
    def __init__(self, connection: psycopg.Connection) -> None:
        self._raw_connection = connection

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
    def cursor(self) -> "Cursor[Never]":
        return Cursor(self._raw_connection.cursor())
