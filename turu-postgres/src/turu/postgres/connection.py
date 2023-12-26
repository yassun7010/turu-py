from typing import Optional, Type, Union

import psycopg
import psycopg.abc
import psycopg.rows
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


def connect(
    conninfo: str = "",
    *,
    autocommit: bool = False,
    row_factory: Optional[psycopg.rows.RowFactory[psycopg.rows.Row]] = None,
    prepare_threshold: Optional[int] = 5,
    cursor_factory: Optional[Type[psycopg.Cursor[psycopg.rows.Row]]] = None,
    context: Optional[psycopg.abc.AdaptContext] = None,
    **kwargs: Union[None, int, str],
):
    return Connection(
        psycopg.connect(
            conninfo,
            autocommit=autocommit,
            row_factory=row_factory,
            prepare_threshold=prepare_threshold,
            cursor_factory=cursor_factory,
            context=context,
            **kwargs,
        )
    )
