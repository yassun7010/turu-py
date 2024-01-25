import os
from typing import Optional, Type, Union

import psycopg
import psycopg.abc
import psycopg.rows
import turu.core.connection
import turu.core.cursor
import turu.core.mock
from typing_extensions import Never, Self, override

from .cursor import Cursor


class Connection(turu.core.connection.Connection):
    def __init__(self, connection: psycopg.Connection) -> None:
        self._raw_connection = connection

    @override
    @classmethod
    def connect(  # type: ignore[override]
        cls,
        conninfo: str = "",
        *,
        autocommit: bool = False,
        row_factory: Optional[psycopg.rows.RowFactory[psycopg.rows.Row]] = None,
        prepare_threshold: Optional[int] = 5,
        cursor_factory: Optional[Type[psycopg.Cursor[psycopg.rows.Row]]] = None,
        context: Optional[psycopg.abc.AdaptContext] = None,
        **kwargs: Union[None, int, str],
    ) -> Self:
        return cls(
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

    @override
    @classmethod
    def connect_from_env(  # type: ignore[override]
        cls,
        *,
        dbname_envname="POSTGRES_DB",
        user_envname="POSTGRES_USER",
        password_envname="POSTGRES_PASSWORD",
        host_envname="POSTGRES_HOST",
        port_envname="POSTGRES_PORT",
        autocommit: bool = False,
        row_factory: Optional[psycopg.rows.RowFactory[psycopg.rows.Row]] = None,
        prepare_threshold: Optional[int] = 5,
        cursor_factory: Optional[Type[psycopg.Cursor[psycopg.rows.Row]]] = None,
        context: Optional[psycopg.abc.AdaptContext] = None,
        **kwargs: Union[None, int, str],
    ) -> Self:
        return cls.connect(
            "\n".join(
                [
                    f"{key}={value}"
                    for key, value in {
                        "dbname": os.environ.get(dbname_envname),
                        "user": os.environ.get(user_envname),
                        "password": os.environ.get(password_envname),
                        "host": os.environ.get(host_envname),
                        "port": os.environ.get(port_envname),
                    }.items()
                    if value is not None
                ]
            ),
            autocommit=autocommit,
            row_factory=row_factory,
            prepare_threshold=prepare_threshold,
            cursor_factory=cursor_factory,
            context=context,
            **kwargs,
        )

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
