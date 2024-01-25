import os
from typing import Optional, Type, Union

import psycopg
import psycopg.abc
import psycopg.rows
import turu.core.async_connection
import turu.core.mock
import turu.postgres.async_cursor
import turu.postgres.cursor
from typing_extensions import Never, Self, override

from .async_cursor import AsyncCursor


class AsyncConnection(turu.core.async_connection.AsyncConnection):
    def __init__(self, connection: psycopg.AsyncConnection):
        self._raw_connection = connection

    @override
    @classmethod
    async def connect(  # type: ignore[override]
        cls,
        conninfo: str = "",
        *,
        autocommit: bool = False,
        row_factory: Optional[psycopg.rows.AsyncRowFactory[psycopg.rows.Row]] = None,
        prepare_threshold: Optional[int] = 5,
        cursor_factory: Optional[Type[psycopg.AsyncCursor[psycopg.rows.Row]]] = None,
        context: Optional[psycopg.abc.AdaptContext] = None,
        **kwargs: Optional[Union[int, str]],
    ) -> Self:
        return cls(
            await psycopg.AsyncConnection.connect(
                conninfo,
                autocommit=autocommit,
                prepare_threshold=prepare_threshold,
                row_factory=row_factory,
                cursor_factory=cursor_factory,
                context=context,
                **kwargs,
            )
        )

    @override
    @classmethod
    async def connect_from_env(  # type: ignore[override]
        cls,
        *,
        autocommit: bool = False,
        row_factory: Optional[psycopg.rows.AsyncRowFactory[psycopg.rows.Row]] = None,
        prepare_threshold: Optional[int] = 5,
        cursor_factory: Optional[Type[psycopg.AsyncCursor[psycopg.rows.Row]]] = None,
        context: Optional[psycopg.abc.AdaptContext] = None,
        dbname_envname="POSTGRES_DB",
        user_envname="POSTGRES_USER",
        password_envname="POSTGRES_PASSWORD",
        host_envname="POSTGRES_HOST",
        port_envname="POSTGRES_PORT",
        **kwargs: Union[None, int, str],
    ):
        return await cls.connect(
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
    async def close(self) -> None:
        await self._raw_connection.close()

    @override
    async def commit(self) -> None:
        await self._raw_connection.commit()

    @override
    async def rollback(self) -> None:
        await self._raw_connection.rollback()

    @override
    async def cursor(self) -> AsyncCursor[Never]:
        return AsyncCursor(self._raw_connection.cursor())
