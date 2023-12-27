from typing import Optional, Type, Union

import psycopg
import psycopg.abc
import psycopg.rows
import turu.core.async_connection
import turu.core.mock
import turu.postgres.async_cursor
import turu.postgres.cursor
from typing_extensions import Never, override

from .async_cursor import AsyncCursor


class AsyncConnection(turu.core.async_connection.AsyncConnection):
    def __init__(self, connection: psycopg.AsyncConnection):
        self._raw_connection = connection

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


class MockAsyncConnection(turu.core.mock.MockAsyncConnection, AsyncConnection):
    def __init__(self, *args, **kwargs):
        turu.core.mock.MockAsyncConnection.__init__(self)

    @override
    async def cursor(self) -> "turu.postgres.async_cursor.MockAsyncCursor[Never]":
        return turu.postgres.async_cursor.MockAsyncCursor(self._turu_mock_store)


async def connect_async(
    conninfo: str = "",
    *,
    autocommit: bool = False,
    row_factory: Optional[psycopg.rows.AsyncRowFactory[psycopg.rows.Row]] = None,
    prepare_threshold: Optional[int] = 5,
    cursor_factory: Optional[Type[psycopg.AsyncCursor[psycopg.rows.Row]]] = None,
    context: Optional[psycopg.abc.AdaptContext] = None,
    **kwargs: Optional[Union[int, str]],
) -> AsyncConnection:
    return AsyncConnection(
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
