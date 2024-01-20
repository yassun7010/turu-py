import sqlite3
from pathlib import Path
from typing import Optional, Type, Union

import aiosqlite
import turu.core.async_connection
import turu.core.mock
import turu.sqlite3.cursor
from typing_extensions import Never, NotRequired, TypedDict, Unpack, override

from .async_cursor import AsyncCursor


class AsyncConnection(turu.core.async_connection.AsyncConnection):
    def __init__(self, connection: aiosqlite.Connection):
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
        return AsyncCursor(await self._raw_connection.cursor())


class _ConnectArgs(TypedDict):
    timeout: NotRequired[float]
    detect_types: NotRequired[int]
    isolation_level: NotRequired[Optional[str]]
    check_same_thread: NotRequired[bool]
    factory: NotRequired[Optional[Type[sqlite3.Connection]]]
    cached_statements: NotRequired[int]
    uri: NotRequired[bool]


async def connect_async(
    database: Union[str, Path],
    *,
    iter_chunk_size: int = 64,
    **kwargs: Unpack[_ConnectArgs],
) -> AsyncConnection:
    return AsyncConnection(
        await aiosqlite.connect(
            database,
            iter_chunk_size=iter_chunk_size,
            loop=None,
            **kwargs,
        )
    )
