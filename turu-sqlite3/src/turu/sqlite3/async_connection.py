from pathlib import Path
from typing import Any, Union

import aiosqlite
import turu.core.async_connection
import turu.core.mock
import turu.sqlite3.cursor
from typing_extensions import Never, Self, Unpack, override

from .async_cursor import AsyncCursor
from .connection import _ConnectArgs


class AsyncConnection(turu.core.async_connection.AsyncConnection):
    def __init__(self, connection: aiosqlite.Connection):
        self._raw_connection = connection

    @override
    @classmethod
    async def connect(  # type: ignore[override]
        cls,
        database: Union[str, Path],
        *,
        iter_chunk_size: int = 64,
        **kwargs: Unpack[_ConnectArgs],
    ) -> Self:
        return cls(
            await aiosqlite.connect(
                database,
                iter_chunk_size=iter_chunk_size,
                loop=None,
                **kwargs,
            )
        )

    @override
    @classmethod
    async def connect_from_env(cls, *args: Any, **kwargs: Any) -> Self:
        return await cls.connect(*args, **kwargs)

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
