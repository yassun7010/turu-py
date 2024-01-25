import asyncio
import os
from typing import Any, Callable, Dict, Optional, Type, TypedDict, Union

import aiomysql
import aiomysql.connection
import pymysql.cursors
import turu.core.async_connection
import turu.core.mock
import turu.mysql.async_cursor
import turu.mysql.cursor
from typing_extensions import Never, Self, Unpack, override

from .async_cursor import AsyncCursor


class AsyncConnection(turu.core.async_connection.AsyncConnection):
    def __init__(self, connection: aiomysql.Connection):
        self._raw_connection = connection

    @override
    @classmethod
    async def connect(  # type: ignore[override]
        cls,
        user: Optional[str] = None,
        password: str = "",
        host: str = "localhost",
        database: Optional[str] = None,
        port: int = 0,
        **kwargs: Unpack["_ConnectParams"],
    ) -> Self:
        return cls(
            await aiomysql.connection._connect(
                user=user,
                password=password,
                host=host,
                db=database,
                port=port,
                **kwargs,
            )
        )

    @override
    @classmethod
    async def connect_from_env(  # type: ignore[override]
        cls,
        user_envname: str = "MYSQL_USER",
        password_envname: str = "MYSQL_PASSWORD",
        host_envname: str = "MYSQL_HOST",
        database_envname: str = "MYSQL_DATABASE",
        port_envname: str = "MYSQL_PORT",
        **kwargs: Unpack["_ConnectParams"],
    ) -> Self:
        return await cls.connect(
            user=os.environ.get(user_envname),
            password=os.environ.get(password_envname, ""),
            host=os.environ.get(host_envname, "localhost"),
            database=os.environ.get(database_envname),
            port=int(os.environ.get(port_envname, 0)),
            **kwargs,
        )

    @override
    async def close(self) -> None:
        await self._raw_connection.ensure_closed()

    @override
    async def commit(self) -> None:
        await self._raw_connection.commit()

    @override
    async def rollback(self) -> None:
        await self._raw_connection.rollback()

    @override
    async def cursor(self) -> AsyncCursor[Never]:
        return AsyncCursor(
            aiomysql.cursors.Cursor(
                self._raw_connection,
                self._raw_connection._echo,
            )
        )


class _ConnectParams(TypedDict, total=False):
    unix_socket: Optional[str]
    charset: str
    sql_mode: Optional[str]
    read_default_file: Optional[str]
    conv: Dict[int, Callable[[Union[str, bytes]], Any]]
    use_unicode: Optional[bool]
    client_flag: int
    cursorclass: Type[pymysql.cursors.Cursor]
    init_command: Optional[str]
    connect_timeout: Optional[int]
    read_default_group: Optional[str]
    autocommit: bool
    echo: bool
    local_infile: bool
    loop: Optional[asyncio.AbstractEventLoop]
    ssl: Optional[dict]
    auth_plugin: str
    program_name: str
    server_public_key: Optional[Any]
