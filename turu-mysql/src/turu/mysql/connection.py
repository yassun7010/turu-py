import os
from typing import Any, Callable, Dict, Optional, Type, Union

import pymysql
import turu.core.connection
import turu.core.cursor
import turu.core.mock
from typing_extensions import Never, Self, TypedDict, Unpack, override

from .cursor import Cursor


class Connection(turu.core.connection.Connection):
    def __init__(self, connection: pymysql.Connection) -> None:
        self._raw_connection = connection

    @override
    @classmethod
    def connect(  # type: ignore[override]
        cls,
        user: Optional[str] = None,
        password: str = "",
        host: Optional[str] = None,
        database: Optional[str] = None,
        port: int = 0,
        **kwargs: Unpack["_ConnectParams"],
    ) -> Self:
        return cls(
            pymysql.connect(
                user=user,
                password=password,
                host=host,
                database=database,
                port=port,
                **kwargs,
            )
        )

    @override
    @classmethod
    def connect_from_env(  # type: ignore[override]
        cls,
        user_envname: str = "MYSQL_USER",
        password_envname: str = "MYSQL_PASSWORD",
        host_envname: str = "MYSQL_HOST",
        database_envname: str = "MYSQL_DATABASE",
        port_envname: str = "MYSQL_PORT",
        **kwargs: Unpack["_ConnectParams"],
    ) -> Self:
        return cls.connect(
            user=os.environ.get(user_envname),
            password=os.environ.get(password_envname, ""),
            host=os.environ.get(host_envname),
            database=os.environ.get(database_envname),
            port=int(os.environ.get(port_envname, 0)),
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


class _ConnectParams(TypedDict, total=False):
    unix_socket: Optional[str]
    charset: str
    collation: Optional[Any]
    sql_mode: Optional[str]
    read_default_file: Optional[str]
    conv: Dict[int, Callable[[Union[str, bytes]], Any]]
    use_unicode: Optional[bool]
    client_flag: int
    cursorclass: Type[Cursor]
    init_command: Optional[str]
    connect_timeout: Optional[int]
    read_default_group: Optional[str]
    autocommit: bool
    local_infile: bool
    max_allowed_packet: int
    defer_connect: bool
    auth_plugin_map: Optional[dict]
    read_timeout: Optional[int]
    write_timeout: Optional[int]
    bind_address: Optional[str]
    binary_prefix: bool
    program_name: Optional[str]
    server_public_key: Optional[Any]
    ssl: Optional[dict]
    ssl_ca: Optional[str]
    ssl_cert: Optional[str]
    ssl_disabled: Optional[bool]
    ssl_key: Optional[str]
    ssl_verify_cert: Optional[bool]
    ssl_verify_identity: Optional[bool]
