from typing import Any, Iterator, List, Optional, Sequence, Tuple, Type, cast

import aiosqlite
import turu.core.async_cursor
import turu.core.mock
import turu.core.tag
from turu.core.cursor import map_row
from typing_extensions import Never, override


class AsyncCursor(
    turu.core.async_cursor.AsyncCursor[
        turu.core.async_cursor.GenericRowType, Iterator[Any]
    ],
):
    def __init__(
        self,
        cursor: aiosqlite.Cursor,
        *,
        row_type: Optional[Type[turu.core.async_cursor.GenericRowType]] = None,
    ):
        self._raw_cursor = cursor
        self._row_type: Optional[Type[turu.core.async_cursor.GenericRowType]] = row_type
        self._aiter = None

    @property
    def rowcount(self) -> int:
        return self._raw_cursor.rowcount

    @property
    def arraysize(self) -> int:
        return self._raw_cursor.arraysize

    @arraysize.setter
    def arraysize(self, size: int) -> None:
        self._raw_cursor.arraysize = size

    @override
    async def close(self) -> None:
        await self._raw_cursor.close()

    @override
    async def execute(
        self, operation: str, parameters: Optional["Iterator[Any]"] = None, /
    ) -> "AsyncCursor[Tuple[Any]]":
        await self._raw_cursor.execute(operation, parameters)
        self._row_type = None

        return cast(AsyncCursor, self)

    @override
    async def executemany(
        self, operation: str, seq_of_parameters: "Sequence[Iterator[Any]]", /
    ) -> "AsyncCursor[Tuple[Any]]":
        await self._raw_cursor.executemany(operation, seq_of_parameters)
        self._row_type = None

        return cast(AsyncCursor, self)

    @override
    async def execute_map(
        self,
        row_type: Type[turu.core.async_cursor.GenericNewRowType],
        operation: str,
        parameters: "Optional[Iterator[Any]]" = None,
        /,
    ) -> "AsyncCursor[turu.core.async_cursor.GenericNewRowType]":
        await self._raw_cursor.execute(operation, parameters)
        self._row_type = cast(Type[turu.core.async_cursor.GenericRowType], row_type)

        return cast(AsyncCursor, self)

    @override
    async def executemany_map(
        self,
        row_type: Type[turu.core.async_cursor.GenericNewRowType],
        operation: str,
        seq_of_parameters: "Sequence[Iterator[Any]]",
        /,
    ) -> "AsyncCursor[turu.core.async_cursor.GenericNewRowType]":
        await self._raw_cursor.executemany(operation, seq_of_parameters)
        self._row_type = cast(Type[turu.core.async_cursor.GenericRowType], row_type)

        return cast(AsyncCursor, self)

    @override
    async def execute_with_tag(
        self,
        tag: Type[turu.core.tag.Tag],
        operation: str,
        parameters: "Optional[Iterator[Any]]" = None,
    ) -> turu.core.async_cursor.AsyncCursor[Never, Iterator[Any]]:
        return cast(
            turu.core.async_cursor.AsyncCursor,
            await self.execute(operation, parameters),
        )

    @override
    async def executemany_with_tag(
        self,
        tag: Type[turu.core.tag.Tag],
        operation: str,
        seq_of_parameters: Sequence[Iterator[Any]],
    ) -> turu.core.async_cursor.AsyncCursor[Never, Iterator[Any]]:
        return cast(
            turu.core.async_cursor.AsyncCursor,
            await self.executemany(operation, seq_of_parameters),
        )

    @override
    async def fetchone(self) -> Optional[turu.core.async_cursor.GenericRowType]:
        row = await self._raw_cursor.fetchone()
        if row is None:
            return None

        return _map_row(self._row_type, row)

    @override
    async def fetchmany(
        self, size: Optional[int] = None
    ) -> List[turu.core.async_cursor.GenericRowType]:
        return [
            _map_row(self._row_type, row)
            for row in (
                await self._raw_cursor.fetchmany(
                    size if size is not None else self.arraysize
                )
            )
        ]

    @override
    async def fetchall(self) -> List[turu.core.async_cursor.GenericRowType]:
        return [
            _map_row(self._row_type, row) for row in await self._raw_cursor.fetchall()
        ]

    @override
    def __aiter__(self) -> "AsyncCursor[turu.core.async_cursor.GenericRowType]":
        self._aiter = self._raw_cursor.__aiter__()
        return self

    @override
    async def __anext__(self) -> turu.core.async_cursor.GenericRowType:
        if self._aiter is None:
            self._aiter = self._raw_cursor.__aiter__()

        next_row = await self._aiter.__anext__()
        return _map_row(self._row_type, next_row)


def _map_row(
    row_type: Optional[Type[turu.core.async_cursor.GenericRowType]],
    row: Any,
) -> turu.core.async_cursor.GenericRowType:
    if row_type is None:
        return tuple(row)  # type: ignore

    else:
        return map_row(row_type, row)
