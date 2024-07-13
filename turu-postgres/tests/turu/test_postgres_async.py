import os

import pytest
from psycopg import ProgrammingError
from pydantic import BaseModel
from turu.postgres import AsyncConnection


class Row(BaseModel):
    id: int


@pytest.mark.skipif(
    condition="USE_REAL_CONNECTION" not in os.environ
    or os.environ["USE_REAL_CONNECTION"].lower() != "true",
    reason="USE_REAL_CONNECTION flag is not set.",
)
class TestTuruPostgresAsync:
    @pytest.mark.asyncio
    async def test_execute(self, async_connection: AsyncConnection):
        async with await async_connection.execute("select 1") as cursor:
            assert await cursor.fetchall() == [(1,)]

    @pytest.mark.asyncio
    async def test_execute_fetchone(self, async_connection: AsyncConnection):
        async with await async_connection.execute("select 1") as cursor:
            assert await cursor.fetchone() == (1,)

    @pytest.mark.asyncio
    async def test_execute_map_fetchone(self, async_connection: AsyncConnection):
        async with await async_connection.execute_map(Row, "select 1") as cursor:
            assert await cursor.fetchone() == Row(id=1)

    @pytest.mark.asyncio
    async def test_execute_map_fetchmany(self, async_connection: AsyncConnection):
        async with await async_connection.execute_map(
            Row, "select 1 union all select 2"
        ) as cursor:
            assert await cursor.fetchmany() == [Row(id=1)]
            assert await cursor.fetchone() == Row(id=2)
            assert await cursor.fetchone() is None

    @pytest.mark.asyncio
    async def test_execute_map_fetchmany_with_size(
        self, async_connection: AsyncConnection
    ):
        async with await async_connection.execute_map(
            Row, "select 1 union all select 2 union all select 3"
        ) as cursor:
            assert await cursor.fetchmany(2) == [Row(id=1), Row(id=2)]
            assert await cursor.fetchmany(2) == [Row(id=3)]

    @pytest.mark.asyncio
    async def test_execute_map_fetchall(self, async_connection: AsyncConnection):
        async with await async_connection.execute_map(
            Row, "select 1 union all select 2"
        ) as cursor:
            assert await cursor.fetchall() == [Row(id=1), Row(id=2)]
            assert await cursor.fetchone() is None

    @pytest.mark.asyncio
    async def test_executemany(self, async_connection: AsyncConnection):
        async with await async_connection.executemany(
            "select 1 union all select 2", []
        ) as cursor:
            with pytest.raises(ProgrammingError):
                await cursor.fetchone()

    @pytest.mark.asyncio
    async def test_executemany_map(self, async_connection: AsyncConnection):
        async with await async_connection.executemany_map(
            Row, "select 1 union all select 2", []
        ) as cursor:
            with pytest.raises(ProgrammingError):
                await cursor.fetchone()

    @pytest.mark.asyncio
    async def test_execute_iter(self, async_connection: AsyncConnection):
        async with await async_connection.execute(
            "select 1 union all select 2"
        ) as cursor:
            assert [row async for row in cursor] == [(1,), (2,)]

    @pytest.mark.asyncio
    async def test_execute_map_iter(self, async_connection: AsyncConnection):
        async with await async_connection.execute_map(
            Row, "select 1 union all select 2"
        ) as cursor:
            assert [row async for row in cursor] == [Row(id=1), Row(id=2)]

    @pytest.mark.asyncio
    async def test_connection_close(self, async_connection: AsyncConnection):
        await async_connection.close()

    @pytest.mark.asyncio
    async def test_connection_commit(self, async_connection: AsyncConnection):
        await async_connection.commit()

    @pytest.mark.asyncio
    async def test_connection_rollback(self, async_connection: AsyncConnection):
        await async_connection.rollback()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "query,rowcount",
        [
            ("select 1", 1),
            ("select 1 union all select 2", 2),
            ("select 1 union all select 2 union all select 3", 3),
        ],
    )
    async def test_cursor_rowcount(
        self, query: str, rowcount: int, async_connection: AsyncConnection
    ):
        async with await async_connection.execute(query) as cursor:
            assert cursor.rowcount == rowcount

    @pytest.mark.asyncio
    async def test_cursor_arraysize(self, async_connection: AsyncConnection):
        async with await async_connection.cursor() as cursor:
            assert cursor.arraysize == 1

    @pytest.mark.asyncio
    async def test_cursor_arraysize_setter(self, async_connection: AsyncConnection):
        async with await async_connection.cursor() as cursor:
            cursor.arraysize = 2
            assert cursor.arraysize == 2
