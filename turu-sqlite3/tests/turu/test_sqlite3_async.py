from typing import NamedTuple

import pytest
import turu.sqlite3


def test_turu_sqlite3_version():
    assert turu.sqlite3.__version__


class Row(NamedTuple):
    id: int
    name: str


class TestTuruSqlite3Async:
    @pytest.mark.asyncio
    async def test_execute_fetchone(
        self, async_connection: turu.sqlite3.AsyncConnection
    ):
        cursor = await async_connection.execute("select 1")
        assert await cursor.fetchone() == (1,)
        assert await cursor.fetchone() is None

    @pytest.mark.asyncio
    async def test_execute_fetchmany(
        self, async_connection: turu.sqlite3.AsyncConnection
    ):
        cursor = await async_connection.execute("select 1 union all select 2")
        assert await cursor.fetchmany() == [(1,)]
        assert await cursor.fetchmany() == [(2,)]
        assert await cursor.fetchmany() == []
        assert await cursor.fetchone() is None

    @pytest.mark.asyncio
    async def test_execute_fetchmany_with_size(
        self, async_connection: turu.sqlite3.AsyncConnection
    ):
        cursor = await async_connection.execute(
            "select 1 union all select 2 union all select 3"
        )
        assert await cursor.fetchmany(2) == [(1,), (2,)]
        assert await cursor.fetchmany(2) == [(3,)]
        assert await cursor.fetchone() is None

    @pytest.mark.asyncio
    async def test_execute_fetchall(
        self, async_connection: turu.sqlite3.AsyncConnection
    ):
        cursor = await async_connection.execute("select 1")
        assert await cursor.fetchall() == [(1,)]

    @pytest.mark.asyncio
    async def test_execute_iter(self, async_connection: turu.sqlite3.AsyncConnection):
        cursor = await async_connection.execute("select 1 union all select 2")
        assert [row async for row in cursor] == [(1,), (2,)]

    @pytest.mark.asyncio
    async def test_execute_map(self, async_connection: turu.sqlite3.AsyncConnection):
        class Row(NamedTuple):
            id: int
            name: str

        cursor = await async_connection.execute_map(Row, "select 1, 'a'")

        assert (await cursor.__anext__()) == Row(1, "a")

    @pytest.mark.asyncio
    async def test_execute_map_fetchone(
        self, async_connection: turu.sqlite3.AsyncConnection
    ):
        cursor = await async_connection.execute_map(Row, "select 1, 'a'")

        assert await cursor.fetchone() == Row(1, "a")
        assert await cursor.fetchone() is None

    @pytest.mark.asyncio
    async def test_execute_map_fetchmany(
        self, async_connection: turu.sqlite3.AsyncConnection
    ):
        cursor = await async_connection.execute_map(
            Row, "select 1, 'a' union all select 2, 'b'"
        )

        assert await cursor.fetchmany() == [Row(1, "a")]
        assert await cursor.fetchmany() == [Row(2, "b")]
        assert await cursor.fetchmany() == []

    @pytest.mark.asyncio
    async def test_execute_map_fetchall(
        self, async_connection: turu.sqlite3.AsyncConnection
    ):
        cursor = await async_connection.execute_map(
            Row, "select 1, 'a' union all select 2, 'b'"
        )

        assert await cursor.fetchall() == [Row(1, "a"), Row(2, "b")]
        assert await cursor.fetchall() == []

    @pytest.mark.asyncio
    async def test_connection_commit(
        self, async_connection: turu.sqlite3.AsyncConnection
    ):
        await async_connection.commit()

    @pytest.mark.asyncio
    async def test_connection_rollback(
        self, async_connection: turu.sqlite3.AsyncConnection
    ):
        await async_connection.rollback()

    @pytest.mark.asyncio
    async def test_cursor_rowcount(
        self, async_connection: turu.sqlite3.AsyncConnection
    ):
        cursor = await async_connection.cursor()
        assert cursor.rowcount == -1

    @pytest.mark.asyncio
    async def test_cursor_arraysize(
        self, async_connection: turu.sqlite3.AsyncConnection
    ):
        cursor = await async_connection.cursor()
        assert cursor.arraysize == 1

        cursor.arraysize = 2
        assert cursor.arraysize == 2

    @pytest.mark.asyncio
    async def test_cursor_close(self, async_connection: turu.sqlite3.AsyncConnection):
        async with await async_connection.cursor() as cursor:
            await cursor.close()
