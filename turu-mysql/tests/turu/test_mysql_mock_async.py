import pytest
import turu.mysql
from pydantic import BaseModel


class Row(BaseModel):
    id: int


class TestTuruMysqlMockAsync:
    @pytest.mark.asyncio
    async def test_execute(self, mock_async_connection: turu.mysql.MockAsyncConnection):
        expected = [(1,)]
        mock_async_connection.inject_response(None, expected)

        async with await mock_async_connection.execute("select 1") as cursor:
            assert await cursor.fetchall() == expected

    @pytest.mark.asyncio
    async def test_execute_fetchone(
        self, mock_async_connection: turu.mysql.MockAsyncConnection
    ):
        expected = [(1,)]
        mock_async_connection.inject_response(None, expected)

        async with await mock_async_connection.execute("select 1") as cursor:
            assert await cursor.fetchone() == expected[0]

    @pytest.mark.asyncio
    async def test_execute_map_fetchone(
        self, mock_async_connection: turu.mysql.MockAsyncConnection
    ):
        expected = [Row(id=1)]
        mock_async_connection.inject_response(Row, expected)

        async with await mock_async_connection.execute_map(Row, "select 1") as cursor:
            assert await cursor.fetchone() == expected[0]

    @pytest.mark.asyncio
    async def test_execute_map_fetchmany(
        self, mock_async_connection: turu.mysql.MockAsyncConnection
    ):
        expected = [Row(id=1), Row(id=2)]
        mock_async_connection.inject_response(Row, expected)

        async with await mock_async_connection.execute_map(
            Row, "select 1 union all select 2"
        ) as cursor:
            assert await cursor.fetchmany() == [Row(id=1)]
            assert await cursor.fetchone() == Row(id=2)
            assert await cursor.fetchone() is None

    @pytest.mark.asyncio
    async def test_execute_map_fetchmany_with_size(
        self, mock_async_connection: turu.mysql.MockAsyncConnection
    ):
        mock_async_connection.inject_response(Row, [Row(id=1), Row(id=2), Row(id=3)])

        async with await mock_async_connection.execute_map(
            Row, "select 1 union all select 2 union all select 3"
        ) as cursor:
            assert await cursor.fetchmany(2) == [Row(id=1), Row(id=2)]
            assert await cursor.fetchmany(2) == [Row(id=3)]

    @pytest.mark.asyncio
    async def test_execute_map_fetchall(
        self, mock_async_connection: turu.mysql.MockAsyncConnection
    ):
        expected = [Row(id=1), Row(id=2)]

        mock_async_connection.inject_response(Row, expected)

        async with await mock_async_connection.execute_map(
            Row, "select 1 union all select 2"
        ) as cursor:
            assert await cursor.fetchall() == expected
            assert await cursor.fetchone() is None

    @pytest.mark.asyncio
    async def test_executemany(
        self, mock_async_connection: turu.mysql.MockAsyncConnection
    ):
        expected = [(1,)]
        mock_async_connection.inject_response(None, expected)

        async with await mock_async_connection.executemany("select 1", []) as cursor:
            assert await cursor.fetchall() == expected

    @pytest.mark.asyncio
    async def test_executemany_map(
        self, mock_async_connection: turu.mysql.MockAsyncConnection
    ):
        expected = [Row(id=1), Row(id=2)]
        mock_async_connection.inject_response(Row, expected)

        async with await mock_async_connection.executemany_map(
            Row, "select 1 union all select 2", []
        ) as cursor:
            assert await cursor.fetchall() == expected

    @pytest.mark.asyncio
    async def test_execute_iter(
        self, mock_async_connection: turu.mysql.MockAsyncConnection
    ):
        expected = [(1,), (2,)]
        mock_async_connection.inject_response(None, expected)

        async with await mock_async_connection.execute(
            "select 1 union all select 2"
        ) as cursor:
            assert [row async for row in cursor] == expected

    @pytest.mark.asyncio
    async def test_execute_map_iter(
        self, mock_async_connection: turu.mysql.MockAsyncConnection
    ):
        expected = [Row(id=1), Row(id=2)]
        mock_async_connection.inject_response(Row, expected)
        async with await mock_async_connection.execute_map(
            Row, "select 1 union all select 2"
        ) as cursor:
            assert [row async for row in cursor] == expected

    @pytest.mark.asyncio
    async def test_mock_connection_close(
        self, mock_async_connection: turu.mysql.MockAsyncConnection
    ):
        await mock_async_connection.close()

    @pytest.mark.asyncio
    async def test_mock_connection_commit(
        self, mock_async_connection: turu.mysql.MockAsyncConnection
    ):
        await mock_async_connection.commit()

    @pytest.mark.asyncio
    async def test_mock_connection_rollback(
        self, mock_async_connection: turu.mysql.MockAsyncConnection
    ):
        await mock_async_connection.rollback()

    @pytest.mark.asyncio
    async def test_cursor_rowcount(
        self, mock_async_connection: turu.mysql.MockAsyncConnection
    ):
        cursor = await mock_async_connection.cursor()
        assert cursor.rowcount == -1

    @pytest.mark.asyncio
    async def test_cursor_arraysize(
        self, mock_async_connection: turu.mysql.MockAsyncConnection
    ):
        cursor = await mock_async_connection.cursor()
        assert cursor.arraysize == 1

    @pytest.mark.asyncio
    async def test_cursor_arraysize_setter(
        self, mock_async_connection: turu.mysql.MockAsyncConnection
    ):
        cursor = await mock_async_connection.cursor()
        cursor.arraysize = 2
        assert cursor.arraysize == 2
