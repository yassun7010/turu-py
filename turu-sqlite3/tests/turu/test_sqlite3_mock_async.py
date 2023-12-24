from typing import NamedTuple

import pytest
from turu.core.mock.exception import TuruMockUnexpectedFetchError
from turu.sqlite3.async_connection import MockAsyncConnection


class Row(NamedTuple):
    id: int


class TestTuruSqlite3MockAsync:
    @pytest.mark.asyncio
    async def test_mock_execute(self, mock_async_connection: MockAsyncConnection):
        mock_async_connection.inject_response(None, [(1,)])

        cursor = await mock_async_connection.execute("SELECT 1")
        assert await cursor.fetchall() == [(1,)]

    @pytest.mark.asyncio
    async def test_mock_execute_without_response_data(
        self, mock_async_connection: MockAsyncConnection
    ):
        mock_async_connection.inject_response(None)

        cursor = await mock_async_connection.execute("SELECT 1")
        with pytest.raises(TuruMockUnexpectedFetchError):
            assert await cursor.fetchall()

    @pytest.mark.asyncio
    async def test_mock_execute_map(self, mock_async_connection: MockAsyncConnection):
        expected = [Row(1)]
        mock_async_connection.inject_response(Row, expected)
        cursor = await mock_async_connection.execute_map(Row, "SELECT 1")
        assert [row async for row in cursor] == expected

    @pytest.mark.asyncio
    async def test_mock_executemany(self, mock_async_connection: MockAsyncConnection):
        mock_async_connection.inject_response(None)

        cursor = await mock_async_connection.cursor()
        await cursor.executemany("SELECT 1", [])

    @pytest.mark.parametrize("rowsize", range(5))
    @pytest.mark.asyncio
    async def test_mock_execute_map_fetchone(
        self, mock_async_connection: MockAsyncConnection, rowsize: int
    ):
        expected = [Row(i) for i in range(rowsize)]
        mock_async_connection.inject_response(Row, expected)

        async with await mock_async_connection.execute_map(Row, "SELECT 1") as cursor:
            for i in range(rowsize):
                assert await cursor.fetchone() == expected[i]

            assert await cursor.fetchone() is None

    @pytest.mark.parametrize("rowsize", range(5))
    @pytest.mark.asyncio
    async def test_mock_execute_map_fetchmany(
        self,
        mock_async_connection: MockAsyncConnection,
        rowsize: int,
    ):
        expected = [Row(i) for i in range(rowsize)]
        mock_async_connection.inject_response(Row, expected)

        async with await mock_async_connection.execute_map(Row, "SELECT 1") as cursor:
            assert await cursor.fetchmany(rowsize) == expected
            assert await cursor.fetchone() is None

    @pytest.mark.parametrize("rowsize", range(5))
    @pytest.mark.asyncio
    async def test_mock_execute_map_fetchall(
        self, mock_async_connection: MockAsyncConnection, rowsize: int
    ):
        expected = [Row(i) for i in range(rowsize)]
        mock_async_connection.inject_response(Row, expected)

        async with await mock_async_connection.execute_map(Row, "SELECT 1") as cursor:
            assert await cursor.fetchall() == expected
            assert await cursor.fetchall() == []
