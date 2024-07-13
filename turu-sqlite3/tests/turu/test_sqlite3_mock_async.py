from dataclasses import dataclass
from typing import NamedTuple

import pytest
from turu.core import tag
from turu.core.mock.exception import (
    TuruMockResponseTypeMismatchError,
    TuruMockUnexpectedFetchError,
)
from turu.sqlite3.mock_async_connection import MockAsyncConnection


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

    @pytest.mark.asyncio
    async def test_execute_with_tag(self, mock_async_connection: MockAsyncConnection):
        @dataclass
        class Table:
            pass

        mock_async_connection.inject_operation_with_tag(tag.Insert[Table])

        async with await mock_async_connection.cursor() as cursor:
            assert (
                await (
                    await cursor.execute_with_tag(tag.Insert[Table], "INSERT table")
                ).fetchone()
            ) is None

    @pytest.mark.asyncio
    async def test_execute_with_tag_when_other_table(
        self, mock_async_connection: MockAsyncConnection
    ):
        @dataclass
        class Table:
            pass

        @dataclass
        class OtherTable:
            pass

        mock_async_connection.inject_operation_with_tag(tag.Insert[Table])

        with pytest.raises(TuruMockResponseTypeMismatchError):
            async with await mock_async_connection.cursor() as cursor:
                await cursor.execute_with_tag(tag.Insert[OtherTable], "INSERT table")

    @pytest.mark.asyncio
    async def test_execute_with_tag_when_other_operation(
        self, mock_async_connection: MockAsyncConnection
    ):
        @dataclass
        class Table:
            pass

        mock_async_connection.inject_operation_with_tag(tag.Insert[Table])

        with pytest.raises(TuruMockResponseTypeMismatchError):
            async with await mock_async_connection.cursor() as cursor:
                await cursor.execute_with_tag(tag.Update[Table], "UPDATE table")

    @pytest.mark.asyncio
    async def test_executemany_with_tag(
        self, mock_async_connection: MockAsyncConnection
    ):
        @dataclass
        class Table:
            pass

        mock_async_connection.inject_operation_with_tag(tag.Insert[Table])

        async with await mock_async_connection.cursor() as cursor:
            assert (
                await (
                    await cursor.executemany_with_tag(
                        tag.Insert[Table], "INSERT table", []
                    )
                ).fetchone()
            ) is None

    @pytest.mark.asyncio
    async def test_executemany_with_tag_when_other_table(
        self, mock_async_connection: MockAsyncConnection
    ):
        @dataclass
        class Table:
            pass

        @dataclass
        class OtherTable:
            pass

        mock_async_connection.inject_operation_with_tag(tag.Insert[Table])

        with pytest.raises(TuruMockResponseTypeMismatchError):
            async with await mock_async_connection.cursor() as cursor:
                await cursor.executemany_with_tag(
                    tag.Insert[OtherTable], "INSERT table", []
                )

    @pytest.mark.asyncio
    async def test_executemany_with_tag_when_other_operation(
        self, mock_async_connection: MockAsyncConnection
    ):
        @dataclass
        class Table:
            pass

        mock_async_connection.inject_operation_with_tag(tag.Insert[Table])

        with pytest.raises(TuruMockResponseTypeMismatchError):
            async with await mock_async_connection.cursor() as cursor:
                await cursor.executemany_with_tag(tag.Update[Table], "UPDATE table", [])
