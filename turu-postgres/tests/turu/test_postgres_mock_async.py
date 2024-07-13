from dataclasses import dataclass

import pytest
import turu.postgres
from pydantic import BaseModel
from turu.core import tag
from turu.core.mock.exception import TuruMockResponseTypeMismatchError


class Row(BaseModel):
    id: int


class TestTuruPostgresMockAsync:
    @pytest.mark.asyncio
    async def test_execute(
        self, mock_async_connection: turu.postgres.MockAsyncConnection
    ):
        expected = [(1,)]
        mock_async_connection.inject_response(None, expected)

        async with await mock_async_connection.execute("select 1") as cursor:
            assert await cursor.fetchall() == expected

    @pytest.mark.asyncio
    async def test_execute_fetchone(
        self, mock_async_connection: turu.postgres.MockAsyncConnection
    ):
        expected = [(1,)]
        mock_async_connection.inject_response(None, expected)

        async with await mock_async_connection.execute("select 1") as cursor:
            assert await cursor.fetchone() == expected[0]

    @pytest.mark.asyncio
    async def test_execute_map_fetchone(
        self, mock_async_connection: turu.postgres.MockAsyncConnection
    ):
        expected = [Row(id=1)]
        mock_async_connection.inject_response(Row, expected)

        async with await mock_async_connection.execute_map(Row, "select 1") as cursor:
            assert await cursor.fetchone() == expected[0]

    @pytest.mark.asyncio
    async def test_execute_map_fetchmany(
        self, mock_async_connection: turu.postgres.MockAsyncConnection
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
        self, mock_async_connection: turu.postgres.MockAsyncConnection
    ):
        mock_async_connection.inject_response(Row, [Row(id=1), Row(id=2), Row(id=3)])

        async with await mock_async_connection.execute_map(
            Row, "select 1 union all select 2 union all select 3"
        ) as cursor:
            assert await cursor.fetchmany(2) == [Row(id=1), Row(id=2)]
            assert await cursor.fetchmany(2) == [Row(id=3)]

    @pytest.mark.asyncio
    async def test_execute_map_fetchall(
        self, mock_async_connection: turu.postgres.MockAsyncConnection
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
        self, mock_async_connection: turu.postgres.MockAsyncConnection
    ):
        expected = [(1,)]
        mock_async_connection.inject_response(None, expected)

        async with await mock_async_connection.executemany("select 1", []) as cursor:
            assert await cursor.fetchall() == expected

    @pytest.mark.asyncio
    async def test_executemany_map(
        self, mock_async_connection: turu.postgres.MockAsyncConnection
    ):
        expected = [Row(id=1), Row(id=2)]
        mock_async_connection.inject_response(Row, expected)

        async with await mock_async_connection.executemany_map(
            Row, "select 1 union all select 2", []
        ) as cursor:
            assert await cursor.fetchall() == expected

    @pytest.mark.asyncio
    async def test_execute_iter(
        self, mock_async_connection: turu.postgres.MockAsyncConnection
    ):
        expected = [(1,), (2,)]
        mock_async_connection.inject_response(None, expected)

        async with await mock_async_connection.execute(
            "select 1 union all select 2"
        ) as cursor:
            assert [row async for row in cursor] == expected

    @pytest.mark.asyncio
    async def test_execute_map_iter(
        self, mock_async_connection: turu.postgres.MockAsyncConnection
    ):
        expected = [Row(id=1), Row(id=2)]
        mock_async_connection.inject_response(Row, expected)
        async with await mock_async_connection.execute_map(
            Row, "select 1 union all select 2"
        ) as cursor:
            assert [row async for row in cursor] == expected

    @pytest.mark.asyncio
    async def test_mock_connection_close(
        self, mock_async_connection: turu.postgres.MockAsyncConnection
    ):
        await mock_async_connection.close()

    @pytest.mark.asyncio
    async def test_mock_connection_commit(
        self, mock_async_connection: turu.postgres.MockAsyncConnection
    ):
        await mock_async_connection.commit()

    @pytest.mark.asyncio
    async def test_mock_connection_rollback(
        self, mock_async_connection: turu.postgres.MockAsyncConnection
    ):
        await mock_async_connection.rollback()

    @pytest.mark.asyncio
    async def test_cursor_rowcount(
        self, mock_async_connection: turu.postgres.MockAsyncConnection
    ):
        cursor = await mock_async_connection.cursor()
        assert cursor.rowcount == -1

    @pytest.mark.asyncio
    async def test_cursor_arraysize(
        self, mock_async_connection: turu.postgres.MockAsyncConnection
    ):
        cursor = await mock_async_connection.cursor()
        assert cursor.arraysize == 1

    @pytest.mark.asyncio
    async def test_cursor_arraysize_setter(
        self, mock_async_connection: turu.postgres.MockAsyncConnection
    ):
        cursor = await mock_async_connection.cursor()
        cursor.arraysize = 2
        assert cursor.arraysize == 2

    @pytest.mark.asyncio
    async def test_execute_with_tag(
        self, mock_async_connection: turu.postgres.MockAsyncConnection
    ):
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
        self, mock_async_connection: turu.postgres.MockAsyncConnection
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
        self, mock_async_connection: turu.postgres.MockAsyncConnection
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
        self, mock_async_connection: turu.postgres.MockAsyncConnection
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
        self, mock_async_connection: turu.postgres.MockAsyncConnection
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
        self, mock_async_connection: turu.postgres.MockAsyncConnection
    ):
        @dataclass
        class Table:
            pass

        mock_async_connection.inject_operation_with_tag(tag.Insert[Table])

        with pytest.raises(TuruMockResponseTypeMismatchError):
            async with await mock_async_connection.cursor() as cursor:
                await cursor.executemany_with_tag(tag.Update[Table], "UPDATE table", [])
