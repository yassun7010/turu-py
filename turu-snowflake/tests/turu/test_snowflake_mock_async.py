from typing import NamedTuple

import pytest
import turu.snowflake


class Row(NamedTuple):
    id: int


class TestTuruSnowflakeMockAsyncConnection:
    @pytest.mark.asyncio
    async def test_execute(
        self, mock_async_connection: turu.snowflake.MockAsyncConnection
    ):
        mock_async_connection.inject_response(None, [(1,)])
        cursor = await mock_async_connection.cursor().execute("select 1")
        assert await cursor.fetchone() == (1,)
        assert await cursor.fetchone() is None

    @pytest.mark.asyncio
    async def test_execute_map_fetchone(
        self, mock_async_connection: turu.snowflake.MockAsyncConnection
    ):
        expected = [Row(1), Row(2)]

        mock_async_connection.inject_response(Row, expected)
        cursor = await mock_async_connection.cursor().execute_map(
            Row, "select 1 union all select 2"
        )

        assert await cursor.fetchall() == expected

    @pytest.mark.asyncio
    async def test_execute_map_fetchmany(
        self, mock_async_connection: turu.snowflake.MockAsyncConnection
    ):
        expected = [Row(1), Row(2)]
        (
            mock_async_connection.inject_response(Row, expected).inject_response(
                Row, expected
            )
        )

        cursor = await mock_async_connection.cursor().execute_map(
            Row, "select 1 union all select 2"
        )

        assert await cursor.fetchmany() == [Row(1)]
        assert await cursor.fetchone() == Row(2)
        assert await cursor.fetchone() is None

    @pytest.mark.asyncio
    async def test_execute_map_fetchmany_with_size(
        self, mock_async_connection: turu.snowflake.MockAsyncConnection
    ):
        expected = [Row(1), Row(2), Row(3)]
        mock_async_connection.inject_response(Row, expected)

        cursor = await mock_async_connection.cursor().execute_map(
            Row, "select 1 union all select 2 union all select 3"
        )

        assert await cursor.fetchmany(2) == [Row(1), Row(2)]
        assert await cursor.fetchone() == Row(3)
        assert await cursor.fetchone() is None

    @pytest.mark.asyncio
    async def test_execute_map_fetchall(
        self, mock_async_connection: turu.snowflake.MockAsyncConnection
    ):
        expected = [Row(1), Row(2)]
        mock_async_connection.inject_response(Row, expected)

        cursor = await mock_async_connection.cursor().execute_map(
            Row, "select 1 union all select 2"
        )

        assert await cursor.fetchall() == [Row(1), Row(2)]
        assert await cursor.fetchone() is None

    @pytest.mark.asyncio
    async def test_executemany(
        self, mock_async_connection: turu.snowflake.MockAsyncConnection
    ):
        expected = [(1,), (2,)]
        mock_async_connection.inject_response(None, expected)

        cursor = await mock_async_connection.cursor().executemany(
            "select 1 union all select 2", []
        )

        assert await cursor.fetchall() == expected
        assert await cursor.fetchone() is None

    @pytest.mark.asyncio
    async def test_executemany_map(
        self, mock_async_connection: turu.snowflake.MockAsyncConnection
    ):
        expected = [Row(1)]
        mock_async_connection.inject_response(Row, expected)

        cursor = await mock_async_connection.cursor().executemany_map(
            Row, "select 1", []
        )

        assert await cursor.fetchone() == expected[0]
        assert await cursor.fetchone() is None

    @pytest.mark.asyncio
    async def test_connection_timeout(
        self, mock_async_connection: turu.snowflake.MockAsyncConnection
    ):
        expected = [Row(1)]
        mock_async_connection.inject_response(Row, expected)
        async with await mock_async_connection.cursor().execute_map(
            Row, "select 1", timeout=10
        ) as cursor:
            assert await cursor.fetchmany() == expected

    @pytest.mark.asyncio
    async def test_connection_num_statements(
        self, mock_async_connection: turu.snowflake.MockAsyncConnection
    ):
        expected = [Row(1)]
        mock_async_connection.inject_response(Row, expected)
        async with await mock_async_connection.cursor().execute_map(
            Row, "select 1; select 2;", num_statements=2
        ) as cursor:
            assert await cursor.fetchall() == expected
            assert await cursor.fetchone() is None

    @pytest.mark.asyncio
    async def test_cursor_timeout(
        self, mock_async_connection: turu.snowflake.MockAsyncConnection
    ):
        expected = [Row(1)]
        mock_async_connection.inject_response(Row, expected)
        async with await mock_async_connection.cursor().execute_map(
            Row, "select 1", timeout=10
        ) as cursor:
            assert await cursor.fetchmany() == expected

    @pytest.mark.asyncio
    async def test_cursor_num_statements(
        self, mock_async_connection: turu.snowflake.MockAsyncConnection
    ):
        expected = [Row(1)]
        mock_async_connection.inject_response(Row, expected)
        async with await mock_async_connection.cursor().execute_map(
            Row, "select 1; select 2;", num_statements=2
        ) as cursor:
            assert await cursor.fetchall() == expected
            assert await cursor.fetchone() is None

    @pytest.mark.asyncio
    async def test_cursor_use_warehouse(
        self, mock_async_connection: turu.snowflake.MockAsyncConnection
    ):
        expected = [Row(1)]
        mock_async_connection.inject_response(Row, expected)
        async with await (
            mock_async_connection.cursor()
            .use_warehouse("test_warehouse")
            .execute_map(
                Row,
                "select 1",
            )
        ) as cursor:
            assert await cursor.fetchmany() == expected

    @pytest.mark.asyncio
    async def test_cursor_use_database(
        self, mock_async_connection: turu.snowflake.MockAsyncConnection
    ):
        expected = [Row(1)]
        mock_async_connection.inject_response(Row, expected)
        async with await (
            mock_async_connection.cursor()
            .use_database("test_database")
            .execute_map(
                Row,
                "select 1",
            )
        ) as cursor:
            assert await cursor.fetchmany() == expected

    @pytest.mark.asyncio
    async def test_cursor_use_schema(
        self, mock_async_connection: turu.snowflake.MockAsyncConnection
    ):
        expected = [Row(1)]
        mock_async_connection.inject_response(Row, expected)
        async with await (
            mock_async_connection.cursor()
            .use_schema("test_schema")
            .execute_map(
                Row,
                "select 1",
            )
        ) as cursor:
            assert await cursor.fetchmany() == expected

    @pytest.mark.asyncio
    async def test_cursor_use_role(
        self, mock_async_connection: turu.snowflake.MockAsyncConnection
    ):
        expected = [Row(1)]
        mock_async_connection.inject_response(Row, expected)
        async with await (
            mock_async_connection.cursor()
            .use_role("test_role")
            .execute_map(
                Row,
                "select 1",
            )
        ) as cursor:
            assert await cursor.fetchmany() == expected
