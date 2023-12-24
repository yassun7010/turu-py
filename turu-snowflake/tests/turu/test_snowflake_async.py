import os
from typing import NamedTuple

import pytest
import turu.snowflake


class TestTuruSnowflake:
    def test_version(self):
        assert turu.snowflake.__version__


class Row(NamedTuple):
    id: int


@pytest.mark.skipif(
    condition="USE_REAL_CONNECTION" not in os.environ
    or os.environ["USE_REAL_CONNECTION"].lower() != "true",
    reason="USE_REAL_CONNECTION flag is not set.",
)
class TestTuruSnowflakeAsyncConnection:
    @pytest.mark.asyncio
    async def test_execute(self, async_connection: turu.snowflake.AsyncConnection):
        cursor = await async_connection.execute("select 1")
        assert await cursor.fetchall() == [(1,)]

    @pytest.mark.asyncio
    async def test_execute_fetchone(
        self, async_connection: turu.snowflake.AsyncConnection
    ):
        cursor = await async_connection.execute("select 1")
        assert await cursor.fetchone() == (1,)

    @pytest.mark.asyncio
    async def test_execute_map_fetchone(
        self, async_connection: turu.snowflake.AsyncConnection
    ):
        async with await async_connection.cursor().execute_map(
            Row, "select 1"
        ) as cursor:
            assert await cursor.fetchone() == Row(1)

    @pytest.mark.asyncio
    async def test_execute_map_fetchmany(
        self, async_connection: turu.snowflake.AsyncConnection
    ):
        cursor = await async_connection.cursor().execute_map(
            Row, "select 1 union all select 2"
        )
        assert await cursor.fetchmany() == [Row(1)]
        assert await cursor.fetchone() == Row(2)
        assert await cursor.fetchone() is None

    @pytest.mark.asyncio
    async def test_execute_map_fetchmany_with_size(
        self, async_connection: turu.snowflake.AsyncConnection
    ):
        cursor = await async_connection.cursor().execute_map(
            Row, "select 1 union all select 2 union all select 3"
        )

        assert await cursor.fetchmany(2) == [Row(1), Row(2)]
        assert await cursor.fetchmany(2) == [Row(3)]

    @pytest.mark.asyncio
    async def test_execute_map_fetchall(
        self, async_connection: turu.snowflake.AsyncConnection
    ):
        cursor = await async_connection.cursor().execute_map(
            Row, "select 1 union all select 2"
        )

        assert await cursor.fetchall() == [Row(1), Row(2)]
        assert await cursor.fetchone() is None

    @pytest.mark.asyncio
    async def test_executemany(self, async_connection: turu.snowflake.AsyncConnection):
        cursor = await async_connection.cursor().executemany(
            "select 1 union all select 2", [(), ()]
        )

        assert await cursor.fetchall() == [(1,), (2,)]
        assert await cursor.fetchone() is None

    @pytest.mark.asyncio
    async def test_executemany_map(
        self, async_connection: turu.snowflake.AsyncConnection
    ):
        async with await async_connection.executemany_map(
            Row, "select 1", [(), ()]
        ) as cursor:
            assert await cursor.fetchone() == Row(1)
            assert await cursor.fetchone() is None

    @pytest.mark.asyncio
    async def test_execute_iter(self, async_connection: turu.snowflake.AsyncConnection):
        async with await async_connection.execute(
            "select 1 union all select 2"
        ) as cursor:
            assert [row async for row in cursor] == [(1,), (2,)]

    @pytest.mark.asyncio
    async def test_execute_map_iter(
        self, async_connection: turu.snowflake.AsyncConnection
    ):
        async with await async_connection.execute_map(
            Row, "select 1 union all select 2"
        ) as cursor:
            assert [row async for row in cursor] == [Row(1), Row(2)]

    @pytest.mark.asyncio
    async def test_connection_close(
        self, async_connection: turu.snowflake.AsyncConnection
    ):
        await async_connection.close()

    @pytest.mark.asyncio
    async def test_connection_commit(
        self, async_connection: turu.snowflake.AsyncConnection
    ):
        await async_connection.commit()

    @pytest.mark.asyncio
    async def test_connection_rollback(
        self, async_connection: turu.snowflake.AsyncConnection
    ):
        await async_connection.rollback()

    @pytest.mark.asyncio
    async def test_cursor_rowcount(
        self, async_connection: turu.snowflake.AsyncConnection
    ):
        cursor = async_connection.cursor()
        assert cursor.rowcount == -1

    @pytest.mark.asyncio
    async def test_cursor_arraysize(
        self, async_connection: turu.snowflake.AsyncConnection
    ):
        cursor = async_connection.cursor()
        assert cursor.arraysize == 1

    @pytest.mark.asyncio
    async def test_cursor_arraysize_setter(
        self, async_connection: turu.snowflake.AsyncConnection
    ):
        cursor = async_connection.cursor()
        cursor.arraysize = 2
        assert cursor.arraysize == 2

    @pytest.mark.asyncio
    async def test_connection_timeout(
        self, async_connection: turu.snowflake.AsyncConnection
    ):
        async with await async_connection.execute_map(
            Row, "select 1", timeout=10
        ) as cursor:
            assert await cursor.fetchone() == Row(1)

    @pytest.mark.asyncio
    async def test_connection_num_statements(
        self, async_connection: turu.snowflake.AsyncConnection
    ):
        async with await async_connection.execute_map(
            Row, "select 1; select 2;", num_statements=2
        ) as cursor:
            assert await cursor.fetchall() == [Row(1)]
            assert await cursor.fetchone() is None

    @pytest.mark.asyncio
    async def test_cursor_timeout(
        self, async_connection: turu.snowflake.AsyncConnection
    ):
        async with await async_connection.cursor().execute_map(
            Row, "select 1", timeout=10
        ) as cursor:
            assert await cursor.fetchone() == Row(1)

    @pytest.mark.asyncio
    async def test_cursor_num_statements(
        self, async_connection: turu.snowflake.AsyncConnection
    ):
        async with await async_connection.cursor().execute_map(
            Row, "select 1; select 2;", num_statements=2
        ) as cursor:
            assert await cursor.fetchall() == [Row(1)]
            assert await cursor.fetchone() is None

    @pytest.mark.asyncio
    async def test_cursor_use_warehouse(
        self, async_connection: turu.snowflake.AsyncConnection
    ):
        async with await (
            async_connection.cursor()
            .use_warehouse(os.environ["SNOWFLAKE_WAREHOUSE"])
            .execute_map(Row, "select 1")
        ) as cursor:
            assert await cursor.fetchone() == Row(1)

    @pytest.mark.asyncio
    async def test_cursor_use_schema(
        self, async_connection: turu.snowflake.AsyncConnection
    ):
        async with await (
            async_connection.cursor()
            .use_schema(os.environ["SNOWFLAKE_SCHEMA"])
            .execute_map(Row, "select 1")
        ) as cursor:
            assert await cursor.fetchone() == Row(1)

    @pytest.mark.asyncio
    async def test_cursor_use_database(
        self, async_connection: turu.snowflake.AsyncConnection
    ):
        async with await (
            async_connection.cursor()
            .use_database(os.environ["SNOWFLAKE_DATABASE"])
            .execute_map(Row, "select 1")
        ) as cursor:
            assert await cursor.fetchone() == Row(1)

    @pytest.mark.asyncio
    async def test_cursor_use_role(
        self, async_connection: turu.snowflake.AsyncConnection
    ):
        async with await (
            async_connection.cursor()
            .use_role(os.environ["SNOWFLAKE_ROLE"])
            .execute_map(Row, "select 1")
        ) as cursor:
            assert await cursor.fetchone() == Row(1)
