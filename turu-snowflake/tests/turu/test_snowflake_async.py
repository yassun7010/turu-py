import os
from typing import NamedTuple

import pytest
import turu.snowflake
from turu.snowflake.features import USE_PANDAS, USE_PYARROW


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
        async with await async_connection.execute_map(Row, "select 1") as cursor:
            assert await cursor.fetchone() == Row(1)

    @pytest.mark.asyncio
    async def test_execute_map_fetchmany(
        self, async_connection: turu.snowflake.AsyncConnection
    ):
        cursor = await async_connection.execute_map(Row, "select 1 union all select 2")
        assert await cursor.fetchmany() == [Row(1)]
        assert await cursor.fetchone() == Row(2)
        assert await cursor.fetchone() is None

    @pytest.mark.asyncio
    async def test_execute_map_fetchmany_with_size(
        self, async_connection: turu.snowflake.AsyncConnection
    ):
        cursor = await async_connection.execute_map(
            Row, "select 1 union all select 2 union all select 3"
        )

        assert await cursor.fetchmany(2) == [Row(1), Row(2)]
        assert await cursor.fetchmany(2) == [Row(3)]

    @pytest.mark.asyncio
    async def test_execute_map_fetchall(
        self, async_connection: turu.snowflake.AsyncConnection
    ):
        cursor = await async_connection.execute_map(Row, "select 1 union all select 2")

        assert await cursor.fetchall() == [Row(1), Row(2)]
        assert await cursor.fetchone() is None

    @pytest.mark.asyncio
    async def test_executemany(self, async_connection: turu.snowflake.AsyncConnection):
        cursor = await async_connection.executemany(
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
        cursor = await async_connection.cursor()
        assert cursor.rowcount == -1

    @pytest.mark.asyncio
    async def test_cursor_arraysize(
        self, async_connection: turu.snowflake.AsyncConnection
    ):
        cursor = await async_connection.cursor()
        assert cursor.arraysize == 1

    @pytest.mark.asyncio
    async def test_cursor_arraysize_setter(
        self, async_connection: turu.snowflake.AsyncConnection
    ):
        cursor = await async_connection.cursor()
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
        async with await async_connection.execute_map(
            Row, "select 1", timeout=10
        ) as cursor:
            assert await cursor.fetchone() == Row(1)

    @pytest.mark.asyncio
    async def test_cursor_num_statements(
        self, async_connection: turu.snowflake.AsyncConnection
    ):
        async with await async_connection.execute_map(
            Row, "select 1; select 2;", num_statements=2
        ) as cursor:
            assert await cursor.fetchall() == [Row(1)]
            assert await cursor.fetchone() is None

    @pytest.mark.asyncio
    async def test_cursor_use_warehouse(
        self, async_connection: turu.snowflake.AsyncConnection
    ):
        async with await (
            (await async_connection.cursor())
            .use_warehouse(os.environ["SNOWFLAKE_WAREHOUSE"])
            .execute_map(Row, "select 1")
        ) as cursor:
            assert await cursor.fetchone() == Row(1)

    @pytest.mark.asyncio
    async def test_cursor_use_schema(
        self, async_connection: turu.snowflake.AsyncConnection
    ):
        async with await (
            (await async_connection.cursor())
            .use_schema(os.environ["SNOWFLAKE_SCHEMA"])
            .execute_map(Row, "select 1")
        ) as cursor:
            assert await cursor.fetchone() == Row(1)

    @pytest.mark.asyncio
    async def test_cursor_use_database(
        self, async_connection: turu.snowflake.AsyncConnection
    ):
        async with await (
            (await async_connection.cursor())
            .use_database(os.environ["SNOWFLAKE_DATABASE"])
            .execute_map(Row, "select 1")
        ) as cursor:
            assert await cursor.fetchone() == Row(1)

    @pytest.mark.asyncio
    async def test_cursor_use_role(
        self, async_connection: turu.snowflake.AsyncConnection
    ):
        async with await (
            (await async_connection.cursor())
            .use_role(os.environ["SNOWFLAKE_ROLE"])
            .execute_map(Row, "select 1")
        ) as cursor:
            assert await cursor.fetchone() == Row(1)

    @pytest.mark.skipif(
        not (USE_PYARROW and USE_PANDAS),
        reason="pyarrow is not installed",
    )
    @pytest.mark.asyncio
    async def test_fetch_arrow_all(
        self, async_connection: turu.snowflake.AsyncConnection
    ):
        cursor = await async_connection.execute(
            "select 1 as ID union all select 2 as ID"
        )

        expected = {"ID": {0: 1, 1: 2}}

        assert (await cursor.fetch_arrow_all()).to_pandas().to_dict() == expected  # type: ignore[union-attr]

    @pytest.mark.skipif(
        not (USE_PYARROW and USE_PANDAS),
        reason="pyarrow is not installed",
    )
    @pytest.mark.asyncio
    async def test_fetch_arrow_batches(
        self, async_connection: turu.snowflake.AsyncConnection
    ):
        from pandas import DataFrame
        from pandas.testing import assert_frame_equal

        cursor = await async_connection.execute(
            "select 1 as ID union all select 2 as ID"
        )

        for row in await cursor.fetch_arrow_batches():
            assert_frame_equal(row.to_pandas(), DataFrame({"ID": [1, 2]}, dtype="int8"))

    @pytest.mark.skipif(
        not USE_PANDAS,
        reason="pandas is not installed",
    )
    @pytest.mark.asyncio
    async def test_fetch_pandas_all(
        self, async_connection: turu.snowflake.AsyncConnection
    ):
        cursor = await async_connection.execute("select 1 as ID union all select 2 ID")

        assert (await cursor.fetch_pandas_all()).to_dict() == {"ID": {0: 1, 1: 2}}

    @pytest.mark.skipif(
        not USE_PANDAS,
        reason="pandas is not installed",
    )
    @pytest.mark.asyncio
    async def test_fetch_pandas_batches(
        self, async_connection: turu.snowflake.AsyncConnection
    ):
        from pandas import DataFrame
        from pandas.testing import assert_frame_equal

        cursor = await async_connection.execute(
            "select 1 as ID union all select 2 AS ID"
        )

        for df in await cursor.fetch_pandas_batches():
            assert_frame_equal(df, DataFrame({"ID": [1, 2]}, dtype="int8"))
