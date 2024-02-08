import os
import tempfile
from pathlib import Path
from textwrap import dedent
from typing import Annotated, NamedTuple, cast

import pytest
import turu.snowflake
from turu.core.record import record_to_csv
from turu.snowflake.features import USE_PANDAS, USE_PANDERA, USE_PYARROW
from typing_extensions import Never


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
    async def test_execute_map_named_tuple_type(
        self, async_connection: turu.snowflake.AsyncConnection
    ):
        class Row(NamedTuple):
            pass

        _cursor: turu.snowflake.AsyncCursor[
            Row, Never, Never
        ] = await async_connection.execute_map(Row, "select 1")

    @pytest.mark.asyncio
    async def test_execute_map_dataclass_type(
        self, async_connection: turu.snowflake.AsyncConnection
    ):
        from dataclasses import dataclass

        @dataclass
        class Row(NamedTuple):
            pass

        _cursor: turu.snowflake.AsyncCursor[
            Row, Never, Never
        ] = await async_connection.execute_map(Row, "select 1")

    @pytest.mark.skipif(not USE_PANDAS, reason="pandas is not installed")
    @pytest.mark.asyncio
    async def test_execute_map_pandas_type(
        self, async_connection: turu.snowflake.AsyncConnection
    ):
        import pandas as pd  # type: ignore[import]

        _cursor: turu.snowflake.AsyncCursor[
            Never, pd.DataFrame, Never
        ] = await async_connection.execute_map(pd.DataFrame, "select 1")

    @pytest.mark.skipif(not USE_PYARROW, reason="pyarrow is not installed")
    @pytest.mark.asyncio
    async def test_execute_pyarrow_type(
        self, async_connection: turu.snowflake.AsyncConnection
    ):
        import pyarrow as pa  # type: ignore[import]

        _cursor: turu.snowflake.AsyncCursor[
            Never, Never, pa.Table
        ] = await async_connection.execute_map(pa.Table, "select 1")

    @pytest.mark.skipif(not USE_PANDERA, reason="pandera is not installed")
    @pytest.mark.asyncio
    async def test_execute_map_pandera_type(
        self, async_connection: turu.snowflake.AsyncConnection
    ):
        import pandera as pa  # type: ignore[import]
        from turu.snowflake.features import PanderaDataFrame

        class RowModel(pa.DataFrameModel):
            ID: pa.Int8

        _cursor: turu.snowflake.AsyncCursor[
            Never, PanderaDataFrame[RowModel], Never
        ] = await async_connection.execute_map(RowModel, "select 1 as ID")

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
        import pyarrow  # type: ignore[import]

        async with await async_connection.execute_map(
            pyarrow.Table, "select 1 as ID union all select 2 as ID"
        ) as cursor:
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
        import pyarrow  # type: ignore[import]
        from pandas import DataFrame  # type: ignore[import]
        from pandas.testing import assert_frame_equal  # type: ignore[import]

        async with await async_connection.execute_map(
            pyarrow.Table, "select 1 as ID union all select 2 as ID"
        ) as cursor:
            for row in await cursor.fetch_arrow_batches():
                assert_frame_equal(
                    cast(DataFrame, row.to_pandas()),
                    DataFrame({"ID": [1, 2]}, dtype="int8"),
                )

    @pytest.mark.skipif(
        not USE_PANDAS,
        reason="pandas is not installed",
    )
    @pytest.mark.asyncio
    async def test_fetch_pandas_all(
        self, async_connection: turu.snowflake.AsyncConnection
    ):
        from pandas import DataFrame  # type: ignore[import]

        async with await async_connection.execute_map(
            DataFrame, "select 1 as ID union all select 2 ID"
        ) as cursor:
            assert (await cursor.fetch_pandas_all()).to_dict() == {"ID": {0: 1, 1: 2}}

    @pytest.mark.skipif(
        not USE_PANDAS,
        reason="pandas is not installed",
    )
    @pytest.mark.asyncio
    async def test_fetch_pandas_batches(
        self, async_connection: turu.snowflake.AsyncConnection
    ):
        from pandas import DataFrame  # type: ignore[import]
        from pandas.testing import assert_frame_equal  # type: ignore[import]

        async with await async_connection.execute_map(
            DataFrame, "select 1 as ID union all select 2 AS ID"
        ) as cursor:
            for df in await cursor.fetch_pandas_batches():
                assert_frame_equal(df, DataFrame({"ID": [1, 2]}, dtype="int8"))

    @pytest.mark.skipif(not USE_PANDAS, reason="pandas is not installed")
    @pytest.mark.asyncio
    async def test_record_pandas_dataframe(
        self, async_connection: turu.snowflake.AsyncConnection
    ):
        import pandas as pd  # type: ignore[import]
        from pandas.testing import assert_frame_equal  # type: ignore[import]

        with tempfile.NamedTemporaryFile() as file:
            async with record_to_csv(
                file.name,
                await async_connection.execute_map(
                    pd.DataFrame,
                    "select 1 as ID union all select 2 AS ID",
                ),
            ) as cursor:
                expected = pd.DataFrame(
                    {"ID": [1, 2]},
                    dtype="int8",
                )

                assert_frame_equal(await cursor.fetch_pandas_all(), expected)
                for row in expected.values:
                    print(row)

            assert (
                Path(file.name).read_text()
                == dedent(
                    """
                    ID
                    1
                    2
                    """
                ).lstrip()
            )

    @pytest.mark.skipif(
        not (USE_PANDAS and USE_PANDERA), reason="pandas or pandera is not installed"
    )
    @pytest.mark.asyncio
    async def test_fetch_pandas_all_using_pandera_model(
        self, async_connection: turu.snowflake.AsyncConnection
    ):
        import pandera as pa  # type: ignore[import]

        class RowModel(pa.DataFrameModel):
            ID: pa.Int8

        async with await async_connection.execute_map(
            RowModel, "select 1 as ID union all select 2 ID"
        ) as cursor:
            assert (await cursor.fetch_pandas_all()).to_dict() == {"ID": {0: 1, 1: 2}}

    @pytest.mark.skipif(
        not (USE_PANDAS and USE_PANDERA), reason="pandas or pandera is not installed"
    )
    @pytest.mark.asyncio
    async def test_fetch_pandas_all_using_pandera_model_raise_validation_error(
        self, async_connection: turu.snowflake.AsyncConnection
    ):
        import pandera as pa  # type: ignore[import]
        import pandera.errors  # type: ignore[import]

        class RowModel(pa.DataFrameModel):
            uuid: Annotated[pa.Int64, pa.Field(le=5)]

        async with await async_connection.execute_map(
            RowModel, "select 1 as ID union all select 2 ID"
        ) as cursor:
            with pytest.raises(pandera.errors.SchemaInitError):
                await cursor.fetch_pandas_all()
