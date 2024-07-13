import tempfile
from dataclasses import dataclass
from pathlib import Path
from textwrap import dedent
from typing import Annotated, NamedTuple

import pytest
import turu.snowflake
from turu.core import tag
from turu.core.mock.exception import TuruMockResponseTypeMismatchError
from turu.core.record import record_to_csv
from turu.snowflake import MockAsyncConnection
from turu.snowflake.features import USE_PANDAS, USE_PANDERA, USE_PYARROW, PyArrowTable
from typing_extensions import Never


class Row(NamedTuple):
    id: int


class TestTuruSnowflakeMockAsyncConnection:
    @pytest.mark.asyncio
    async def test_execute(self, mock_async_connection: MockAsyncConnection):
        mock_async_connection.inject_response(None, [(1,)])
        cursor = await mock_async_connection.execute("select 1")
        assert await cursor.fetchone() == (1,)
        assert await cursor.fetchone() is None

    @pytest.mark.asyncio
    async def test_execute_map_named_tuple_type(
        self, mock_async_connection: MockAsyncConnection
    ):
        class Row(NamedTuple):
            id: int

        _cursor: turu.snowflake.AsyncCursor[
            Row, Never, Never
        ] = await mock_async_connection.inject_response(Row, Row(1)).execute_map(
            Row, "select 1"
        )

    @pytest.mark.asyncio
    async def test_execute_map_dataclass_type(
        self, mock_async_connection: MockAsyncConnection
    ):
        from dataclasses import dataclass

        @dataclass
        class Row:
            id: int

        _cursor: turu.snowflake.AsyncCursor[
            Row, Never, Never
        ] = await mock_async_connection.inject_response(Row, Row(id=1)).execute_map(
            Row, "select 1"
        )

    @pytest.mark.skipif(not USE_PANDAS, reason="pandas is not installed")
    @pytest.mark.asyncio
    async def test_execute_map_pandas_type(
        self, mock_async_connection: MockAsyncConnection
    ):
        import pandas as pd  # type: ignore[import]

        _cursor: turu.snowflake.AsyncCursor[
            Never, pd.DataFrame, Never
        ] = await mock_async_connection.inject_response(
            pd.DataFrame, pd.DataFrame({"id": [1]})
        ).execute_map(pd.DataFrame, "select 1")

    @pytest.mark.skipif(not USE_PYARROW, reason="pyarrow is not installed")
    @pytest.mark.asyncio
    async def test_execute_pyarrow_type(
        self, mock_async_connection: MockAsyncConnection
    ):
        import pyarrow as pa  # type: ignore[import]

        expected: pa.Table = pa.table(
            data=[pa.array([1], type=pa.int64())],
            schema=pa.schema([pa.field("ID", pa.int64())]),
        )  # type: ignore
        _cursor: turu.snowflake.AsyncCursor[
            Never, Never, pa.Table
        ] = await mock_async_connection.inject_response(
            pa.Table,
            expected,
        ).execute_map(pa.Table, "select 1")

    @pytest.mark.skipif(not USE_PANDERA, reason="pandera is not installed")
    @pytest.mark.asyncio
    async def test_execute_map_pandera_type(
        self, mock_async_connection: MockAsyncConnection
    ):
        import pandas as pd  # type: ignore[import]
        import pandera as pa  # type: ignore[import]
        from turu.snowflake.features import PanderaDataFrame

        class RowModel(pa.DataFrameModel):
            ID: pa.Int8

        _cursor: turu.snowflake.AsyncCursor[
            Never, PanderaDataFrame[RowModel], Never
        ] = await mock_async_connection.inject_response(
            RowModel, pd.DataFrame({"id": [1]})
        ).execute_map(RowModel, "select 1 as ID")

    @pytest.mark.asyncio
    async def test_execute_map_fetchone(
        self, mock_async_connection: MockAsyncConnection
    ):
        expected = [Row(1), Row(2)]

        mock_async_connection.inject_response(Row, expected)
        cursor = await mock_async_connection.execute_map(
            Row, "select 1 union all select 2"
        )

        assert await cursor.fetchall() == expected

    @pytest.mark.asyncio
    async def test_execute_map_fetchmany(
        self, mock_async_connection: MockAsyncConnection
    ):
        expected = [Row(1), Row(2)]
        (
            mock_async_connection.chain()
            .inject_response(Row, expected)
            .inject_response(Row, expected)
        )

        cursor = await mock_async_connection.execute_map(
            Row, "select 1 union all select 2"
        )

        assert await cursor.fetchmany() == [Row(1)]
        assert await cursor.fetchone() == Row(2)
        assert await cursor.fetchone() is None

    @pytest.mark.asyncio
    async def test_execute_map_fetchmany_with_size(
        self, mock_async_connection: MockAsyncConnection
    ):
        expected = [Row(1), Row(2), Row(3)]
        mock_async_connection.inject_response(Row, expected)

        cursor = await mock_async_connection.execute_map(
            Row, "select 1 union all select 2 union all select 3"
        )

        assert await cursor.fetchmany(2) == [Row(1), Row(2)]
        assert await cursor.fetchone() == Row(3)
        assert await cursor.fetchone() is None

    @pytest.mark.asyncio
    async def test_execute_map_fetchall(
        self, mock_async_connection: MockAsyncConnection
    ):
        expected = [Row(1), Row(2)]
        mock_async_connection.inject_response(Row, expected)

        cursor = await mock_async_connection.execute_map(
            Row, "select 1 union all select 2"
        )

        assert await cursor.fetchall() == [Row(1), Row(2)]
        assert await cursor.fetchone() is None

    @pytest.mark.asyncio
    async def test_executemany(self, mock_async_connection: MockAsyncConnection):
        expected = [(1,), (2,)]
        mock_async_connection.inject_response(None, expected)

        cursor = await mock_async_connection.executemany(
            "select 1 union all select 2", []
        )

        assert await cursor.fetchall() == expected
        assert await cursor.fetchone() is None

    @pytest.mark.asyncio
    async def test_executemany_map(self, mock_async_connection: MockAsyncConnection):
        expected = [Row(1)]
        mock_async_connection.inject_response(Row, expected)

        cursor = await mock_async_connection.executemany_map(Row, "select 1", [])

        assert await cursor.fetchone() == expected[0]
        assert await cursor.fetchone() is None

    @pytest.mark.asyncio
    async def test_connection_timeout(self, mock_async_connection: MockAsyncConnection):
        expected = [Row(1)]
        mock_async_connection.inject_response(Row, expected)
        async with await mock_async_connection.execute_map(
            Row, "select 1", timeout=10
        ) as cursor:
            assert await cursor.fetchmany() == expected

    @pytest.mark.asyncio
    async def test_connection_num_statements(
        self, mock_async_connection: MockAsyncConnection
    ):
        expected = [Row(1)]
        mock_async_connection.inject_response(Row, expected)
        async with await mock_async_connection.execute_map(
            Row, "select 1; select 2;", num_statements=2
        ) as cursor:
            assert await cursor.fetchall() == expected
            assert await cursor.fetchone() is None

    @pytest.mark.asyncio
    async def test_cursor_timeout(self, mock_async_connection: MockAsyncConnection):
        expected = [Row(1)]
        mock_async_connection.inject_response(Row, expected)
        async with await mock_async_connection.execute_map(
            Row, "select 1", timeout=10
        ) as cursor:
            assert await cursor.fetchmany() == expected

    @pytest.mark.asyncio
    async def test_cursor_num_statements(
        self, mock_async_connection: MockAsyncConnection
    ):
        expected = [Row(1)]
        mock_async_connection.inject_response(Row, expected)
        async with await mock_async_connection.execute_map(
            Row, "select 1; select 2;", num_statements=2
        ) as cursor:
            assert await cursor.fetchall() == expected
            assert await cursor.fetchone() is None

    @pytest.mark.asyncio
    async def test_cursor_use_warehouse(
        self, mock_async_connection: MockAsyncConnection
    ):
        expected = [Row(1)]
        mock_async_connection.inject_response(Row, expected)
        async with await (
            (await mock_async_connection.cursor())
            .use_warehouse("test_warehouse")
            .execute_map(
                Row,
                "select 1",
            )
        ) as cursor:
            assert await cursor.fetchmany() == expected

    @pytest.mark.asyncio
    async def test_cursor_use_database(
        self, mock_async_connection: MockAsyncConnection
    ):
        expected = [Row(1)]
        mock_async_connection.inject_response(Row, expected)
        async with await (
            (await mock_async_connection.cursor())
            .use_database("test_database")
            .execute_map(
                Row,
                "select 1",
            )
        ) as cursor:
            assert await cursor.fetchmany() == expected

    @pytest.mark.asyncio
    async def test_cursor_use_schema(self, mock_async_connection: MockAsyncConnection):
        expected = [Row(1)]
        mock_async_connection.inject_response(Row, expected)
        async with await (
            (await mock_async_connection.cursor())
            .use_schema("test_schema")
            .execute_map(
                Row,
                "select 1",
            )
        ) as cursor:
            assert await cursor.fetchmany() == expected

    @pytest.mark.asyncio
    async def test_cursor_use_role(self, mock_async_connection: MockAsyncConnection):
        expected = [Row(1)]
        mock_async_connection.inject_response(Row, expected)
        async with await (
            (await mock_async_connection.cursor())
            .use_role("test_role")
            .execute_map(
                Row,
                "select 1",
            )
        ) as cursor:
            assert await cursor.fetchmany() == expected

    @pytest.mark.skipif(
        not (USE_PYARROW and USE_PANDAS),
        reason="pyarrow is not installed",
    )
    @pytest.mark.asyncio
    async def test_fetch_arrow_all(self, mock_async_connection: MockAsyncConnection):
        import pyarrow as pa  # type: ignore[import]

        expected: pa.Table = pa.table(
            data=[pa.array([1, 2], type=pa.int8())],
            schema=pa.schema([pa.field("ID", pa.int8(), False)]),
        )  # type: ignore

        mock_async_connection.inject_response(PyArrowTable, expected)

        async with await mock_async_connection.execute_map(
            PyArrowTable, "select 1 as ID union all select 2 as ID"
        ) as cursor:
            assert (await cursor.fetch_arrow_all()) == expected

    @pytest.mark.skipif(
        not (USE_PYARROW and USE_PANDAS),
        reason="pyarrow is not installed",
    )
    @pytest.mark.asyncio
    async def test_fetch_arrow_batches(
        self, mock_async_connection: MockAsyncConnection
    ):
        import pyarrow as pa  # type: ignore[import]

        expected: pa.Table = pa.table(
            data=[pa.array([1, 2], type=pa.int8())],
            schema=pa.schema([pa.field("ID", pa.int8(), False)]),
        )  # type: ignore

        async with await mock_async_connection.inject_response(
            PyArrowTable, expected
        ).execute_map(
            PyArrowTable, "select 1 as ID union all select 2 as ID"
        ) as cursor:
            assert [batch async for batch in cursor.fetch_arrow_batches()] == [expected]

    @pytest.mark.skipif(not USE_PANDAS, reason="pandas is not installed")
    @pytest.mark.asyncio
    async def test_fetch_pandas_all(self, mock_async_connection: MockAsyncConnection):
        import pandas as pd  # type: ignore[import]

        expected = pd.DataFrame({"ID": [1, 2]})

        mock_async_connection.inject_response(pd.DataFrame, expected)

        async with await mock_async_connection.execute_map(
            pd.DataFrame, "select 1 as ID union all select 2 as ID"
        ) as cursor:
            assert (await cursor.fetch_pandas_all()).to_dict() == {"ID": {0: 1, 1: 2}}

    @pytest.mark.skipif(not USE_PANDAS, reason="pandas is not installed")
    @pytest.mark.asyncio
    async def test_fetch_pandas_batches(
        self, mock_async_connection: MockAsyncConnection
    ):
        import pandas as pd  # type: ignore[import]

        expected = pd.DataFrame({"ID": [1, 2]})

        async with await mock_async_connection.inject_response(
            pd.DataFrame, expected
        ).execute_map(
            pd.DataFrame, "select 1 as ID union all select 2 as ID"
        ) as cursor:
            assert [batch async for batch in cursor.fetch_pandas_batches()] == [
                expected
            ]

    @pytest.mark.skipif(
        not (USE_PANDAS and USE_PANDERA), reason="pandas or pandera is not installed"
    )
    @pytest.mark.asyncio
    async def test_fetch_pandas_all_using_pandera_model(
        self, mock_async_connection: MockAsyncConnection
    ):
        import pandas as pd  # type: ignore[import]
        import pandera as pa  # type: ignore[import]

        class RowModel(pa.DataFrameModel):
            ID: pa.Int64

        expected = pd.DataFrame({"ID": [1, 2]})

        async with await mock_async_connection.inject_response(
            RowModel, expected
        ).execute_map(RowModel, "select 1 as ID union all select 2 ID") as cursor:
            assert (await cursor.fetch_pandas_all()).to_dict() == {"ID": {0: 1, 1: 2}}

    @pytest.mark.skipif(
        not (USE_PANDAS and USE_PANDERA), reason="pandas or pandera is not installed"
    )
    @pytest.mark.asyncio
    async def test_fetch_pandas_all_using_pandera_model_raise_validation_error(
        self, mock_async_connection: MockAsyncConnection
    ):
        import pandas as pd  # type: ignore[import]
        import pandera as pa  # type: ignore[import]
        import pandera.errors  # type: ignore[import]

        class RowModel(pa.DataFrameModel):
            uuid: Annotated[pa.Int64, pa.Field(le=5)]

        expected = pd.DataFrame({"ID": [1, 2]})

        async with await mock_async_connection.inject_response(
            RowModel, expected
        ).execute_map(RowModel, "select 1 as ID union all select 2 ID") as cursor:
            with pytest.raises(pandera.errors.SchemaInitError):
                await cursor.fetch_pandas_all()

    @pytest.mark.skipif(not USE_PANDAS, reason="pandas is not installed")
    @pytest.mark.asyncio
    async def test_inject_pyarrow_response_from_csv(
        self, mock_async_connection: MockAsyncConnection
    ):
        import pyarrow as pa  # type: ignore[import]

        expected: pa.Table = pa.table(
            data=[pa.array([1, 2], type=pa.int64())],
            schema=pa.schema([pa.field("ID", pa.int64())]),
        )  # type: ignore

        with tempfile.NamedTemporaryFile() as file:
            Path(file.name).write_text(
                dedent(
                    """
                    ID
                    1
                    2
                    """.lstrip()
                )
            )

            async with await mock_async_connection.inject_response_from_csv(
                PyArrowTable,
                file.name,
            ).execute_map(
                PyArrowTable, "select 1 as ID union all select 2 as ID"
            ) as cursor:
                assert (await cursor.fetch_pandas_all()).equals(expected)

    @pytest.mark.skipif(not USE_PANDAS, reason="pandas is not installed")
    @pytest.mark.asyncio
    async def test_inject_pandas_response_from_csv(
        self, mock_async_connection: MockAsyncConnection
    ):
        import pandas as pd  # type: ignore[import]

        expected = pd.DataFrame({"ID": [1, 2]})

        with tempfile.NamedTemporaryFile() as file:
            Path(file.name).write_text(
                dedent(
                    """
                    ID
                    1
                    2
                    """.lstrip()
                )
            )

            async with (
                await mock_async_connection.chain()
                .inject_response_from_csv(pd.DataFrame, file.name)
                .execute_map(pd.DataFrame, "select 1 as ID union all select 2 as ID")
            ) as cursor:
                assert (await cursor.fetch_pandas_all()).equals(expected)

    @pytest.mark.skipif(
        not (USE_PANDAS and USE_PANDERA), reason="pandas is not installed"
    )
    @pytest.mark.asyncio
    async def test_inject_pandas_response_from_csv_with_pandera_validation(
        self, mock_async_connection: MockAsyncConnection
    ):
        import pandas as pd  # type: ignore[import]
        import pandera as pa  # type: ignore[import]

        class RowModel(pa.DataFrameModel):
            ID: pa.Int64

        expected = pd.DataFrame({"ID": [1, 2]})

        with tempfile.NamedTemporaryFile() as file:
            Path(file.name).write_text(
                dedent(
                    """
                    ID
                    1
                    2
                    """.lstrip()
                )
            )

            async with (
                await mock_async_connection.chain()
                .inject_response_from_csv(RowModel, file.name)
                .execute_map(RowModel, "select 1 as ID union all select 2 as ID")
            ) as cursor:
                assert (await cursor.fetch_pandas_all()).equals(expected)

    @pytest.mark.skipif(not USE_PANDAS, reason="pandas is not installed")
    @pytest.mark.asyncio
    async def test_record_to_csv_and_fetch_pandas_all_with_limit_options(
        self, mock_async_connection: MockAsyncConnection
    ):
        import pandas as pd  # type: ignore[import]

        with tempfile.NamedTemporaryFile() as file:
            async with record_to_csv(
                file.name,
                await mock_async_connection.inject_response(
                    pd.DataFrame, pd.DataFrame({"ID": list(range(10))})
                ).execute_map(pd.DataFrame, "select 1"),
                limit=5,
            ) as cursor:
                assert (await cursor.fetch_pandas_all()).equals(
                    pd.DataFrame({"ID": list(range(5))})
                )

    @pytest.mark.skipif(not USE_PANDAS, reason="pandas is not installed")
    @pytest.mark.asyncio
    async def test_record_to_csv_and_fetch_pandas_batches_with_limit_options(
        self, mock_async_connection: MockAsyncConnection
    ):
        import pandas as pd  # type: ignore[import]

        with tempfile.NamedTemporaryFile() as file:
            async with record_to_csv(
                file.name,
                await mock_async_connection.inject_response(
                    pd.DataFrame, pd.DataFrame({"ID": list(range(10))})
                ).execute_map(pd.DataFrame, "select 1"),
                limit=5,
            ) as cursor:
                async for batch in cursor.fetch_pandas_batches():
                    assert batch.equals(pd.DataFrame({"ID": list(range(5))}))

    @pytest.mark.skipif(
        not (USE_PANDAS and USE_PYARROW), reason="pandas or pyarrow is not installed"
    )
    @pytest.mark.asyncio
    async def test_record_to_csv_and_fetch_arrow_all_with_limit_options(
        self, mock_async_connection: MockAsyncConnection
    ):
        import pandas as pd  # type: ignore[import]
        import pyarrow as pa  # type: ignore[import]

        with tempfile.NamedTemporaryFile() as file:
            async with record_to_csv(
                file.name,
                await mock_async_connection.inject_response(
                    PyArrowTable,
                    pa.table(pd.DataFrame({"ID": list(range(10))})),
                ).execute_map(PyArrowTable, "select 1"),
                limit=5,
            ) as cursor:
                assert (await cursor.fetch_arrow_all()).equals(
                    pa.table(pd.DataFrame({"ID": list(range(5))}))
                )

    @pytest.mark.skipif(
        not (USE_PANDAS and USE_PYARROW), reason="pandas or pyarrow is not installed"
    )
    @pytest.mark.asyncio
    async def test_record_to_csv_and_fetch_arrow_batches_with_limit_options(
        self, mock_async_connection: MockAsyncConnection
    ):
        import pandas as pd  # type: ignore[import]
        import pyarrow as pa  # type: ignore[import]

        with tempfile.NamedTemporaryFile() as file:
            async with record_to_csv(
                file.name,
                await mock_async_connection.inject_response(
                    PyArrowTable,
                    pa.table(pd.DataFrame({"ID": list(range(10))})),
                ).execute_map(PyArrowTable, "select 1"),
                limit=5,
            ) as cursor:
                async for batch in cursor.fetch_arrow_batches():
                    assert batch.equals(pa.table(pd.DataFrame({"ID": list(range(5))})))

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
