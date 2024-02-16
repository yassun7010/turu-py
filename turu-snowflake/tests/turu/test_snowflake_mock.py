import tempfile
from pathlib import Path
from textwrap import dedent
from typing import Annotated, NamedTuple

import pytest
import turu.snowflake
from turu.core.record import record_to_csv
from turu.snowflake.features import (
    USE_PANDAS,
    USE_PANDERA,
    USE_PYARROW,
    PandasDataFrame,
    PyArrowTable,
)
from typing_extensions import Never


class Row(NamedTuple):
    id: int


class TestTuruSnowflakeMockConnection:
    def test_execute(self, mock_connection: turu.snowflake.MockConnection):
        mock_connection.inject_response(None, [(1,)])
        cursor = mock_connection.cursor().execute("select 1")
        assert cursor.fetchone() == (1,)
        assert cursor.fetchone() is None

    def test_execute_map_named_tuple_type(
        self, mock_connection: turu.snowflake.MockConnection
    ):
        class Row(NamedTuple):
            id: int

        _cursor: turu.snowflake.Cursor[
            Row, Never, Never
        ] = mock_connection.inject_response(Row, Row(1)).execute_map(Row, "select 1")

    def test_execute_map_dataclass_type(
        self, mock_connection: turu.snowflake.MockConnection
    ):
        from dataclasses import dataclass

        @dataclass
        class Row:
            id: int

        _cursor: turu.snowflake.Cursor[
            Row, Never, Never
        ] = mock_connection.inject_response(Row, Row(id=1)).execute_map(Row, "select 1")

    @pytest.mark.skipif(not USE_PANDAS, reason="pandas is not installed")
    def test_execute_map_pandas_type(
        self, mock_connection: turu.snowflake.MockConnection
    ):
        import pandas as pd  # type: ignore[import]

        _cursor: turu.snowflake.Cursor[
            Never, pd.DataFrame, Never
        ] = mock_connection.inject_response(
            pd.DataFrame, pd.DataFrame({"id": [1]})
        ).execute_map(pd.DataFrame, "select 1")

    @pytest.mark.skipif(
        not (USE_PANDAS and USE_PYARROW), reason="pandas or pyarrow are not installed"
    )
    def test_execute_pyarrow_type(self, mock_connection: turu.snowflake.MockConnection):
        import pandas as pd  # type: ignore[import]
        import pyarrow as pa  # type: ignore[import]

        expected: pa.Table = pa.table(
            pd.DataFrame({"id": [1]}),
        )
        _cursor: turu.snowflake.Cursor[
            Never, Never, pa.Table
        ] = mock_connection.inject_response(
            pa.Table,
            expected,
        ).execute_map(pa.Table, "select 1")

    @pytest.mark.skipif(not USE_PANDERA, reason="pandera is not installed")
    def test_execute_map_pandera_type(
        self, mock_connection: turu.snowflake.MockConnection
    ):
        import pandas as pd  # type: ignore[import]
        import pandera as pa  # type: ignore[import]
        from turu.snowflake.features import PanderaDataFrame

        class RowModel(pa.DataFrameModel):
            ID: pa.Int8

        _cursor: turu.snowflake.Cursor[
            Never, PanderaDataFrame[RowModel], Never
        ] = mock_connection.inject_response(
            RowModel, pd.DataFrame({"id": [1]})
        ).execute_map(RowModel, "select 1 as ID")

    def test_execute_map_fetchone(self, mock_connection: turu.snowflake.MockConnection):
        expected = [Row(1), Row(2)]

        mock_connection.inject_response(Row, expected)
        cursor = mock_connection.cursor().execute_map(
            Row, "select 1 union all select 2"
        )

        assert cursor.fetchall() == expected

    def test_execute_map_fetchmany(
        self, mock_connection: turu.snowflake.MockConnection
    ):
        expected = [Row(1), Row(2)]
        (mock_connection.inject_response(Row, expected).inject_response(Row, expected))

        cursor = mock_connection.cursor().execute_map(
            Row, "select 1 union all select 2"
        )

        assert cursor.fetchmany() == [Row(1)]
        assert cursor.fetchone() == Row(2)
        assert cursor.fetchone() is None

    def test_execute_map_fetchmany_with_size(
        self, mock_connection: turu.snowflake.MockConnection
    ):
        expected = [Row(1), Row(2), Row(3)]
        mock_connection.inject_response(Row, expected)

        cursor = mock_connection.cursor().execute_map(
            Row, "select 1 union all select 2 union all select 3"
        )

        assert cursor.fetchmany(2) == [Row(1), Row(2)]
        assert cursor.fetchone() == Row(3)
        assert cursor.fetchone() is None

    def test_execute_map_fetchall(self, mock_connection: turu.snowflake.MockConnection):
        expected = [Row(1), Row(2)]
        mock_connection.inject_response(Row, expected)

        cursor = mock_connection.cursor().execute_map(
            Row, "select 1 union all select 2"
        )

        assert cursor.fetchall() == [Row(1), Row(2)]
        assert cursor.fetchone() is None

    def test_executemany(self, mock_connection: turu.snowflake.MockConnection):
        expected = [(1,), (2,)]
        mock_connection.inject_response(None, expected)

        cursor = mock_connection.cursor().executemany("select 1 union all select 2", [])

        assert cursor.fetchall() == expected
        assert cursor.fetchone() is None

    def test_executemany_map(self, mock_connection: turu.snowflake.MockConnection):
        expected = [Row(1)]
        mock_connection.inject_response(Row, expected)

        cursor = mock_connection.cursor().executemany_map(Row, "select 1", [])

        assert cursor.fetchone() == expected[0]
        assert cursor.fetchone() is None

    def test_connection_timeout(self, mock_connection: turu.snowflake.MockConnection):
        expected = [Row(1)]
        mock_connection.inject_response(Row, expected)
        with mock_connection.cursor().execute_map(
            Row, "select 1", timeout=10
        ) as cursor:
            assert cursor.fetchmany() == expected

    def test_connection_num_statements(
        self, mock_connection: turu.snowflake.MockConnection
    ):
        expected = [Row(1)]
        mock_connection.inject_response(Row, expected)
        with mock_connection.cursor().execute_map(
            Row, "select 1; select 2;", num_statements=2
        ) as cursor:
            assert cursor.fetchall() == expected
            assert cursor.fetchone() is None

    def test_cursor_timeout(self, mock_connection: turu.snowflake.MockConnection):
        expected = [Row(1)]
        mock_connection.inject_response(Row, expected)
        with mock_connection.cursor().execute_map(
            Row, "select 1", timeout=10
        ) as cursor:
            assert cursor.fetchmany() == expected

    def test_cursor_num_statements(
        self, mock_connection: turu.snowflake.MockConnection
    ):
        expected = [Row(1)]
        mock_connection.inject_response(Row, expected)
        with mock_connection.cursor().execute_map(
            Row, "select 1; select 2;", num_statements=2
        ) as cursor:
            assert cursor.fetchall() == expected
            assert cursor.fetchone() is None

    def test_cursor_use_warehouse(self, mock_connection: turu.snowflake.MockConnection):
        expected = [Row(1)]
        mock_connection.inject_response(Row, expected)
        with mock_connection.cursor().use_warehouse("test_warehouse").execute_map(
            Row,
            "select 1",
        ) as cursor:
            assert cursor.fetchmany() == expected

    def test_cursor_use_database(self, mock_connection: turu.snowflake.MockConnection):
        expected = [Row(1)]
        mock_connection.inject_response(Row, expected)
        with mock_connection.cursor().use_database("test_database").execute_map(
            Row,
            "select 1",
        ) as cursor:
            assert cursor.fetchmany() == expected

    def test_cursor_use_schema(self, mock_connection: turu.snowflake.MockConnection):
        expected = [Row(1)]
        mock_connection.inject_response(Row, expected)
        with mock_connection.cursor().use_schema("test_schema").execute_map(
            Row,
            "select 1",
        ) as cursor:
            assert cursor.fetchmany() == expected

    def test_cursor_use_role(self, mock_connection: turu.snowflake.MockConnection):
        expected = [Row(1)]
        mock_connection.inject_response(Row, expected)
        with mock_connection.cursor().use_role("test_role").execute_map(
            Row,
            "select 1",
        ) as cursor:
            assert cursor.fetchmany() == expected

    @pytest.mark.skipif(
        not (USE_PYARROW and USE_PANDAS),
        reason="pyarrow is not installed",
    )
    def test_fetch_arrow_all(self, mock_connection: turu.snowflake.MockConnection):
        import pandas as pd  # type: ignore[import]
        import pyarrow as pa  # type: ignore[import]

        expected: pa.Table = pa.table(
            pd.DataFrame({"ID": [1, 2]}),
        )  # type: ignore

        mock_connection.inject_response(PyArrowTable, expected)

        with mock_connection.execute_map(
            PyArrowTable, "select 1 as ID union all select 2 as ID"
        ) as cursor:
            assert cursor.fetch_arrow_all() == expected

    @pytest.mark.skipif(
        not (USE_PYARROW and USE_PANDAS),
        reason="pyarrow is not installed",
    )
    def test_fetch_arrow_batches(self, mock_connection: turu.snowflake.MockConnection):
        import pandas as pd  # type: ignore[import]
        import pyarrow as pa  # type: ignore[import]

        expected: pa.Table = pa.table(
            pd.DataFrame({"ID": [1, 2]}),
        )  # type: ignore

        with mock_connection.inject_response(PyArrowTable, expected).execute_map(
            PyArrowTable, "select 1 as ID union all select 2 as ID"
        ) as cursor:
            assert list(cursor.fetch_arrow_batches()) == [expected]

    @pytest.mark.skipif(not USE_PANDAS, reason="pandas is not installed")
    def test_fetch_pandas_all(self, mock_connection: turu.snowflake.MockConnection):
        import pandas as pd  # type: ignore[import]

        expected = pd.DataFrame({"ID": [1, 2]})

        mock_connection.inject_response(PandasDataFrame, expected)

        with mock_connection.execute_map(
            PandasDataFrame, "select 1 as ID union all select 2 as ID"
        ) as cursor:
            assert cursor.fetch_pandas_all().equals(expected)

    @pytest.mark.skipif(not USE_PANDAS, reason="pandas is not installed")
    def test_fetch_pandas_batches(self, mock_connection: turu.snowflake.MockConnection):
        import pandas as pd  # type: ignore[import]

        expected = pd.DataFrame({"ID": [1, 2]})

        with mock_connection.inject_response(PandasDataFrame, expected).execute_map(
            PandasDataFrame, "select 1 as ID union all select 2 as ID"
        ) as cursor:
            assert list(cursor.fetch_pandas_batches()) == [expected]

    @pytest.mark.skipif(
        not (USE_PANDAS and USE_PYARROW), reason="pandas or pyarrow are not installed"
    )
    def test_inject_pyarrow_response_from_csv(
        self, mock_connection: turu.snowflake.MockConnection
    ):
        import pandas as pd  # type: ignore[import]
        import pyarrow as pa  # type: ignore[import]

        expected: pa.Table = pa.table(
            pd.DataFrame({"ID": [1, 2]}),
        )

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

            with (
                mock_connection.chain()
                .inject_response_from_csv(PyArrowTable, file.name)
                .execute_map(PyArrowTable, "select 1 as ID union all select 2 as ID")
            ) as cursor:
                assert cursor.fetch_pandas_all().equals(expected)

    @pytest.mark.skipif(not USE_PANDAS, reason="pandas is not installed")
    def test_inject_pandas_response_from_csv(
        self, mock_connection: turu.snowflake.MockConnection
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

            with (
                mock_connection.chain()
                .inject_response_from_csv(pd.DataFrame, file.name)
                .execute_map(pd.DataFrame, "select 1 as ID union all select 2 as ID")
            ) as cursor:
                assert cursor.fetch_pandas_all().equals(expected)

    @pytest.mark.skipif(
        not (USE_PANDAS and USE_PANDERA), reason="pandas is not installed"
    )
    def test_inject_pandas_response_from_csv_with_pandera_validation(
        self, mock_connection: turu.snowflake.MockConnection
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

            with (
                mock_connection.chain()
                .inject_response_from_csv(RowModel, file.name)
                .execute_map(RowModel, "select 1 as ID union all select 2 as ID")
            ) as cursor:
                assert cursor.fetch_pandas_all().equals(expected)

    @pytest.mark.skipif(
        not (USE_PANDAS and USE_PANDERA), reason="pandas or pandera is not installed"
    )
    def test_fetch_pandas_all_using_pandera_model(
        self, mock_connection: turu.snowflake.MockConnection
    ):
        import pandas as pd  # type: ignore[import]
        import pandera as pa  # type: ignore[import]

        class RowModel(pa.DataFrameModel):
            ID: pa.Int64

        with mock_connection.inject_response(
            RowModel, pd.DataFrame({"ID": [1, 2]})
        ).execute_map(RowModel, "select 1 as ID union all select 2 ID") as cursor:
            assert cursor.fetch_pandas_all().to_dict() == {"ID": {0: 1, 1: 2}}

    @pytest.mark.skipif(
        not (USE_PANDAS and USE_PANDERA), reason="pandas or pandera is not installed"
    )
    def test_fetch_pandas_all_using_pandera_model_raise_validation_error(
        self, mock_connection: turu.snowflake.MockConnection
    ):
        import pandas as pd  # type: ignore[import]
        import pandera as pa  # type: ignore[import]
        import pandera.errors  # type: ignore[import]

        class RowModel(pa.DataFrameModel):
            uuid: Annotated[pa.Int64, pa.Field(le=5)]

        with mock_connection.inject_response(
            RowModel, pd.DataFrame({"ID": [1, 2]})
        ).execute_map(RowModel, "select 1 as ID union all select 2 ID") as cursor:
            with pytest.raises(pandera.errors.SchemaInitError):
                cursor.fetch_pandas_all()

    @pytest.mark.skipif(not USE_PANDAS, reason="pandas is not installed")
    def test_record_to_csv_and_fetch_pandas_all_with_limit_options(
        self, mock_connection: turu.snowflake.MockConnection
    ):
        import pandas as pd  # type: ignore[import]

        with tempfile.NamedTemporaryFile() as file:
            with record_to_csv(
                file.name,
                mock_connection.inject_response(
                    pd.DataFrame, pd.DataFrame({"ID": list(range(10))})
                ).execute_map(pd.DataFrame, "select 1"),
                limit=5,
            ) as cursor:
                assert cursor.fetch_pandas_all().equals(
                    pd.DataFrame({"ID": list(range(5))})
                )

    @pytest.mark.skipif(not USE_PANDAS, reason="pandas is not installed")
    def test_record_to_csv_and_fetch_pandas_batches_with_limit_options(
        self, mock_connection: turu.snowflake.MockConnection
    ):
        import pandas as pd  # type: ignore[import]

        with tempfile.NamedTemporaryFile() as file:
            with record_to_csv(
                file.name,
                mock_connection.inject_response(
                    pd.DataFrame, pd.DataFrame({"ID": list(range(10))})
                ).execute_map(pd.DataFrame, "select 1"),
                limit=5,
            ) as cursor:
                for batch in cursor.fetch_pandas_batches():
                    assert batch.equals(pd.DataFrame({"ID": list(range(5))}))

    @pytest.mark.skipif(
        not (USE_PANDAS and USE_PYARROW), reason="pyarrow is not installed"
    )
    def test_record_to_csv_and_fetch_arrow_all_with_limit_options(
        self, mock_connection: turu.snowflake.MockConnection
    ):
        import pandas as pd  # type: ignore[import]
        import pyarrow as pa  # type: ignore[import]

        with tempfile.NamedTemporaryFile() as file:
            with record_to_csv(
                file.name,
                mock_connection.inject_response(
                    pa.Table,
                    pa.table(pd.DataFrame({"ID": list(range(10))})),
                ).execute_map(pa.Table, "select 1 as ID"),
                limit=5,
            ) as cursor:
                assert cursor.fetch_arrow_all().equals(
                    pa.table(pd.DataFrame({"ID": list(range(5))}))
                )

    @pytest.mark.skipif(
        not (USE_PANDAS and USE_PYARROW), reason="pyarrow is not installed"
    )
    def test_record_to_csv_and_fetch_arrow_batches_with_limit_options(
        self, mock_connection: turu.snowflake.MockConnection
    ):
        import pandas as pd  # type: ignore[import]
        import pyarrow as pa  # type: ignore[import]

        with tempfile.NamedTemporaryFile() as file:
            with record_to_csv(
                file.name,
                mock_connection.inject_response(
                    pa.Table,
                    pa.table(
                        pd.DataFrame({"ID": list(range(10))}),
                    ),
                ).execute_map(pa.Table, "select 1 as ID"),
                limit=5,
            ) as cursor:
                for batch in cursor.fetch_arrow_batches():
                    assert batch.equals(pa.table(pd.DataFrame({"ID": list(range(5))})))
