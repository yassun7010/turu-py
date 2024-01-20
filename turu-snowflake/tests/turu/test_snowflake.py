import os
from typing import NamedTuple

import pytest
import turu.snowflake
from turu.snowflake.features import USE_PANDAS, USE_PYARROW


def test_version():
    assert turu.snowflake.__version__


class Row(NamedTuple):
    id: int


@pytest.mark.skipif(
    condition="USE_REAL_CONNECTION" not in os.environ
    or os.environ["USE_REAL_CONNECTION"].lower() != "true",
    reason="USE_REAL_CONNECTION flag is not set.",
)
class TestTuruSnowflake:
    def test_execute(self, connection: turu.snowflake.Connection):
        assert connection.execute("select 1").fetchall() == [(1,)]

    def test_execute_fetchone(self, connection: turu.snowflake.Connection):
        assert connection.execute("select 1").fetchone() == (1,)

    def test_execute_map_fetchone(self, connection: turu.snowflake.Connection):
        cursor = connection.execute_map(Row, "select 1")

        assert cursor.fetchone() == Row(1)

    def test_execute_map_fetchmany(self, connection: turu.snowflake.Connection):
        cursor = connection.execute_map(Row, "select 1 union all select 2")

        assert cursor.fetchmany() == [Row(1)]
        assert cursor.fetchone() == Row(2)
        assert cursor.fetchone() is None

    def test_execute_map_fetchmany_with_size(
        self, connection: turu.snowflake.Connection
    ):
        cursor = connection.execute_map(
            Row, "select 1 union all select 2 union all select 3"
        )

        assert cursor.fetchmany(2) == [Row(1), Row(2)]
        assert cursor.fetchmany(2) == [Row(3)]

    def test_execute_map_fetchall(self, connection: turu.snowflake.Connection):
        cursor = connection.execute_map(Row, "select 1 union all select 2")

        assert cursor.fetchall() == [Row(1), Row(2)]
        assert cursor.fetchone() is None

    def test_executemany(self, connection: turu.snowflake.Connection):
        cursor = connection.executemany("select 1 union all select 2", [(), ()])

        assert cursor.fetchall() == [(1,), (2,)]
        with pytest.raises(StopIteration):
            next(cursor)

    def test_executemany_map(self, connection: turu.snowflake.Connection):
        with connection.executemany_map(Row, "select 1", [(), ()]) as cursor:
            assert cursor.fetchone() == Row(1)
            assert cursor.fetchone() is None

    def test_execute_iter(self, connection: turu.snowflake.Connection):
        with connection.execute("select 1 union all select 2") as cursor:
            assert list(cursor) == [(1,), (2,)]

    def test_execute_map_iter(self, connection: turu.snowflake.Connection):
        with connection.execute_map(Row, "select 1 union all select 2") as cursor:
            assert list(cursor) == [Row(1), Row(2)]

    def test_connection_close(self, connection: turu.snowflake.Connection):
        connection.close()

    def test_connection_commit(self, connection: turu.snowflake.Connection):
        connection.commit()

    def test_connection_rollback(self, connection: turu.snowflake.Connection):
        connection.rollback()

    def test_cursor_rowcount(self, connection: turu.snowflake.Connection):
        cursor = connection.cursor()
        assert cursor.rowcount == -1

    def test_cursor_arraysize(self, connection: turu.snowflake.Connection):
        cursor = connection.cursor()
        assert cursor.arraysize == 1

    def test_cursor_arraysize_setter(self, connection: turu.snowflake.Connection):
        cursor = connection.cursor()
        cursor.arraysize = 2
        assert cursor.arraysize == 2

    def test_connection_timeout(self, connection: turu.snowflake.Connection):
        with connection.execute_map(Row, "select 1", timeout=10) as cursor:
            assert cursor.fetchone() == Row(1)

    def test_connection_num_statements(self, connection: turu.snowflake.Connection):
        with connection.execute_map(
            Row, "select 1; select 2;", num_statements=2
        ) as cursor:
            assert cursor.fetchall() == [Row(1)]
            assert cursor.fetchone() is None

    def test_cursor_timeout(self, connection: turu.snowflake.Connection):
        with connection.cursor().execute_map(Row, "select 1", timeout=10) as cursor:
            assert cursor.fetchone() == Row(1)

    def test_cursor_num_statements(self, connection: turu.snowflake.Connection):
        with connection.cursor().execute_map(
            Row, "select 1; select 2;", num_statements=2
        ) as cursor:
            assert cursor.fetchall() == [Row(1)]
            assert cursor.fetchone() is None

    def test_cursor_use_warehouse(self, connection: turu.snowflake.Connection):
        with connection.cursor().use_warehouse(
            os.environ["SNOWFLAKE_WAREHOUSE"]
        ).execute_map(Row, "select 1") as cursor:
            assert cursor.fetchone() == Row(1)

    def test_cursor_use_schema(self, connection: turu.snowflake.Connection):
        with connection.cursor().use_schema(os.environ["SNOWFLAKE_SCHEMA"]).execute_map(
            Row, "select 1"
        ) as cursor:
            assert cursor.fetchone() == Row(1)

    def test_cursor_use_database(self, connection: turu.snowflake.Connection):
        with connection.cursor().use_database(
            os.environ["SNOWFLAKE_DATABASE"]
        ).execute_map(Row, "select 1") as cursor:
            assert cursor.fetchone() == Row(1)

    def test_cursor_use_role(self, connection: turu.snowflake.Connection):
        with connection.cursor().use_role(os.environ["SNOWFLAKE_ROLE"]).execute_map(
            Row, "select 1"
        ) as cursor:
            assert cursor.fetchone() == Row(1)

    @pytest.mark.skipif(
        not (USE_PYARROW and USE_PANDAS),
        reason="pyarrow is not installed",
    )
    def test_fetch_arrow_all(self, connection: turu.snowflake.Connection):
        import pyarrow as pa

        expected = pa.table(
            data=[pa.array([1, 2], type=pa.int8())],
            schema=pa.schema([pa.field("ID", pa.int8(), False)]),
        )

        with connection.execute("select 1 as ID union all select 2 as ID") as cursor:
            assert cursor.fetch_arrow_all() == expected

    @pytest.mark.skipif(not USE_PYARROW, reason="pyarrow is not installed")
    def test_fetch_arrow_batches(self, connection: turu.snowflake.Connection):
        from pandas import DataFrame
        from pandas.testing import assert_frame_equal

        with connection.execute("select 1 as ID union all select 2 as ID") as cursor:
            for row in cursor.fetch_arrow_batches():
                assert_frame_equal(
                    row.to_pandas(),
                    DataFrame({"ID": [1, 2]}, dtype="int8"),
                )

    @pytest.mark.skipif(not USE_PANDAS, reason="pandas is not installed")
    def test_fetch_pandas_all(self, connection: turu.snowflake.Connection):
        with connection.execute("select 1 as ID union all select 2 ID") as cursor:
            assert cursor.fetch_pandas_all().to_dict() == {"ID": {0: 1, 1: 2}}

    @pytest.mark.skipif(not USE_PANDAS, reason="pandas is not installed")
    def test_fetch_pandas_batches(self, connection: turu.snowflake.Connection):
        from pandas import DataFrame
        from pandas.testing import assert_frame_equal

        with connection.execute("select 1 as ID union all select 2 AS ID") as cursor:
            for df in cursor.fetch_pandas_batches():
                assert_frame_equal(df, DataFrame({"ID": [1, 2]}, dtype="int8"))
