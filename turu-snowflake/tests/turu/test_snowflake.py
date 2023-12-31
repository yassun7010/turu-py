import os
from typing import NamedTuple

import pytest
import turu.snowflake


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
