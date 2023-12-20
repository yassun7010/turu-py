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
class TestTuruSnowflakeConnection:
    def test_execute(self, connection: turu.snowflake.Connection):
        connection.cursor().execute("select 1")

    def test_execute_map_fetchone(self, connection: turu.snowflake.Connection):
        cursor = connection.cursor().execute_map(Row, "select 1")

        assert cursor.fetchone() == Row(1)

    def test_execute_map_fetchmany(self, connection: turu.snowflake.Connection):
        cursor = connection.cursor().execute_map(Row, "select 1 union all select 2")

        assert cursor.fetchmany() == [Row(1)]
        assert cursor.fetchone() == Row(2)
        assert cursor.fetchone() is None

    def test_execute_map_fetchmany_with_size(
        self, connection: turu.snowflake.Connection
    ):
        cursor = connection.cursor().execute_map(
            Row, "select 1 union all select 2 union all select 3"
        )

        assert cursor.fetchmany(2) == [Row(1), Row(2)]
        assert cursor.fetchmany(2) == [Row(3)]

    def test_execute_map_fetchall(self, connection: turu.snowflake.Connection):
        cursor = connection.cursor().execute_map(Row, "select 1 union all select 2")

        assert cursor.fetchall() == [Row(1), Row(2)]
        assert cursor.fetchone() is None

    def test_executemany(self, connection: turu.snowflake.Connection):
        cursor = connection.cursor().executemany(
            "select 1 union all select 2", [(), ()]
        )

        assert cursor.fetchall() == [(1,), (2,)]
        assert next(cursor) is None

    def test_executemany_map(self, connection: turu.snowflake.Connection):
        cursor = connection.cursor().executemany_map(Row, "select 1", [(), ()])

        assert cursor.fetchone() == Row(1)
        assert cursor.fetchone() is None
