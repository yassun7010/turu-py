import os

import pytest
import turu.postgres
from psycopg import ProgrammingError
from pydantic import BaseModel
from turu.postgres import Connection


class Row(BaseModel):
    id: int


def test_version():
    assert turu.postgres.__version__


@pytest.mark.skipif(
    condition="USE_REAL_CONNECTION" not in os.environ
    or os.environ["USE_REAL_CONNECTION"].lower() != "true",
    reason="USE_REAL_CONNECTION flag is not set.",
)
class TestTuruPostgres:
    def test_execute(self, connection: Connection):
        assert connection.execute("select 1").fetchall() == [(1,)]

    def test_execute_fetchone(self, connection: Connection):
        assert connection.execute("select 1").fetchone() == (1,)

    def test_execute_map_fetchone(self, connection: Connection):
        cursor = connection.cursor().execute_map(Row, "select 1")

        assert cursor.fetchone() == Row(id=1)

    def test_execute_map_fetchmany(self, connection: Connection):
        cursor = connection.cursor().execute_map(Row, "select 1 union all select 2")

        assert cursor.fetchmany() == [Row(id=1)]
        assert cursor.fetchone() == Row(id=2)
        assert cursor.fetchone() is None

    def test_execute_map_fetchmany_with_size(self, connection: Connection):
        cursor = connection.cursor().execute_map(
            Row, "select 1 union all select 2 union all select 3"
        )

        assert cursor.fetchmany(2) == [Row(id=1), Row(id=2)]
        assert cursor.fetchmany(2) == [Row(id=3)]

    def test_execute_map_fetchall(self, connection: Connection):
        cursor = connection.cursor().execute_map(Row, "select 1 union all select 2")

        assert cursor.fetchall() == [Row(id=1), Row(id=2)]
        assert cursor.fetchone() is None

    def test_executemany(self, connection: Connection):
        cursor = connection.cursor().executemany("select 1 union all select 2", [])

        with pytest.raises(ProgrammingError):
            cursor.fetchone()

    def test_executemany_map(self, connection: Connection):
        cursor = connection.cursor().executemany_map(
            Row, "select 1 union all select 2", []
        )

        with pytest.raises(ProgrammingError):
            cursor.fetchone()

    def test_execute_iter(self, connection: Connection):
        cursor = connection.cursor().execute("select 1 union all select 2")

        assert list(cursor) == [(1,), (2,)]

    def test_execute_map_iter(self, connection: Connection):
        cursor = connection.cursor().execute_map(Row, "select 1 union all select 2")

        assert list(cursor) == [Row(id=1), Row(id=2)]

    def test_connection_close(self, connection: Connection):
        connection.close()

    def test_connection_commit(self, connection: Connection):
        connection.commit()

    def test_connection_rollback(self, connection: Connection):
        connection.rollback()

    def test_cursor_rowcount(self, connection: Connection):
        cursor = connection.cursor().execute("select 1 union all select 2")
        assert cursor.rowcount == 2

        cursor = connection.cursor().execute("select 1")
        assert cursor.rowcount == 1

        cursor = connection.cursor().execute(
            "select 1 union all select 2 union all select 3"
        )
        assert cursor.rowcount == 3

    def test_cursor_arraysize(self, connection: Connection):
        cursor = connection.cursor()
        assert cursor.arraysize == 1

    def test_cursor_arraysize_setter(self, connection: Connection):
        cursor = connection.cursor()
        cursor.arraysize = 2
        assert cursor.arraysize == 2
