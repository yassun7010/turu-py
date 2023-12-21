from typing import NamedTuple

import turu.sqlite3


def test_turu_sqlite3_version():
    assert turu.sqlite3.__version__


class TestSqlite3:
    def test_execute_fetchone(self, connection: turu.sqlite3.Connection):
        cursor = connection.cursor().execute("select 1")
        assert cursor.fetchone() == (1,)
        assert cursor.fetchone() is None

    def test_execute_fetchmany(self, connection: turu.sqlite3.Connection):
        cursor = connection.cursor().execute("select 1 union all select 2")
        assert cursor.fetchmany() == [(1,)]
        assert cursor.fetchmany() == [(2,)]
        assert cursor.fetchmany() == []
        assert cursor.fetchone() is None

    def test_execute_fetchmany_with_size(self, connection: turu.sqlite3.Connection):
        cursor = connection.cursor().execute(
            "select 1 union all select 2 union all select 3"
        )
        assert cursor.fetchmany(2) == [(1,), (2,)]
        assert cursor.fetchmany(2) == [(3,)]
        assert cursor.fetchone() is None

    def test_execute_fetchall(self, connection: turu.sqlite3.Connection):
        cursor = connection.cursor().execute("select 1")
        assert cursor.fetchall() == [(1,)]

    def test_execute_iter(self, connection: turu.sqlite3.Connection):
        cursor = connection.execute("select 1 union all select 2")
        assert list(cursor) == [(1,), (2,)]

    def test_execute_map(self, connection: turu.sqlite3.Connection):
        class Row(NamedTuple):
            id: int
            name: str

        rows = connection.cursor().execute_map(Row, "select 1, 'a'")

        assert next(rows) == Row(1, "a")

    def test_execute_map_fetchone(self, connection: turu.sqlite3.Connection):
        class Row(NamedTuple):
            id: int
            name: str

        rows = connection.cursor().execute_map(Row, "select 1, 'a'")

        assert rows.fetchone() == Row(1, "a")
        assert rows.fetchone() is None

    def test_execute_map_fetchmany(self, connection: turu.sqlite3.Connection):
        class Row(NamedTuple):
            id: int
            name: str

        rows = connection.cursor().execute_map(
            Row, "select 1, 'a' union all select 2, 'b'"
        )

        assert rows.fetchmany() == [Row(1, "a")]
        assert rows.fetchmany() == [Row(2, "b")]
        assert rows.fetchmany() == []

    def test_execute_map_fetchall(self, connection: turu.sqlite3.Connection):
        class Row(NamedTuple):
            id: int
            name: str

        rows = connection.execute_map(Row, "select 1, 'a' union all select 2, 'b'")

        assert rows.fetchall() == [Row(1, "a"), Row(2, "b")]
        assert rows.fetchall() == []

    def test_connection_close(self, connection: turu.sqlite3.Connection):
        connection.close()

    def test_connection_commit(self, connection: turu.sqlite3.Connection):
        connection.commit()

    def test_connection_rollback(self, connection: turu.sqlite3.Connection):
        connection.rollback()

    def test_cursor_rowcount(self, connection: turu.sqlite3.Connection):
        cursor = connection.cursor()
        assert cursor.rowcount == -1

    def test_cursor_arraysize(self, connection: turu.sqlite3.Connection):
        cursor = connection.cursor()
        assert cursor.arraysize == 1

        cursor.arraysize = 2
        assert cursor.arraysize == 2

    def test_cursor_close(self, connection: turu.sqlite3.Connection):
        with connection.cursor() as cursor:
            cursor.close()
