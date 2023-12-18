from typing import NamedTuple

import turu.sqlite3


class TestSqlite3:
    def test_execute(self):
        cursor = turu.sqlite3.connect("test.db").cursor().execute("select 1")
        assert cursor.fetchall() == [(1,)]

    def test_execute_map(self):
        class Row(NamedTuple):
            id: int
            name: str

        rows = (
            turu.sqlite3.connect("test.db").cursor().execute_map(Row, "select 1, 'a'")
        )

        assert next(rows) == Row(1, "a")

    def test_execute_map_fetchone(self):
        class Row(NamedTuple):
            id: int
            name: str

        rows = (
            turu.sqlite3.connect("test.db").cursor().execute_map(Row, "select 1, 'a'")
        )

        assert rows.fetchone() == Row(1, "a")
        assert rows.fetchone() is None

    def test_execute_map_fetchmany(self):
        class Row(NamedTuple):
            id: int
            name: str

        rows = (
            turu.sqlite3.connect("test.db")
            .cursor()
            .execute_map(Row, "select 1, 'a' union all select 2, 'b'")
        )

        assert rows.fetchmany() == [Row(1, "a")]
        assert rows.fetchmany() == [Row(2, "b")]
        assert rows.fetchmany() == []

    def test_execute_map_fetchall(self):
        class Row(NamedTuple):
            id: int
            name: str

        rows = (
            turu.sqlite3.connect("test.db")
            .cursor()
            .execute_map(Row, "select 1, 'a' union all select 2, 'b'")
        )

        assert rows.fetchall() == [Row(1, "a"), Row(2, "b")]
        assert rows.fetchall() == []
