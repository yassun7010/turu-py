from typing import NamedTuple

import typingsql.sqlite3


class TestSqlite3:
    def test_execute(self):
        cursor = typingsql.sqlite3.connect("test.db").cursor().execute("select 1")
        assert cursor.fetchone() == (1,)

    def test_execute_typing(self):
        class Row(NamedTuple):
            id: int
            name: str

        rows = (
            typingsql.sqlite3.connect("test.db")
            .cursor()
            .execute_typing(Row, "select 1, 'a'")
        )

        assert next(rows) == Row(1, "a")
