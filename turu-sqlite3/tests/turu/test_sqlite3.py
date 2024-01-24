import turu.sqlite3
from pydantic import BaseModel
from turu.core.record import record_to_csv

from tests.data.records import TEST_RECORD_DIR


def test_turu_sqlite3_version():
    assert turu.sqlite3.__version__


class Row(BaseModel):
    id: int
    name: str


class TestTuruSqlite3:
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
        cursor = connection.cursor().execute_map(Row, "select 1, 'a'")

        assert next(cursor) == Row(id=1, name="a")

    def test_execute_map_fetchone(self, connection: turu.sqlite3.Connection):
        cursor = connection.cursor().execute_map(Row, "select 1, 'a'")

        assert cursor.fetchone() == Row(id=1, name="a")
        assert cursor.fetchone() is None

    def test_execute_map_fetchmany(self, connection: turu.sqlite3.Connection):
        cursor = connection.cursor().execute_map(
            Row, "select 1, 'a' union all select 2, 'b'"
        )

        assert cursor.fetchmany() == [Row(id=1, name="a")]
        assert cursor.fetchmany() == [Row(id=2, name="b")]
        assert cursor.fetchmany() == []

    def test_execute_map_fetchall(self, connection: turu.sqlite3.Connection):
        cursor = connection.execute_map(Row, "select 1, 'a' union all select 2, 'b'")

        assert cursor.fetchall() == [Row(id=1, name="a"), Row(id=2, name="b")]
        assert cursor.fetchall() == []

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

    def test_recording_and_testing(
        self,
        connection: turu.sqlite3.Connection,
        mock_connection: turu.sqlite3.MockConnection,
    ):
        csv_file = TEST_RECORD_DIR / "test_recording_and_testing.csv"

        def do_something(connection: turu.sqlite3.Connection):
            with record_to_csv(
                csv_file, connection.execute_map(Row, "select 1, 'taro'")
            ) as cursor:
                assert cursor.fetchall() == [Row(id=1, name="taro")]

        # NOTE: production code
        do_something(connection)

        # NOTE: testing code
        mock_connection.inject_response_from_csv(Row, csv_file)
        do_something(mock_connection)
