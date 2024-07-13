from dataclasses import dataclass

import pytest
from pydantic import BaseModel
from turu.core import tag
from turu.core.mock.exception import TuruMockResponseTypeMismatchError
from turu.postgres import MockConnection


class Row(BaseModel):
    id: int


class TestTuruPostgresMock:
    def test_execute(self, mock_connection: MockConnection):
        expected = [(1,)]
        mock_connection.inject_response(None, expected)

        assert mock_connection.execute("select 1").fetchall() == expected

    def test_execute_fetchone(self, mock_connection: MockConnection):
        expected = [(1,)]
        mock_connection.inject_response(None, expected)

        assert mock_connection.execute("select 1").fetchone() == expected[0]

    def test_execute_map_fetchone(self, mock_connection: MockConnection):
        expected = [Row(id=1)]
        mock_connection.inject_response(Row, expected)

        with mock_connection.execute_map(Row, "select 1") as cursor:
            assert cursor.fetchone() == expected[0]

    def test_execute_map_fetchmany(self, mock_connection: MockConnection):
        expected = [Row(id=1), Row(id=2)]
        mock_connection.inject_response(Row, expected)

        with mock_connection.execute_map(Row, "select 1 union all select 2") as cursor:
            assert cursor.fetchmany() == [Row(id=1)]
            assert cursor.fetchone() == Row(id=2)
            assert cursor.fetchone() is None

    def test_execute_map_fetchmany_with_size(self, mock_connection: MockConnection):
        mock_connection.inject_response(Row, [Row(id=1), Row(id=2), Row(id=3)])

        with mock_connection.cursor().execute_map(
            Row, "select 1 union all select 2 union all select 3"
        ) as cursor:
            assert cursor.fetchmany(2) == [Row(id=1), Row(id=2)]
            assert cursor.fetchmany(2) == [Row(id=3)]

    def test_execute_map_fetchall(self, mock_connection: MockConnection):
        expected = [Row(id=1), Row(id=2)]

        mock_connection.inject_response(Row, expected)

        cursor = mock_connection.cursor().execute_map(
            Row, "select 1 union all select 2"
        )

        assert cursor.fetchall() == expected
        assert cursor.fetchone() is None

    def test_executemany(self, mock_connection: MockConnection):
        expected = [(1,)]
        mock_connection.inject_response(None, expected)

        with mock_connection.cursor().executemany("select 1", []) as cursor:
            assert cursor.fetchall() == expected

    def test_executemany_map(self, mock_connection: MockConnection):
        expected = [Row(id=1), Row(id=2)]
        mock_connection.inject_response(Row, expected)

        with mock_connection.cursor().executemany_map(
            Row, "select 1 union all select 2", []
        ) as cursor:
            assert cursor.fetchall() == expected

    def test_execute_iter(self, mock_connection: MockConnection):
        expected = [(1,), (2,)]
        mock_connection.inject_response(None, expected)

        with mock_connection.cursor().execute("select 1 union all select 2") as cursor:
            assert list(cursor) == expected

    def test_execute_map_iter(self, mock_connection: MockConnection):
        expected = [Row(id=1), Row(id=2)]
        mock_connection.inject_response(Row, expected)
        with mock_connection.cursor().execute_map(
            Row, "select 1 union all select 2"
        ) as cursor:
            assert list(cursor) == expected

    def test_mock_connection_close(self, mock_connection: MockConnection):
        mock_connection.close()

    def test_mock_connection_commit(self, mock_connection: MockConnection):
        mock_connection.commit()

    def test_mock_connection_rollback(self, mock_connection: MockConnection):
        mock_connection.rollback()

    def test_cursor_rowcount(self, mock_connection: MockConnection):
        cursor = mock_connection.cursor()
        assert cursor.rowcount == -1

    def test_cursor_arraysize(self, mock_connection: MockConnection):
        cursor = mock_connection.cursor()
        assert cursor.arraysize == 1

    def test_cursor_arraysize_setter(self, mock_connection: MockConnection):
        cursor = mock_connection.cursor()
        cursor.arraysize = 2
        assert cursor.arraysize == 2

    def test_execute_with_tag(self, mock_connection: MockConnection):
        @dataclass
        class Table:
            pass

        mock_connection.inject_operation_with_tag(tag.Insert[Table])

        with mock_connection.cursor() as cursor:
            assert (
                cursor.execute_with_tag(tag.Insert[Table], "INSERT table").fetchone()
                is None
            )

    def test_execute_with_tag_when_other_table(self, mock_connection: MockConnection):
        @dataclass
        class Table:
            pass

        @dataclass
        class OtherTable:
            pass

        mock_connection.inject_operation_with_tag(tag.Insert[Table])

        with pytest.raises(TuruMockResponseTypeMismatchError):
            with mock_connection.cursor() as cursor:
                cursor.execute_with_tag(tag.Insert[OtherTable], "INSERT table")

    def test_execute_with_tag_when_other_operation(
        self, mock_connection: MockConnection
    ):
        @dataclass
        class Table:
            pass

        mock_connection.inject_operation_with_tag(tag.Insert[Table])

        with pytest.raises(TuruMockResponseTypeMismatchError):
            with mock_connection.cursor() as cursor:
                cursor.execute_with_tag(tag.Update[Table], "UPDATE table")

    def test_executemany_with_tag(self, mock_connection: MockConnection):
        @dataclass
        class Table:
            pass

        mock_connection.inject_operation_with_tag(tag.Insert[Table])

        with mock_connection.cursor() as cursor:
            assert (
                cursor.executemany_with_tag(
                    tag.Insert[Table], "INSERT table", []
                ).fetchone()
                is None
            )

    def test_executemany_with_tag_when_other_table(
        self, mock_connection: MockConnection
    ):
        @dataclass
        class Table:
            pass

        @dataclass
        class OtherTable:
            pass

        mock_connection.inject_operation_with_tag(tag.Insert[Table])

        with pytest.raises(TuruMockResponseTypeMismatchError):
            with mock_connection.cursor() as cursor:
                cursor.executemany_with_tag(tag.Insert[OtherTable], "INSERT table", [])

    def test_executemany_with_tag_when_other_operation(
        self, mock_connection: MockConnection
    ):
        @dataclass
        class Table:
            pass

        mock_connection.inject_operation_with_tag(tag.Insert[Table])

        with pytest.raises(TuruMockResponseTypeMismatchError):
            with mock_connection.cursor() as cursor:
                cursor.executemany_with_tag(tag.Update[Table], "UPDATE table", [])
