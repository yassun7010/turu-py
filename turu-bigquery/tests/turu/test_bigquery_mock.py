from dataclasses import dataclass
from typing import NamedTuple

import pytest
from turu.bigquery import MockConnection
from turu.core import tag
from turu.core.mock.exception import (
    TuruMockResponseTypeMismatchError,
    TuruMockUnexpectedFetchError,
)


class Row(NamedTuple):
    id: int


class TestBigqueryMock:
    def test_mock_execute(self, mock_connection: MockConnection):
        mock_connection.inject_response(None, [(1,)])

        cursor = mock_connection.cursor().execute("SELECT 1")
        assert list(cursor.fetchall()) == [(1,)]

    def test_mock_execute_without_response_data(self, mock_connection: MockConnection):
        mock_connection.inject_response(None)

        cursor = mock_connection.cursor().execute("SELECT 1")
        with pytest.raises(TuruMockUnexpectedFetchError):
            assert list(cursor.fetchall())

    def test_mock_execute_map(self, mock_connection: MockConnection):
        expected = [Row(1)]
        mock_connection.inject_response(Row, expected)

        cursor = mock_connection.cursor().execute_map(Row, "SELECT 1")
        assert list(cursor) == expected

    def test_mock_executemany(self, mock_connection: MockConnection):
        mock_connection.inject_response(None)

        mock_connection.cursor().executemany("SELECT 1", [])

    @pytest.mark.parametrize("rowsize", range(5))
    def test_mock_execute_map_fetchone(
        self, mock_connection: MockConnection, rowsize: int
    ):
        expected = [Row(i) for i in range(rowsize)]
        mock_connection.inject_response(Row, expected)

        with mock_connection.execute_map(Row, "SELECT 1") as cursor:
            for i in range(rowsize):
                assert cursor.fetchone() == expected[i]
            assert cursor.fetchone() is None

    @pytest.mark.parametrize("rowsize", range(5))
    def test_mock_execute_map_fetchmany(
        self,
        mock_connection: MockConnection,
        rowsize: int,
    ):
        expected = [Row(i) for i in range(rowsize)]
        mock_connection.inject_response(Row, expected)

        with mock_connection.execute_map(Row, "SELECT 1") as cursor:
            assert cursor.fetchmany(rowsize) == expected
            assert cursor.fetchone() is None

    @pytest.mark.parametrize("rowsize", range(5))
    def test_mock_execute_map_fetchall(
        self, mock_connection: MockConnection, rowsize: int
    ):
        expected = [Row(i) for i in range(rowsize)]
        mock_connection.inject_response(Row, expected)

        with mock_connection.execute_map(Row, "SELECT 1") as cursor:
            assert cursor.fetchall() == expected
            assert cursor.fetchall() == []

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
