from typing import NamedTuple

import pytest
from turu.mock.extension import TuruMockUnexpectedFetchError
from turu.sqlite3.connection import MockConnection


class TestMock:
    def test_mock_execute(self, mock_connection: MockConnection):
        mock_connection.inject_response(None, [(1,)])

        cursor = mock_connection.cursor().execute("SELECT 1")
        assert list(cursor.fetchall()) == [(1,)]

    def test_mock_execute_without_response_data(self, mock_connection: MockConnection):
        mock_connection.inject_response(None)

        cursor = mock_connection.cursor().execute("SELECT 1")
        with pytest.raises(TuruMockUnexpectedFetchError):
            assert list(cursor.fetchall())

    def test_mock_execute_typing(self, mock_connection: MockConnection):
        class Row(NamedTuple):
            id: int

        expected = [Row(1)]
        mock_connection.inject_response(Row, expected)

        cursor = mock_connection.cursor().execute_typing(Row, "SELECT 1")
        assert list(cursor) == expected

    def test_mock_executemany(self, mock_connection: MockConnection):
        mock_connection.inject_response(None)

        mock_connection.cursor().executemany("SELECT 1", [])
