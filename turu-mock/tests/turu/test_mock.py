import pytest
import turu.mock
from pydantic import BaseModel
from turu.mock.exception import TuruMockUnexpectedFetchError


def test_version():
    assert turu.mock.__version__


class Row(BaseModel):
    id: int


class TestTuruMock:
    def test_execute(self, mock_connection: turu.mock.MockConnection):
        mock_connection.inject_response(None, [(1,)])
        cursor = mock_connection.cursor().execute("select 1")
        assert cursor.fetchone() == (1,)
        assert cursor.fetchone() is None

    def test_execute_map_fetchone(self, mock_connection: turu.mock.MockConnection):
        expected = [Row(id=1), Row(id=2)]

        mock_connection.inject_response(Row, expected)
        cursor = mock_connection.cursor().execute_map(
            Row, "select 1 union all select 2"
        )

        assert cursor.fetchall() == expected

    def test_execute_map_fetchmany(self, mock_connection: turu.mock.MockConnection):
        expected = [Row(id=1), Row(id=2)]

        mock_connection.inject_response(Row, expected)
        cursor = mock_connection.cursor().execute_map(
            Row, "select 1 union all select 2"
        )

        assert cursor.fetchmany() == [Row(id=1)]
        assert cursor.fetchmany() == [Row(id=2)]

    def test_execute_map_fetchall(self, mock_connection: turu.mock.MockConnection):
        expected = [Row(id=1), Row(id=2)]

        mock_connection.inject_response(Row, expected)
        cursor = mock_connection.cursor().execute_map(
            Row, "select 1 union all select 2"
        )

        assert cursor.fetchall() == expected
        assert cursor.fetchall() == []

    def test_execute_map_fetchall_with_none(
        self, mock_connection: turu.mock.MockConnection
    ):
        mock_connection.inject_response(None)
        cursor = mock_connection.cursor().execute("select 1")
        with pytest.raises(TuruMockUnexpectedFetchError):
            cursor.fetchall()
