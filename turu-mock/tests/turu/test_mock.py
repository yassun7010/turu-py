from collections import namedtuple
from dataclasses import dataclass

import pytest
import turu.mock
from pydantic import BaseModel
from turu.mock.exception import TuruMockUnexpectedFetchError


def test_version():
    assert turu.mock.__version__


class Row(BaseModel):
    id: int


class TestTuruMock:
    def test_execute_fetch(self, mock_connection: turu.mock.MockConnection):
        mock_connection.inject_response(None, [(1,)])
        cursor = mock_connection.cursor().execute("select 1")
        assert cursor.fetchall() == [(1,)]

    def test_execute_fetch_with_none(self, mock_connection: turu.mock.MockConnection):
        mock_connection.inject_response(None)
        cursor = mock_connection.cursor().execute("select 1")
        with pytest.raises(TuruMockUnexpectedFetchError):
            cursor.fetchall()

    def test_execute(self, mock_connection: turu.mock.MockConnection):
        mock_connection.inject_response(None, [(1,)])
        cursor = mock_connection.cursor().execute("select 1")
        assert cursor.fetchone() == (1,)
        assert cursor.fetchone() is None

    @pytest.mark.parametrize("rowsize", range(5))
    def test_mock_execute_map_fetchone(
        self, mock_connection: turu.mock.MockConnection, rowsize: int
    ):
        expected = [Row(id=i) for i in range(rowsize)]
        mock_connection.inject_response(Row, expected)

        cursor = mock_connection.cursor().execute_map(Row, "SELECT 1")
        for i in range(rowsize):
            assert cursor.fetchone() == expected[i]
        assert cursor.fetchone() is None

    @pytest.mark.parametrize("rowsize", range(5))
    def test_mock_execute_map_fetchmany(
        self,
        mock_connection: turu.mock.MockConnection,
        rowsize: int,
    ):
        expected = [Row(id=i) for i in range(rowsize)]
        mock_connection.inject_response(Row, expected)

        cursor = mock_connection.cursor().execute_map(Row, "SELECT 1")
        assert list(cursor.fetchmany(rowsize)) == expected
        assert cursor.fetchone() is None

    @pytest.mark.parametrize("rowsize", range(5))
    def test_mock_execute_map_fetchall(
        self, mock_connection: turu.mock.MockConnection, rowsize: int
    ):
        expected = [Row(id=i) for i in range(rowsize)]
        mock_connection.inject_response(Row, expected)

        cursor = mock_connection.cursor().execute_map(Row, "SELECT 1")
        assert list(cursor.fetchall()) == expected
        assert cursor.fetchall() == []

    def test_execute_map_by_namedtuple(self, mock_connection: turu.mock.MockConnection):
        Row = namedtuple("Row", ["id"])

        expected = [Row(id=1)]
        mock_connection.inject_response(Row, expected)

        cursor = mock_connection.cursor().execute_map(Row, "SELECT 1")
        assert list(cursor) == expected

    def test_execute_map_by_dataclass(self, mock_connection: turu.mock.MockConnection):
        @dataclass
        class Row:
            id: int

        expected = [Row(id=1)]
        mock_connection.inject_response(Row, expected)

        cursor = mock_connection.cursor().execute_map(Row, "SELECT 1")
        assert list(cursor) == expected

    def test_execute_map_by_pydantic(self, mock_connection: turu.mock.MockConnection):
        class Row(BaseModel):
            id: int

        expected = [Row(id=1)]
        mock_connection.inject_response(Row, expected)

        cursor = mock_connection.cursor().execute_map(Row, "SELECT 1")
        assert list(cursor) == expected