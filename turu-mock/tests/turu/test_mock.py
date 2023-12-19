from dataclasses import dataclass
from typing import Any, NamedTuple

import pytest
import turu.mock
from pydantic import BaseModel
from turu.mock.exception import TuruMockUnexpectedFetchError


def test_version():
    assert turu.mock.__version__


class RowNamedTuple(NamedTuple):
    id: int


@dataclass
class RowDataclass:
    id: int


class RowPydantic(BaseModel):
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
        expected = [RowPydantic(id=i) for i in range(rowsize)]
        mock_connection.inject_response(RowPydantic, expected)

        cursor = mock_connection.cursor().execute_map(RowPydantic, "SELECT 1")
        for i in range(rowsize):
            assert cursor.fetchone() == expected[i]
        assert cursor.fetchone() is None

    @pytest.mark.parametrize("rowsize", range(5))
    def test_mock_execute_map_fetchmany(
        self,
        mock_connection: turu.mock.MockConnection,
        rowsize: int,
    ):
        expected = [RowPydantic(id=i) for i in range(rowsize)]
        mock_connection.inject_response(RowPydantic, expected)

        cursor = mock_connection.cursor().execute_map(RowPydantic, "SELECT 1")
        assert list(cursor.fetchmany(rowsize)) == expected
        assert cursor.fetchone() is None

    @pytest.mark.parametrize("rowsize", range(5))
    def test_mock_execute_map_fetchall(
        self, mock_connection: turu.mock.MockConnection, rowsize: int
    ):
        expected = [RowPydantic(id=i) for i in range(rowsize)]
        mock_connection.inject_response(RowPydantic, expected)

        cursor = mock_connection.cursor().execute_map(RowPydantic, "SELECT 1")
        assert list(cursor.fetchall()) == expected
        assert cursor.fetchall() == []

    @pytest.mark.parametrize("rowtype", [RowNamedTuple, RowDataclass, RowPydantic])
    def test_execute_map_by_rowtype(
        self, rowtype: Any, mock_connection: turu.mock.MockConnection
    ):
        expected = [rowtype(id=1)]
        mock_connection.inject_response(rowtype, expected)

        cursor = mock_connection.cursor().execute_map(rowtype, "SELECT 1")
        assert list(cursor) == expected

    @pytest.mark.parametrize("execition_time", range(5))
    def test_execute_map_multi_call(
        self, execition_time: int, mock_connection: turu.mock.MockConnection
    ):
        expected = [RowPydantic(id=i) for i in range(3)]
        for _ in range(execition_time):
            mock_connection.inject_response(RowPydantic, expected)

        cursor = mock_connection.cursor()
        for _ in range(execition_time):
            assert cursor.execute_map(RowPydantic, "SELECT 1").fetchall() == expected

        assert cursor.fetchone() is None

    @pytest.mark.parametrize("execition_time", range(5))
    def test_execute_map_each_inject_and_execute(
        self, execition_time: int, mock_connection: turu.mock.MockConnection
    ):
        expected = [RowPydantic(id=i) for i in range(3)]
        for _ in range(execition_time):
            mock_connection.inject_response(RowPydantic, expected)
            cursor = mock_connection.cursor()

            assert cursor.execute_map(RowPydantic, "SELECT 1").fetchall() == expected
            assert cursor.fetchone() is None
