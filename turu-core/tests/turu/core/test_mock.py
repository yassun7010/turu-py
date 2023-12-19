from dataclasses import dataclass
from typing import Any, NamedTuple

import pytest
import turu.core.mock
from pydantic import BaseModel
from turu.core.mock.exception import (
    TuruMockStoreDataNotFoundError,
    TuruMockUnexpectedFetchError,
)


class RowNamedTuple(NamedTuple):
    id: int


@dataclass
class RowDataclass:
    id: int


class RowPydantic(BaseModel):
    id: int


class TestTuruMock:
    def test_execute_fetch(self, mock_connection: turu.core.mock.MockConnection):
        mock_connection.inject_response(None, [(1,)])
        cursor = mock_connection.cursor().execute("select 1")

        assert cursor.fetchall() == [(1,)]

    def test_execute_without_injection(
        self, mock_connection: turu.core.mock.MockConnection
    ):
        with pytest.raises(TuruMockStoreDataNotFoundError):
            mock_connection.cursor().execute("select 1")

    def test_execute_fetch_with_none(
        self, mock_connection: turu.core.mock.MockConnection
    ):
        mock_connection.inject_response(None)
        cursor = mock_connection.cursor().execute("select 1")
        with pytest.raises(TuruMockUnexpectedFetchError):
            cursor.fetchall()

    def test_execute(self, mock_connection: turu.core.mock.MockConnection):
        mock_connection.inject_response(None, [(1,)])
        cursor = mock_connection.cursor().execute("select 1")

        assert cursor.fetchone() == (1,)
        assert cursor.fetchone() is None

    @pytest.mark.parametrize("rowsize", range(5))
    def test_mock_execute_map_fetchone(
        self, mock_connection: turu.core.mock.MockConnection, rowsize: int
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
        mock_connection: turu.core.mock.MockConnection,
        rowsize: int,
    ):
        expected = [RowPydantic(id=i) for i in range(rowsize)]
        mock_connection.inject_response(RowPydantic, expected)

        cursor = mock_connection.cursor().execute_map(RowPydantic, "SELECT 1")

        assert list(cursor.fetchmany(rowsize)) == expected
        assert cursor.fetchone() is None

    @pytest.mark.parametrize("rowsize", range(5))
    def test_mock_execute_map_fetchall(
        self, mock_connection: turu.core.mock.MockConnection, rowsize: int
    ):
        expected = [RowPydantic(id=i) for i in range(rowsize)]
        mock_connection.inject_response(RowPydantic, expected)

        cursor = mock_connection.cursor().execute_map(RowPydantic, "SELECT 1")

        assert list(cursor.fetchall()) == expected
        assert cursor.fetchall() == []

    @pytest.mark.parametrize(
        "GenericRowType", [RowNamedTuple, RowDataclass, RowPydantic]
    )
    def test_execute_map_by_rowtype(
        self, GenericRowType: Any, mock_connection: turu.core.mock.MockConnection
    ):
        expected = [GenericRowType(id=1)]
        mock_connection.inject_response(GenericRowType, expected)

        cursor = mock_connection.cursor().execute_map(GenericRowType, "SELECT 1")

        assert list(cursor) == expected

    @pytest.mark.parametrize("execition_time", range(5))
    def test_execute_map_multi_call(
        self, execition_time: int, mock_connection: turu.core.mock.MockConnection
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
        self, execition_time: int, mock_connection: turu.core.mock.MockConnection
    ):
        expected = [RowPydantic(id=i) for i in range(3)]
        for _ in range(execition_time):
            mock_connection.inject_response(RowPydantic, expected)
            cursor = mock_connection.cursor()

            assert cursor.execute_map(RowPydantic, "SELECT 1").fetchall() == expected
            assert cursor.fetchone() is None

    def test_multi_injection(self, mock_connection: turu.core.mock.MockConnection):
        expected = [RowPydantic(id=i) for i in range(3)]
        (
            mock_connection.chain()
            .inject_response(RowPydantic, expected)
            .inject_response(RowPydantic, expected)
            .inject_response(RowPydantic, expected)
            .inject_response(RowPydantic, expected)
        )

        cursor = mock_connection.cursor()
        for _ in range(4):
            assert cursor.execute_map(RowPydantic, "SELECT 1").fetchall() == expected
            assert cursor.fetchone() is None

    def test_cursor_iterator(self, mock_connection: turu.core.mock.MockConnection):
        expected = [RowPydantic(id=i) for i in range(3)]
        mock_connection.inject_response(RowPydantic, expected)

        for i, row in enumerate(
            mock_connection.cursor().execute_map(RowPydantic, "SELECT 1")
        ):
            assert row == expected[i]
