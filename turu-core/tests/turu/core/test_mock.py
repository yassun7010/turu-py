from dataclasses import dataclass
from itertools import islice
from typing import Any, NamedTuple

import pytest
import turu.core.mock
from pydantic import BaseModel
from turu.core.mock.exception import (
    TuruMockStoreDataNotFoundError,
    TuruMockUnexpectedFetchError,
)

from tests.data import TEST_DATA_DIR


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

    @pytest.mark.parametrize("execition_time", range(3, 10))
    def test_execute_map_multi_call(
        self, execition_time: int, mock_connection: turu.core.mock.MockConnection
    ):
        def batched(iterable, n: int):
            if n < 1:
                raise ValueError("n must be at least one")
            it = iter(iterable)
            while batch := list(islice(it, n)):
                yield batch

        expected_array = list(
            batched(
                [RowPydantic(id=i) for i in range(3 * execition_time)],
                execition_time,
            )
        )

        for expected in expected_array:
            mock_connection.inject_response(RowPydantic, expected)

        cursor = mock_connection.cursor()
        for expected in expected_array:
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

    def test_executemany(self, mock_connection: turu.core.mock.MockConnection):
        mock_connection.inject_response(None, [(1,), (1,)])
        with mock_connection.executemany("SELECT 1", [(), ()]) as cursor:
            assert cursor.fetchall() == [(1,), (1,)]

    def test_executemany_map(self, mock_connection: turu.core.mock.MockConnection):
        expected = [RowPydantic(id=i) for i in range(3)]
        mock_connection.inject_response(RowPydantic, expected)

        with mock_connection.executemany_map(
            RowPydantic, "SELECT 1", [(), ()]
        ) as cursor:
            assert cursor.fetchall() == expected
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

    def test_inject_response_from_csv(
        self, mock_connection: turu.core.mock.MockConnection
    ):
        class Row(BaseModel):
            id: int
            name: str

        mock_connection.inject_response_from_csv(
            Row, TEST_DATA_DIR / "inject_response_from_csv.csv"
        )

        cursor = mock_connection.cursor().execute_map(Row, "SELECT 1")

        assert cursor.fetchall() == [
            Row(id=0, name="name0"),
            Row(id=1, name="name1"),
            Row(id=2, name="name2"),
        ]

    def test_with_statement(self, mock_connection: turu.core.mock.MockConnection):
        expected = [RowPydantic(id=i) for i in range(3)]

        mock_connection.inject_response(RowPydantic, expected)

        with mock_connection.execute_map(RowPydantic, "SELECT 1") as cursor:
            assert cursor.fetchall() == expected
            assert cursor.fetchone() is None

    def test_inject_execption(self, mock_connection: turu.core.mock.MockConnection):
        mock_connection.inject_response(RowPydantic, ValueError("test"))

        with pytest.raises(ValueError):
            mock_connection.execute_map(RowPydantic, "SELECT 1")
