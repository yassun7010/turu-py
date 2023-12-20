import os

import pytest
import turu.bigquery
from pydantic import BaseModel


def test_version():
    assert turu.bigquery.__version__


class PydanticRow(BaseModel):
    id: int


@pytest.mark.skipif(
    condition="USE_REAL_CONNECTION" not in os.environ
    or os.environ["USE_REAL_CONNECTION"].lower() != "true",
    reason="USE_REAL_CONNECTION flag is not set.",
)
class TestBigquery:
    def test_execute_fetchone(self, connection: turu.bigquery.Connection):
        assert connection.execute("select 1").fetchone() == (1,)

    def test_execute_fetchmany(self, connection: turu.bigquery.Connection):
        cursor = connection.execute("select 1 union all select 2")
        assert cursor.fetchmany() == [(1,)]
        assert cursor.fetchmany() == [(2,)]

    def test_execute_fetchmany_with_size(self, connection: turu.bigquery.Connection):
        cursor = connection.execute("select 1 union all select 2 union all select 3")
        assert cursor.fetchmany(2) == [(1,), (2,)]
        assert cursor.fetchmany(2) == [(3,)]

    def test_execute_fetchall(self, connection: turu.bigquery.Connection):
        assert connection.execute("select 1 union all select 2").fetchall() == [
            (1,),
            (2,),
        ]

    def test_execute_map_fetchone(self, connection: turu.bigquery.Connection):
        with connection.execute_map(PydanticRow, "select 1") as cursor:
            assert cursor.fetchone() == PydanticRow(id=1)
            assert cursor.fetchone() is None

    def test_execute_map_fetchmany(self, connection: turu.bigquery.Connection):
        with connection.execute_map(
            PydanticRow, "select 1 union all select 2"
        ) as cursor:
            assert cursor.fetchmany() == [PydanticRow(id=1)]
            assert cursor.fetchone() == PydanticRow(id=2)
            assert cursor.fetchone() is None

    def test_execute_map_fetchall(self, connection: turu.bigquery.Connection):
        with connection.execute_map(
            PydanticRow, "select 1 union all select 2"
        ) as cursor:
            assert cursor.fetchall() == [PydanticRow(id=1), PydanticRow(id=2)]
            assert cursor.fetchone() is None

    def test_executemany_fetchall(self, connection: turu.bigquery.Connection):
        cursor = connection.executemany("select 1 union all select 2", [None])
        assert cursor.fetchall() == [(1,), (2,)]

    def test_executemany_map_fetchall(self, connection: turu.bigquery.Connection):
        with connection.executemany_map(
            PydanticRow, "select 1 union all select 2", [None]
        ) as cursor:
            assert cursor.fetchall() == [PydanticRow(id=1), PydanticRow(id=2)]
            assert cursor.fetchone() is None
