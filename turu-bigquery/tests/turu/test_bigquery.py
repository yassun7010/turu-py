import os

import pytest
import turu.bigquery


def test_version():
    assert turu.bigquery.__version__


@pytest.mark.skipif(
    condition="REAL_BIGQUERY_TEST" not in os.environ
    or os.environ["REAL_BIGQUERY_TEST"].lower() != "true",
    reason="REAL_BIGQUERY_TEST is not set to true",
)
class TestBigquery:
    def test_execute_fetchone(self, connection: turu.bigquery.Connection):
        assert connection.execute("select 1").fetchone() == (1,)

    def test_execute_fetchmany(self, connection: turu.bigquery.Connection):
        assert connection.execute("select 1 union all select 2").fetchmany() == [
            (1,),
        ]

    def test_execute_fetchall(self, connection: turu.bigquery.Connection):
        assert connection.execute("select 1 union all select 2").fetchall() == [
            (1,),
            (2,),
        ]
