import pytest
import turu.bigquery


@pytest.fixture
def connection() -> turu.bigquery.Connection:
    return turu.bigquery.connect()


@pytest.fixture
def mock_connection() -> turu.bigquery.MockConnection:
    return turu.bigquery.MockConnection()
