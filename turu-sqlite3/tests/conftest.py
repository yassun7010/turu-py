import pytest
import turu.sqlite3


@pytest.fixture
def connection() -> turu.sqlite3.Connection:
    return turu.sqlite3.connect("test.db")


@pytest.fixture
def mock_connection() -> turu.sqlite3.MockConnection:
    return turu.sqlite3.MockConnection()
