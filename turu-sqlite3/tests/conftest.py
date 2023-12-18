import pytest
from turu.sqlite3.connection import MockConnection


@pytest.fixture
def mock_connection() -> MockConnection:
    return MockConnection()
