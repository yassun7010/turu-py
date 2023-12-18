import pytest
import turu.mock


class MockConnection(turu.mock.MockConnection):
    def cursor(self) -> turu.mock.MockCursor:
        return turu.mock.MockCursor(self._turu_mock_store)


@pytest.fixture
def mock_connection() -> turu.mock.MockConnection:
    return MockConnection()
