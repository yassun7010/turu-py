from typing import Any

import pytest
import turu.core.mock
from typing_extensions import Never


def pytest_configure(config):
    # for csv_file in TEST_RECORD_DIR.glob("*.csv"):
    #     csv_file.unlink()
    pass


class MockConnection(turu.core.mock.MockConnection):
    def cursor(self) -> turu.core.mock.MockCursor[Never, Any]:
        return turu.core.mock.MockCursor(self._turu_mock_store)


@pytest.fixture
def mock_connection() -> turu.core.mock.MockConnection:
    return MockConnection()


class MockAsyncConnection(turu.core.mock.MockAsyncConnection):
    async def cursor(self) -> turu.core.mock.MockAsyncCursor[Never, Any]:
        return turu.core.mock.MockAsyncCursor(self._turu_mock_store)


@pytest.fixture
def mock_async_connection() -> turu.core.mock.MockAsyncConnection:
    return MockAsyncConnection()
