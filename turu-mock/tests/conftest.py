from typing import Any

import pytest
import turu.mock
from typing_extensions import Never


class MockConnection(turu.mock.MockConnection):
    def cursor(self) -> turu.mock.MockCursor[Never, Any]:
        return turu.mock.MockCursor(self._turu_mock_store)


@pytest.fixture
def mock_connection() -> turu.mock.MockConnection:
    return MockConnection()
