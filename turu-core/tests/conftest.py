from typing import Any

import pytest
import turu.core.mock
from typing_extensions import Never


class MockConnection(turu.core.mock.MockConnection):
    def cursor(self) -> turu.core.mock.MockCursor[Never, Any]:
        return turu.core.mock.MockCursor(self._turu_mock_store)


@pytest.fixture
def mock_connection() -> turu.core.mock.MockConnection:
    return MockConnection()
