import turu.bigquery.cursor
import turu.core.connection
import turu.core.mock
from typing_extensions import Never, override

from .connection import Connection
from .mock_cursor import MockCursor


class MockConnection(turu.core.mock.MockConnection, Connection):
    def __init__(self, *args, **kwargs):
        turu.core.mock.MockConnection.__init__(self)

    @override
    def cursor(self) -> "MockCursor[Never]":
        return MockCursor(self._turu_mock_store)
