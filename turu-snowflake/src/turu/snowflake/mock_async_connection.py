import turu.core.async_connection
import turu.core.cursor
import turu.core.mock
from typing_extensions import Never, override

from .async_connection import AsyncConnection
from .mock_async_cursor import MockAsyncCursor


class MockAsyncConnection(AsyncConnection, turu.core.mock.MockAsyncConnection):
    def __init__(self, *args, **kwargs):
        turu.core.mock.MockAsyncConnection.__init__(self)

    @override
    async def cursor(self) -> "MockAsyncCursor[Never]":
        return MockAsyncCursor(self._turu_mock_store)
