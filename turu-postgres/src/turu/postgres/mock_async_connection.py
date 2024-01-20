import turu.core.async_connection
import turu.core.mock
import turu.postgres.cursor
import turu.postgres.mock_async_cursor
from typing_extensions import Never, override

from .async_connection import AsyncConnection


class MockAsyncConnection(turu.core.mock.MockAsyncConnection, AsyncConnection):
    def __init__(self, *args, **kwargs):
        turu.core.mock.MockAsyncConnection.__init__(self)

    @override
    async def cursor(self) -> "turu.postgres.mock_async_cursor.MockAsyncCursor[Never]":
        return turu.postgres.mock_async_cursor.MockAsyncCursor(self._turu_mock_store)
