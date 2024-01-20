import turu.core.async_cursor
import turu.core.mock

from .async_cursor import AsyncCursor
from .cursor import Parameters


class MockAsyncCursor(  # type: ignore
    turu.core.mock.MockAsyncCursor[turu.core.async_cursor.GenericRowType, Parameters],
    AsyncCursor[turu.core.async_cursor.GenericRowType],
):
    pass
