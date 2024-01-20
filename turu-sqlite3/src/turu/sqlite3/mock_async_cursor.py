from typing import Any, Iterator

import turu.core.async_cursor
import turu.core.mock

from .async_cursor import AsyncCursor


class MockAsyncCursor(  # type: ignore
    turu.core.mock.MockAsyncCursor[
        turu.core.async_cursor.GenericRowType, Iterator[Any]
    ],
    AsyncCursor[turu.core.async_cursor.GenericRowType],
):
    pass
