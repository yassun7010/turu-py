from typing import TYPE_CHECKING

import turu.core.cursor
import turu.core.mock

from .cursor import Cursor

if TYPE_CHECKING:
    from .cursor import _Parameters  # noqa: F401


class MockCursor(  # type: ignore
    turu.core.mock.MockCursor[turu.core.cursor.GenericRowType, "_Parameters"],
    Cursor[turu.core.cursor.GenericRowType],
):
    pass
