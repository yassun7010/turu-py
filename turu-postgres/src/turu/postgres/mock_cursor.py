import turu.core.cursor
import turu.core.mock

from .cursor import Cursor, Parameters


class MockCursor(  # type: ignore
    turu.core.mock.MockCursor[turu.core.cursor.GenericRowType, Parameters],
    Cursor[turu.core.cursor.GenericRowType],
):
    pass
