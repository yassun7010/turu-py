import turu.core.cursor
import turu.core.mock

from .cursor import Cursor, Parameter


class MockCursor(  # type: ignore
    turu.core.mock.MockCursor[turu.core.cursor.GenericRowType, Parameter],
    Cursor[turu.core.cursor.GenericRowType],
):
    pass
