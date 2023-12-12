from typing import Any

import turu.core.cursor

import snowflake.connector


class Cursor(snowflake.connector.cursor.SnowflakeCursor, turu.core.cursor.Cursor[Any]):  # type: ignore[reportIncompatibleMethodOverride]
    pass
