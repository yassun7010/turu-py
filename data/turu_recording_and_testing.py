import os

import turu.sqlite3
from pydantic import BaseModel
from turu.core.record import record_to_csv


class Row(BaseModel):
    id: int
    name: str


# Production code
def do_something(connection: turu.sqlite3.Connection):
    with record_to_csv(
        "test.csv",
        connection.cursor(),
        enable=os.environ.get("ENABLE_RECORDING"),
        limit=100,
    ) as cursor:
        cursor.execute_map(Row, "select 1, 'a'")


# Test code
def test_do_something(connection: turu.sqlite3.MockConnection):
    connection.inject_response_from_csv(Row, "test.csv")

    assert do_something(connection) is None
