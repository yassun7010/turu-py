import turu.sqlite3
from pydantic import BaseModel


class Row(BaseModel):
    id: int
    name: str


expected1 = [Row(id=1, name="a"), Row(id=2, name="b")]
expected2 = [Row(id=3, name="c"), Row(id=4, name="d")]
expected3 = [Row(id=5, name="e"), Row(id=6, name="f")]

connection = turu.sqlite3.MockConnection()

(
    connection.chain()
    .inject_response(Row, expected1)
    .inject_response(Row, expected2)
    .inject_response(Row, expected3)
)

for expected in [expected1, expected2, expected3]:
    with connection.execute_map(Row, "select 1, 'a'") as cursor:
        assert cursor.fetchall() == expected
