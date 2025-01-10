import pydantic
import turu.sqlite3
from typing_extensions import Never


class Row(pydantic.BaseModel):
    id: int
    name: str


connection = turu.sqlite3.connect(":memory:")

cursor1: turu.sqlite3.Cursor[Never] = connection.cursor()
cursor2: turu.sqlite3.Cursor[Row] = cursor1.execute_map(
    Row, "SELECT :id, :name", {"id": 1, "name": "taro"}
)
