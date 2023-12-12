import turu.sqlite3


class TestRowType:
    def test_named_type(self):
        from typing import NamedTuple

        class Row(NamedTuple):
            id: int
            name: str

        rows = (
            turu.sqlite3.connect("test.db")
            .cursor()
            .execute_typing(Row, "select 1, 'a'")
        )

        assert next(rows) == Row(1, "a")

    def test_pydantic(self):
        from pydantic import BaseModel

        class Row(BaseModel):
            id: int
            name: str

        rows = (
            turu.sqlite3.connect("test.db")
            .cursor()
            .execute_typing(Row, "select 1, 'a'")
        )

        assert next(rows) == Row(id=1, name="a")
