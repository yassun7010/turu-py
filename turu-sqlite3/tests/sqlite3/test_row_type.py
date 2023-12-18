import turu.sqlite3


class TestRowType:
    def test_named_type(self, connection: turu.sqlite3.Connection):
        from typing import NamedTuple

        class Row(NamedTuple):
            id: int
            name: str

        rows = connection.cursor().execute_map(Row, "select 1, 'a'")

        assert next(rows) == Row(1, "a")

    def test_dataclass(self, connection: turu.sqlite3.Connection):
        from dataclasses import dataclass

        @dataclass
        class Row:
            id: int
            name: str

        rows = connection.cursor().execute_map(Row, "select 1, 'a'")

        assert next(rows) == Row(1, "a")

    def test_pydantic(self, connection: turu.sqlite3.Connection):
        from pydantic import BaseModel

        class Row(BaseModel):
            id: int
            name: str

        rows = connection.cursor().execute_map(Row, "select 1, 'a'")

        assert next(rows) == Row(id=1, name="a")
