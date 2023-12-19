import turu.mock
from pydantic import BaseModel
from turu.core.recorder import record


class RowPydantic(BaseModel):
    id: int


class TestRecord:
    def test_record(self, mock_connection: turu.mock.MockConnection):
        expected = [RowPydantic(id=i) for i in range(5)]
        mock_connection.inject_response(RowPydantic, expected)
        with record("test.csv", mock_connection.cursor()) as cursor:
            cursor = cursor.execute_map(RowPydantic, "select 1")
            assert cursor.fetchall() == expected
