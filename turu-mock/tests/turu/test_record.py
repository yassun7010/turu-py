import turu.mock
from pydantic import BaseModel
from turu.core.recorders import record_as_csv


class RowPydantic(BaseModel):
    id: int


class TestRecord:
    def test_record_as_csv_execute(self, mock_connection: turu.mock.MockConnection):
        expected = [(i,) for i in range(5)]
        mock_connection.inject_response(None, expected)

        with record_as_csv(
            "test_record_as_csv_execute.csv", mock_connection.cursor()
        ) as cursor:
            cursor = cursor.execute("select 1")

            assert cursor.fetchall() == expected

    def test_record_as_csv_execute_map(self, mock_connection: turu.mock.MockConnection):
        expected = [RowPydantic(id=i) for i in range(5)]
        mock_connection.inject_response(RowPydantic, expected)

        with record_as_csv(
            "test_record_as_csv_execute_map.csv", mock_connection.cursor()
        ) as cursor:
            cursor = cursor.execute_map(RowPydantic, "select 1")

            assert cursor.fetchall() == expected
