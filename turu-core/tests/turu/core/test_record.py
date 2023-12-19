import turu.core.mock
from pydantic import BaseModel
from turu.core.record import record_as_csv

from tests.data import TEST_DATA_DIR


class RowPydantic(BaseModel):
    id: int
    name: str


class TestRecord:
    def test_record_as_csv_execute(
        self, mock_connection: turu.core.mock.MockConnection
    ):
        expected = [(i, f"name{i}") for i in range(5)]
        mock_connection.inject_response(None, expected)

        with record_as_csv(
            TEST_DATA_DIR / "test_record_as_csv_execute.csv",
            mock_connection.cursor(),
        ) as cursor:
            cursor = cursor.execute("select 1, 'taro")

            assert cursor.fetchall() == expected

    def test_record_as_csv_execute_map(
        self, mock_connection: turu.core.mock.MockConnection
    ):
        expected = [RowPydantic(id=i, name=f"name{i}") for i in range(5)]
        mock_connection.inject_response(RowPydantic, expected)

        with record_as_csv(
            TEST_DATA_DIR / "test_record_as_csv_execute_map.csv",
            mock_connection.cursor(),
        ) as cursor:
            cursor = cursor.execute_map(RowPydantic, "select 1, 'name")

            assert cursor.fetchall() == expected

    def test_record_as_csv_execute_map_with_header_options(
        self, mock_connection: turu.core.mock.MockConnection
    ):
        expected = [RowPydantic(id=i, name=f"name{i}") for i in range(5)]
        mock_connection.inject_response(RowPydantic, expected)

        with record_as_csv(
            TEST_DATA_DIR / "test_record_as_csv_execute_map_with_header_options.csv",
            mock_connection.cursor(),
            header=True,
        ) as cursor:
            cursor = cursor.execute_map(RowPydantic, "select 1, 'name")

            assert cursor.fetchall() == expected

    def test_record_as_csv_execute_map_with_header_and_rowsize_options(
        self, mock_connection: turu.core.mock.MockConnection
    ):
        expected = [RowPydantic(id=i, name=f"name{i}") for i in range(5)]
        mock_connection.inject_response(RowPydantic, expected)

        with record_as_csv(
            TEST_DATA_DIR
            / "test_record_as_csv_execute_map_with_header_and_rowsize_options.csv",
            mock_connection.cursor(),
            header=True,
            rowsize=3,
        ) as cursor:
            cursor = cursor.execute_map(RowPydantic, "select 1, 'name")

            assert cursor.fetchall() == expected

    def test_record_as_csv_execute_map_with_disable_options(
        self, mock_connection: turu.core.mock.MockConnection
    ):
        expected = [RowPydantic(id=i, name=f"name{i}") for i in range(5)]
        mock_connection.inject_response(RowPydantic, expected)

        with record_as_csv(
            TEST_DATA_DIR / "test_record_as_csv_execute_map_with_enable_options.csv",
            mock_connection.cursor(),
            enable=False,
            header=True,
            rowsize=3,
        ) as cursor:
            cursor = cursor.execute_map(RowPydantic, "select 1, 'name")

            assert cursor.fetchall() == expected
