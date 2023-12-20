import pytest
import turu.core.mock
from pydantic import BaseModel
from turu.core.exception import TuruRowTypeNotSupportedError
from turu.core.record import record_as_csv

from tests.data.record import TEST_RECORD_DIR


class RowPydantic(BaseModel):
    id: int
    name: str


class TestRecord:
    def test_record_as_csv_execute_tuple(
        self, mock_connection: turu.core.mock.MockConnection
    ):
        expected = [(i, f"name{i}") for i in range(5)]
        mock_connection.inject_response(None, expected)

        with pytest.raises(TuruRowTypeNotSupportedError):
            with record_as_csv(
                TEST_RECORD_DIR / "test_record_as_csv_execute_tuple.csv",
                mock_connection.execute("select 1, 'taro"),
            ) as cursor:
                assert cursor.fetchall() == expected

    def test_record_as_csv_execute_tuple_without_header(
        self, mock_connection: turu.core.mock.MockConnection
    ):
        expected = [(i, f"name{i}") for i in range(5)]
        mock_connection.inject_response(None, expected)

        with record_as_csv(
            TEST_RECORD_DIR / "test_record_as_csv_execute_tuple_without_header.csv",
            mock_connection.execute("select 1, 'taro"),
            header=False,
        ) as cursor:
            assert cursor.fetchall() == expected

    def test_record_as_csv_execute_map(
        self, mock_connection: turu.core.mock.MockConnection
    ):
        expected = [RowPydantic(id=i, name=f"name{i}") for i in range(5)]
        mock_connection.inject_response(RowPydantic, expected)

        with record_as_csv(
            TEST_RECORD_DIR / "test_record_as_csv_execute_map.csv",
            mock_connection.execute_map(RowPydantic, "select 1, 'name"),
        ) as cursor:
            assert cursor.fetchall() == expected

    def test_record_as_csv_execute_map_without_header_options(
        self, mock_connection: turu.core.mock.MockConnection
    ):
        expected = [RowPydantic(id=i, name=f"name{i}") for i in range(5)]
        mock_connection.inject_response(RowPydantic, expected)

        with record_as_csv(
            TEST_RECORD_DIR
            / "test_record_as_csv_execute_map_without_header_options.csv",
            mock_connection.execute_map(RowPydantic, "select 1, 'name"),
            header=False,
        ) as cursor:
            assert cursor.fetchall() == expected

    def test_record_as_csv_execute_map_with_limit_options(
        self, mock_connection: turu.core.mock.MockConnection
    ):
        expected = [RowPydantic(id=i, name=f"name{i}") for i in range(5)]
        mock_connection.inject_response(RowPydantic, expected)

        with record_as_csv(
            TEST_RECORD_DIR / "test_record_as_csv_execute_map_with_limit_options.csv",
            mock_connection.execute_map(RowPydantic, "select 1, 'name"),
            limit=3,
        ) as cursor:
            assert cursor.fetchall() == expected

    def test_record_as_csv_execute_map_with_disable_options(
        self, mock_connection: turu.core.mock.MockConnection
    ):
        expected = [RowPydantic(id=i, name=f"name{i}") for i in range(5)]
        mock_connection.inject_response(RowPydantic, expected)

        with record_as_csv(
            TEST_RECORD_DIR / "test_record_as_csv_execute_map_with_disable_options.csv",
            mock_connection.execute_map(RowPydantic, "select 1, 'name"),
            disable=True,
        ) as cursor:
            assert cursor.fetchall() == expected
