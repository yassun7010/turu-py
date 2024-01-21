from typing import Any, Literal

import pytest
import turu.core.mock
from pydantic import BaseModel
from turu.core.exception import TuruRowTypeNotSupportedError
from turu.core.record import _RecordCursor, record_as_csv
from typing_extensions import Never

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

        assert (
            TEST_RECORD_DIR / "test_record_as_csv_execute_tuple.csv"
        ).read_text() == ""

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

    @pytest.mark.parametrize("enable", ["true", True])
    def test_record_as_csv_execute_map_with_enable_options(
        self,
        mock_connection: turu.core.mock.MockConnection,
        enable: Literal["true", True],
    ):
        expected = [RowPydantic(id=i, name=f"name{i}") for i in range(5)]
        mock_connection.inject_response(RowPydantic, expected)

        with record_as_csv(
            TEST_RECORD_DIR / "test_record_as_csv_execute_map_with_enable_options.csv",
            mock_connection.execute_map(RowPydantic, "select 1, 'name"),
            enable=enable,
        ) as cursor:
            assert isinstance(cursor, _RecordCursor)
            assert cursor.fetchall() == expected

    @pytest.mark.parametrize("enable", ["false", False, None])
    def test_record_as_csv_execute_map_with_disable_options(
        self,
        mock_connection: turu.core.mock.MockConnection,
        enable: Literal["false", False, None],
    ):
        expected = [RowPydantic(id=i, name=f"name{i}") for i in range(5)]
        mock_connection.inject_response(RowPydantic, expected)

        with record_as_csv(
            TEST_RECORD_DIR / "test_record_as_csv_execute_map_with_disable_options.csv",
            mock_connection.execute_map(RowPydantic, "select 1, 'name"),
            enable=enable,
        ) as cursor:
            assert not isinstance(cursor, _RecordCursor)

    def test_record_as_csv_use_custom_method(self):
        class CustomCursor(turu.core.mock.MockCursor[Never, Any]):
            def custom_method(self, value: int) -> None:
                pass

        class CustomConnection(turu.core.mock.MockConnection):
            def cursor(self) -> CustomCursor:
                return CustomCursor(self._turu_mock_store)

            def custom_method(self, value: int) -> None:
                pass

        with record_as_csv(
            TEST_RECORD_DIR / "test_record_as_csv_use_custom_method.csv",
            CustomConnection().cursor(),
        ) as cursor:
            assert cursor.custom_method(1) is None

        assert (
            TEST_RECORD_DIR.joinpath(
                "test_record_as_csv_use_custom_method.csv"
            ).read_text()
            == ""
        )
