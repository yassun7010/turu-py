import tempfile
from pathlib import Path
from textwrap import dedent
from typing import Any, Literal

import pytest
import turu.core.mock
from pydantic import BaseModel
from turu.core.exception import TuruRowTypeNotSupportedError
from turu.core.record import RecordCursor, record_to_csv
from typing_extensions import Never, Self


class RowPydantic(BaseModel):
    id: int
    name: str


class TestRecord:
    def test_record_to_csv_execute_tuple(
        self, mock_connection: turu.core.mock.MockConnection
    ):
        expected = [(i, f"name{i}") for i in range(5)]
        mock_connection.inject_response(None, expected)

        with tempfile.NamedTemporaryFile() as file:
            with pytest.raises(TuruRowTypeNotSupportedError):
                with record_to_csv(
                    file.name,
                    mock_connection.execute("select 1 as ID, 'taro' as NAME"),
                ) as cursor:
                    assert cursor.fetchall() == expected

            assert Path(file.name).read_text() == ""

    def test_record_to_csv_execute_tuple_without_header(
        self, mock_connection: turu.core.mock.MockConnection
    ):
        expected = [(i, f"name{i}") for i in range(5)]
        mock_connection.inject_response(None, expected)

        with tempfile.NamedTemporaryFile() as file:
            with record_to_csv(
                file.name,
                mock_connection.execute("select 1, 'taro"),
                header=False,
            ) as cursor:
                assert cursor.fetchall() == expected

            assert (
                Path(file.name).read_text()
                == dedent(
                    """
                    0,name0
                    1,name1
                    2,name2
                    3,name3
                    4,name4
                    """
                ).lstrip()
            )

    def test_record_to_csv_execute_map(
        self, mock_connection: turu.core.mock.MockConnection
    ):
        expected = [RowPydantic(id=i, name=f"name{i}") for i in range(5)]
        mock_connection.inject_response(RowPydantic, expected)

        with tempfile.NamedTemporaryFile() as file:
            with record_to_csv(
                file.name,
                mock_connection.execute_map(RowPydantic, "select 1, 'name"),
            ) as cursor:
                assert cursor.fetchall() == expected

            assert (
                Path(file.name).read_text()
                == dedent(
                    """
                    id,name
                    0,name0
                    1,name1
                    2,name2
                    3,name3
                    4,name4
                    """
                ).lstrip()
            )

    def test_record_to_csv_execute_map_without_header_options(
        self, mock_connection: turu.core.mock.MockConnection
    ):
        expected = [RowPydantic(id=i, name=f"name{i}") for i in range(5)]
        mock_connection.inject_response(RowPydantic, expected)

        with tempfile.NamedTemporaryFile() as file:
            with record_to_csv(
                file.name,
                mock_connection.execute_map(RowPydantic, "select 1, 'name"),
                header=False,
            ) as cursor:
                assert cursor.fetchall() == expected

            assert (
                Path(file.name).read_text()
                == dedent(
                    """
                    0,name0
                    1,name1
                    2,name2
                    3,name3
                    4,name4
                    """
                ).lstrip()
            )

    def test_record_to_csv_execute_map_with_limit_options(
        self, mock_connection: turu.core.mock.MockConnection
    ):
        expected = [RowPydantic(id=i, name=f"name{i}") for i in range(5)]
        mock_connection.inject_response(RowPydantic, expected)

        with tempfile.NamedTemporaryFile() as file:
            with record_to_csv(
                file.name,
                mock_connection.execute_map(RowPydantic, "select 1, 'name"),
                limit=3,
            ) as cursor:
                assert cursor.fetchall() == expected

            assert (
                Path(file.name).read_text()
                == dedent(
                    """
                    id,name
                    0,name0
                    1,name1
                    2,name2
                    """
                ).lstrip()
            )

    @pytest.mark.parametrize("enable", ["true", True])
    def test_record_to_csv_execute_map_with_enable_options(
        self,
        mock_connection: turu.core.mock.MockConnection,
        enable: Literal["true", True],
    ):
        expected = [RowPydantic(id=i, name=f"name{i}") for i in range(5)]
        mock_connection.inject_response(RowPydantic, expected)

        with tempfile.NamedTemporaryFile() as file:
            with record_to_csv(
                file.name,
                mock_connection.execute_map(RowPydantic, "select 1, 'name"),
                enable=enable,
            ) as cursor:
                assert isinstance(cursor, RecordCursor)
                assert cursor.fetchall() == expected

            assert (
                Path(file.name).read_text()
                == dedent(
                    """
                    id,name
                    0,name0
                    1,name1
                    2,name2
                    3,name3
                    4,name4
                    """
                ).lstrip()
            )

    @pytest.mark.parametrize("enable", ["false", False, None])
    def test_record_to_csv_execute_map_with_disable_options(
        self,
        mock_connection: turu.core.mock.MockConnection,
        enable: Literal["false", False, None],
    ):
        expected = [RowPydantic(id=i, name=f"name{i}") for i in range(5)]
        mock_connection.inject_response(RowPydantic, expected)

        with tempfile.NamedTemporaryFile() as file:
            with record_to_csv(
                file.name,
                mock_connection.execute_map(RowPydantic, "select 1, 'name"),
                enable=enable,
            ) as cursor:
                assert not isinstance(cursor, RecordCursor)

            assert Path(file.name).read_text() == ""

    def test_record_to_csv_use_custom_method(self):
        class CustomCursor(turu.core.mock.MockCursor[Never, Any]):
            def custom_method(self, value: int) -> None:
                pass

        class CustomConnection(turu.core.mock.MockConnection):
            @classmethod
            def connect(cls) -> Self:
                return cls()

            @classmethod
            def connect_from_env(cls) -> Self:
                return cls.connect()

            def cursor(self) -> CustomCursor:
                return CustomCursor(self._turu_mock_store)

            def custom_method(self, value: int) -> None:
                pass

        with tempfile.NamedTemporaryFile() as file:
            with record_to_csv(
                file.name,
                CustomConnection().cursor(),
            ) as cursor:
                assert cursor.custom_method(1) is None

            assert Path(file.name).read_text() == ""
