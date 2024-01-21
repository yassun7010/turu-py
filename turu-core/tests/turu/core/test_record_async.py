import tempfile
from pathlib import Path
from textwrap import dedent
from typing import Any, Literal

import pytest
import turu.core.mock
from pydantic import BaseModel
from turu.core.exception import TuruRowTypeNotSupportedError
from turu.core.record import RecordCursor, record_as_csv
from typing_extensions import Never


class RowPydantic(BaseModel):
    id: int
    name: str


class TestRecord:
    @pytest.mark.asyncio
    async def test_record_as_csv_execute_tuple(
        self, mock_async_connection: turu.core.mock.MockAsyncConnection
    ):
        expected = [(i, f"name{i}") for i in range(5)]
        mock_async_connection.inject_response(None, expected)

        with tempfile.NamedTemporaryFile() as file:
            with pytest.raises(TuruRowTypeNotSupportedError):
                async with record_as_csv(
                    file.name,
                    await mock_async_connection.execute(
                        "select 1 as ID, 'taro' as NAME"
                    ),
                ) as cursor:
                    assert await cursor.fetchall() == expected

            assert Path(file.name).read_text() == ""

    @pytest.mark.asyncio
    async def test_record_as_csv_execute_tuple_without_header(
        self, mock_async_connection: turu.core.mock.MockAsyncConnection
    ):
        expected = [(i, f"name{i}") for i in range(5)]
        mock_async_connection.inject_response(None, expected)

        with tempfile.NamedTemporaryFile() as file:
            async with record_as_csv(
                file.name,
                await mock_async_connection.execute("select 1, 'taro"),
                header=False,
            ) as cursor:
                assert await cursor.fetchall() == expected

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

    @pytest.mark.asyncio
    async def test_record_as_csv_execute_map(
        self, mock_async_connection: turu.core.mock.MockAsyncConnection
    ):
        expected = [RowPydantic(id=i, name=f"name{i}") for i in range(5)]
        mock_async_connection.inject_response(RowPydantic, expected)

        with tempfile.NamedTemporaryFile() as file:
            async with record_as_csv(
                file.name,
                await mock_async_connection.execute_map(RowPydantic, "select 1, 'name"),
            ) as cursor:
                assert await cursor.fetchall() == expected

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

    @pytest.mark.asyncio
    async def test_record_as_csv_execute_map_without_header_options(
        self, mock_async_connection: turu.core.mock.MockAsyncConnection
    ):
        expected = [RowPydantic(id=i, name=f"name{i}") for i in range(5)]
        mock_async_connection.inject_response(RowPydantic, expected)

        with tempfile.NamedTemporaryFile() as file:
            async with record_as_csv(
                file.name,
                await mock_async_connection.execute_map(RowPydantic, "select 1, 'name"),
                header=False,
            ) as cursor:
                assert await cursor.fetchall() == expected

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

    @pytest.mark.asyncio
    async def test_record_as_csv_execute_map_with_limit_options(
        self, mock_async_connection: turu.core.mock.MockAsyncConnection
    ):
        expected = [RowPydantic(id=i, name=f"name{i}") for i in range(5)]
        mock_async_connection.inject_response(RowPydantic, expected)

        with tempfile.NamedTemporaryFile() as file:
            async with record_as_csv(
                file.name,
                await mock_async_connection.execute_map(RowPydantic, "select 1, 'name"),
                limit=3,
            ) as cursor:
                assert await cursor.fetchall() == expected

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
    @pytest.mark.asyncio
    async def test_record_as_csv_execute_map_with_enable_options(
        self,
        mock_async_connection: turu.core.mock.MockAsyncConnection,
        enable: Literal["true", True],
    ):
        expected = [RowPydantic(id=i, name=f"name{i}") for i in range(5)]

        with tempfile.NamedTemporaryFile() as file:
            async with record_as_csv(
                file.name,
                (
                    await mock_async_connection.chain()
                    .inject_response(RowPydantic, expected)
                    .execute_map(RowPydantic, "select 1, 'name")
                ),
                enable=enable,
            ) as cursor:
                assert await cursor.fetchall() == expected

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
    @pytest.mark.asyncio
    async def test_record_as_csv_execute_map_with_disable_options(
        self,
        mock_async_connection: turu.core.mock.MockAsyncConnection,
        enable: Literal["false", False, None],
    ):
        expected = [RowPydantic(id=i, name=f"name{i}") for i in range(5)]
        mock_async_connection.inject_response(RowPydantic, expected)

        with tempfile.NamedTemporaryFile() as file:
            async with record_as_csv(
                file.name,
                await mock_async_connection.execute_map(RowPydantic, "select 1, 'name"),
                enable=enable,
            ) as cursor:
                assert not isinstance(cursor, RecordCursor)

            assert Path(file.name).read_text() == ""

    @pytest.mark.asyncio
    async def test_record_as_csv_use_custom_method(self):
        class CustomCursor(turu.core.mock.MockAsyncCursor[Never, Any]):
            def custom_method(self, value: int) -> None:
                pass

        class CustomConnection(turu.core.mock.MockAsyncConnection):
            async def cursor(self) -> CustomCursor:
                return CustomCursor(self._turu_mock_store)

            def custom_method(self, value: int) -> None:
                pass

        with tempfile.NamedTemporaryFile() as file:
            async with record_as_csv(
                file.name,
                await CustomConnection().cursor(),
            ) as cursor:
                assert cursor.custom_method(1) is None

            assert Path(file.name).read_text() == ""
