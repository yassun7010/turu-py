from typing import AsyncGenerator

import pytest
import pytest_asyncio
import turu.sqlite3


@pytest.fixture
def connection() -> turu.sqlite3.Connection:
    return turu.sqlite3.connect("test.db")


@pytest_asyncio.fixture
async def async_connection() -> AsyncGenerator[turu.sqlite3.AsyncConnection, None]:
    connection = await turu.sqlite3.connect_async("test.db")

    yield connection

    await connection.close()


@pytest.fixture
def mock_connection() -> turu.sqlite3.MockConnection:
    return turu.sqlite3.MockConnection()


@pytest_asyncio.fixture
async def mock_async_connection() -> turu.sqlite3.MockAsyncConnection:
    return turu.sqlite3.MockAsyncConnection()
