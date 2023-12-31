from typing import AsyncGenerator

import pytest
import pytest_asyncio
import turu.mysql


@pytest.fixture
def connection() -> turu.mysql.Connection:
    return turu.mysql.connect_from_env()


@pytest_asyncio.fixture
async def async_connection() -> AsyncGenerator[turu.mysql.AsyncConnection, None]:
    conn = await turu.mysql.connect_async_from_env()
    yield conn
    await conn.close()


@pytest.fixture
def mock_connection() -> turu.mysql.MockConnection:
    return turu.mysql.MockConnection()


@pytest.fixture
def mock_async_connection() -> turu.mysql.MockAsyncConnection:
    return turu.mysql.MockAsyncConnection()
