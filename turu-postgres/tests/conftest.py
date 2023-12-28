import pytest
import pytest_asyncio
import turu.postgres


@pytest.fixture
def connection() -> turu.postgres.Connection:
    return turu.postgres.connect_from_env()


@pytest_asyncio.fixture
async def async_connection() -> turu.postgres.AsyncConnection:
    return await turu.postgres.connect_async_from_env()


@pytest.fixture
def mock_connection() -> turu.postgres.MockConnection:
    return turu.postgres.MockConnection()


@pytest.fixture
def mock_async_connection() -> turu.postgres.MockAsyncConnection:
    return turu.postgres.MockAsyncConnection()
