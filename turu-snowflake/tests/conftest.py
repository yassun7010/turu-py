import pytest
import pytest_asyncio
import turu.snowflake


@pytest.fixture
def connection() -> turu.snowflake.Connection:
    return turu.snowflake.connect_from_env()


@pytest_asyncio.fixture
async def async_connection() -> turu.snowflake.AsyncConnection:
    return await turu.snowflake.connect_async_from_env()


@pytest.fixture
def mock_connection() -> turu.snowflake.MockConnection:
    return turu.snowflake.MockConnection()


@pytest.fixture
def mock_async_connection() -> turu.snowflake.MockAsyncConnection:
    return turu.snowflake.MockAsyncConnection()
