import os

import pytest
import turu.snowflake


@pytest.fixture
def connection() -> turu.snowflake.Connection:
    SNOWFLAKE_USER = os.environ["SNOWFLAKE_USER"]
    SNOWFLAKE_PASSWORD = os.environ["SNOWFLAKE_PASSWORD"]
    SNOWFLAKE_ACCOUNT = os.environ["SNOWFLAKE_ACCOUNT"]
    SNOWFLAKE_DATABASE = os.environ["SNOWFLAKE_DATABASE"]
    SNOWFLAKE_SCHEMA = os.environ["SNOWFLAKE_SCHEMA"]
    SNOWFLAKE_WAREHOUSE = os.environ["SNOWFLAKE_WAREHOUSE"]
    SNOWFLAKE_ROLE = os.environ["SNOWFLAKE_ROLE"]

    return turu.snowflake.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        warehouse=SNOWFLAKE_WAREHOUSE,
        role=SNOWFLAKE_ROLE,
    )


@pytest.fixture
def mock_connection() -> turu.snowflake.MockConnection:
    return turu.snowflake.MockConnection()
