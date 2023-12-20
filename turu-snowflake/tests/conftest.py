import os

import pytest
import turu.snowflake


@pytest.fixture
def connection() -> turu.snowflake.Connection:
    return turu.snowflake.connect(
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema=os.environ["SNOWFLAKE_SCHEMA"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        role=os.environ["SNOWFLAKE_ROLE"],
    )


@pytest.fixture
def mock_connection() -> turu.snowflake.MockConnection:
    return turu.snowflake.MockConnection()
