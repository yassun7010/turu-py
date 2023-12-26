import pytest
import turu.postgres


@pytest.fixture
def connection() -> turu.postgres.Connection:
    return turu.postgres.connect(
        "postgresql://postgres:postgres@localhost:5432/postgres"
    )
