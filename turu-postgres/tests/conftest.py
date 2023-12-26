import psycopg
import pytest
import turu.postgres


@pytest.fixture
def connection() -> turu.postgres.Connection:
    return turu.postgres.Connection(
        psycopg.connect("postgresql://postgres:postgres@localhost:5432/postgres")
    )
