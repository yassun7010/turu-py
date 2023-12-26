from typing import Optional

import google.api_core.client_info
import google.api_core.client_options
import google.auth.credentials
import google.cloud.bigquery
import google.cloud.bigquery.dbapi
import google.cloud.bigquery.job
import turu.bigquery.cursor
import turu.core.connection
import turu.core.mock
from typing_extensions import Never, deprecated

from .cursor import Cursor, MockCursor


class Connection(turu.core.connection.Connection):
    def __init__(self, connection: google.cloud.bigquery.dbapi.Connection):
        self._raw_connection = connection

    def close(self) -> None:
        """Close the connection and any cursors created from it."""

        self._raw_connection.close()

    def commit(self) -> None:
        """No-op, but for consistency raise an error if connection is closed."""

        self._raw_connection.commit()

    @deprecated("rollback is not supported in BigQuery")
    def rollback(self) -> None:
        raise NotImplementedError()

    def cursor(self) -> Cursor[Never]:
        """Return a new cursor object."""

        return Cursor(self._raw_connection.cursor())


class MockConnection(Connection, turu.core.mock.MockConnection):
    def __init__(self, *args, **kwargs):
        turu.core.mock.MockConnection.__init__(self)

    def cursor(self) -> "MockCursor[Never]":
        return MockCursor(self._turu_mock_store)


try:
    from google.cloud.bigquery_storage import BigQueryReadClient  # type: ignore


except ImportError:

    class BigQueryReadClient:
        pass


def connect(
    client: Optional[google.cloud.bigquery.Client] = None,
    bqstorage_client: Optional[BigQueryReadClient] = None,
) -> Connection:
    import google.cloud.bigquery
    import google.cloud.bigquery.dbapi

    return Connection(
        google.cloud.bigquery.dbapi.connect(
            client=client,
            bqstorage_client=bqstorage_client,
        ),
    )
