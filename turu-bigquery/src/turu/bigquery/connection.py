from typing import Any, Optional

import google.api_core.client_info
import google.api_core.client_options
import google.auth.credentials
import google.cloud.bigquery
import google.cloud.bigquery.dbapi
import google.cloud.bigquery.job
import turu.bigquery.cursor
import turu.core.connection
import turu.core.mock
from typing_extensions import Never, Self, deprecated, override

from .cursor import Cursor

try:
    from google.cloud.bigquery_storage import BigQueryReadClient  # type: ignore

except ImportError:

    class BigQueryReadClient:
        pass


class Connection(turu.core.connection.Connection):
    def __init__(self, connection: google.cloud.bigquery.dbapi.Connection):
        self._raw_connection = connection

    @override
    @classmethod
    def connect(  # type: ignore[override]
        cls,
        client: Optional[google.cloud.bigquery.Client] = None,
        bqstorage_client: Optional[BigQueryReadClient] = None,
    ) -> Self:
        import google.cloud.bigquery
        import google.cloud.bigquery.dbapi

        return cls(
            google.cloud.bigquery.dbapi.connect(
                client=client,
                bqstorage_client=bqstorage_client,
            ),
        )

    @classmethod
    @override
    def connect_from_env(cls, *args: Any, **kwargs: Any) -> Self:
        return cls.connect(*args, **kwargs)

    @override
    def close(self) -> None:
        """Close the connection and any cursors created from it."""

        self._raw_connection.close()

    @override
    def commit(self) -> None:
        """No-op, but for consistency raise an error if connection is closed."""

        self._raw_connection.commit()

    @deprecated("rollback is not supported in BigQuery")
    def rollback(self) -> None:
        raise NotImplementedError()

    @override
    def cursor(self) -> Cursor[Never]:
        """Return a new cursor object."""

        return Cursor(self._raw_connection.cursor())
