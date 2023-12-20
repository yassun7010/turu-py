import google.cloud.bigquery
import google.cloud.bigquery.dbapi
import turu.core.connection
from turu.bigquery.cursor import Cursor
from typing_extensions import Never, deprecated


class Connection(turu.core.connection.Connection):
    def __init__(self, client: google.cloud.bigquery.Client):
        self._raw_connection = google.cloud.bigquery.dbapi.connect(client)

    def close(self) -> None:
        self._raw_connection.close()

    def commit(self) -> None:
        self._raw_connection.commit()

    @deprecated("rollback is not supported in BigQuery")
    def rollback(self) -> None:
        raise NotImplementedError()

    def cursor(self) -> Cursor[Never]:
        return Cursor(self._raw_connection.cursor())
