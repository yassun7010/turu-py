from typing import Dict, Optional, Union

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

    def cursor(self) -> turu.bigquery.cursor.Cursor[Never]:
        return turu.bigquery.cursor.Cursor(self._raw_connection.cursor())


class MockConnection(Connection, turu.core.mock.MockConnection):
    def __init__(self, **kwargs):
        turu.core.mock.MockConnection.__init__(self)

    def cursor(self) -> "turu.bigquery.cursor.MockCursor[Never]":
        return turu.bigquery.cursor.MockCursor(self._turu_mock_store)


def connect(
    project: Optional[str] = None,
    credentials: Optional[google.auth.credentials.Credentials] = None,
    location: Optional[str] = None,
    default_query_job_config: Optional[google.cloud.bigquery.job.QueryJobConfig] = None,
    default_load_job_config: Optional[google.cloud.bigquery.job.LoadJobConfig] = None,
    client_info: Optional[google.api_core.client_info.ClientInfo] = None,
    client_options: Optional[
        Union[google.api_core.client_options.ClientOptions, Dict]
    ] = None,
) -> Connection:
    return Connection(
        google.cloud.bigquery.Client(
            project=project,
            credentials=credentials,
            location=location,
            default_query_job_config=default_query_job_config,
            default_load_job_config=default_load_job_config,
            client_info=client_info,
            client_options=client_options,
        )
    )
