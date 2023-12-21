from typing import NamedTuple

import turu.snowflake


class Row(NamedTuple):
    id: int


class TestTuruSnowflakeConnection:
    def test_execute(self, mock_connection: turu.snowflake.MockConnection):
        mock_connection.inject_response(None, [(1,)])
        cursor = mock_connection.cursor().execute("select 1")
        assert cursor.fetchone() == (1,)
        assert cursor.fetchone() is None

    def test_execute_map_fetchone(self, mock_connection: turu.snowflake.MockConnection):
        expected = [Row(1), Row(2)]

        mock_connection.inject_response(Row, expected)
        cursor = mock_connection.cursor().execute_map(
            Row, "select 1 union all select 2"
        )

        assert cursor.fetchall() == expected

    def test_execute_map_fetchmany(
        self, mock_connection: turu.snowflake.MockConnection
    ):
        expected = [Row(1), Row(2)]
        (mock_connection.inject_response(Row, expected).inject_response(Row, expected))

        cursor = mock_connection.cursor().execute_map(
            Row, "select 1 union all select 2"
        )

        assert cursor.fetchmany() == [Row(1)]
        assert cursor.fetchone() == Row(2)
        assert cursor.fetchone() is None

    def test_execute_map_fetchmany_with_size(
        self, mock_connection: turu.snowflake.MockConnection
    ):
        expected = [Row(1), Row(2), Row(3)]
        mock_connection.inject_response(Row, expected)

        cursor = mock_connection.cursor().execute_map(
            Row, "select 1 union all select 2 union all select 3"
        )

        assert cursor.fetchmany(2) == [Row(1), Row(2)]
        assert cursor.fetchone() == Row(3)
        assert cursor.fetchone() is None

    def test_execute_map_fetchall(self, mock_connection: turu.snowflake.MockConnection):
        expected = [Row(1), Row(2)]
        mock_connection.inject_response(Row, expected)

        cursor = mock_connection.cursor().execute_map(
            Row, "select 1 union all select 2"
        )

        assert cursor.fetchall() == [Row(1), Row(2)]
        assert cursor.fetchone() is None

    def test_executemany(self, mock_connection: turu.snowflake.MockConnection):
        expected = [(1,), (2,)]
        mock_connection.inject_response(None, expected)

        cursor = mock_connection.cursor().executemany("select 1 union all select 2", [])

        assert cursor.fetchall() == expected
        assert cursor.fetchone() is None

    def test_executemany_map(self, mock_connection: turu.snowflake.MockConnection):
        expected = [Row(1)]
        mock_connection.inject_response(Row, expected)

        cursor = mock_connection.cursor().executemany_map(Row, "select 1", [])

        assert cursor.fetchone() == expected[0]
        assert cursor.fetchone() is None

    def test_connection_timeout(self, mock_connection: turu.snowflake.MockConnection):
        expected = [Row(1)]
        mock_connection.inject_response(Row, expected)
        with mock_connection.cursor().execute_map(
            Row, "select 1", timeout=10
        ) as cursor:
            assert cursor.fetchmany() == expected

    def test_connection_num_statements(
        self, mock_connection: turu.snowflake.MockConnection
    ):
        expected = [Row(1)]
        mock_connection.inject_response(Row, expected)
        with mock_connection.cursor().execute_map(
            Row, "select 1; select 2;", num_statements=2
        ) as cursor:
            assert cursor.fetchall() == expected
            assert cursor.fetchone() is None

    def test_cursor_timeout(self, mock_connection: turu.snowflake.MockConnection):
        expected = [Row(1)]
        mock_connection.inject_response(Row, expected)
        with mock_connection.cursor().execute_map(
            Row, "select 1", timeout=10
        ) as cursor:
            assert cursor.fetchmany() == expected

    def test_cursor_num_statements(
        self, mock_connection: turu.snowflake.MockConnection
    ):
        expected = [Row(1)]
        mock_connection.inject_response(Row, expected)
        with mock_connection.cursor().execute_map(
            Row, "select 1; select 2;", num_statements=2
        ) as cursor:
            assert cursor.fetchall() == expected
            assert cursor.fetchone() is None
