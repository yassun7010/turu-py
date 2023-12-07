import turu.sqlite3


class TestTuruSqlite3:
    def test_turu_sqlite3_version(self):
        assert turu.sqlite3.__version__
