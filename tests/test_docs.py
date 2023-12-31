import os
from pathlib import Path

import pytest

DOCS_DATA_DIR = Path(__file__).parents[1] / "docs" / "data"


class TestTuruDocs:
    @pytest.mark.skipif(
        condition="USE_REAL_CONNECTION" not in os.environ
        or os.environ["USE_REAL_CONNECTION"].lower() != "true",
        reason="USE_REAL_CONNECTION flag is not set.",
    )
    @pytest.mark.parametrize(
        "script_file",
        [
            "turu_sqlite3_sample.py",
            "turu_mysql_sample.py",
            "turu_postgres_sample.py",
            "turu_snowflake_sample.py",
            "turu_bigquery_sample.py",
        ],
    )
    def test_turu_docs_sample(self, script_file: str):
        exec((DOCS_DATA_DIR / script_file).read_text())
