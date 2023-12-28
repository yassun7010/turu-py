from pathlib import Path

import pytest

DOCS_DATA_DIR = Path(__file__).parents[1] / "docs" / "data"


class TestTuruDocs:
    @pytest.mark.parametrize("script_file", ["turu_snowflake_sample.py"])
    def test_turu_snowflake_sample(self, script_file: str):
        exec((DOCS_DATA_DIR / script_file).read_text())
