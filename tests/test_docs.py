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
        DOCS_DATA_DIR.glob("turu_*.py"),
    )
    def test_turu_docs_sample(self, script_file: Path):
        exec(script_file.read_text())
