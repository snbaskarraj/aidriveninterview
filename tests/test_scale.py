import os

import pytest

from src.data.generate import SCALE_MIN_ROWS
from src.pipeline.runner import load_scale
from src.store import connect


def test_scale_contract_constant():
    assert SCALE_MIN_ROWS == 6000000


def test_scale_path_one_thousand(tmp_path):
    conn = connect(str(tmp_path / "scale.duckdb"))
    try:
        result = load_scale(conn, 1000)
        stored = conn.execute("SELECT count(*) FROM curated.transaction").fetchone()[0]
    finally:
        conn.close()
    assert result["status"] == "committed"
    assert result["inserted"] == 1000
    assert stored == 1000


@pytest.mark.scale
@pytest.mark.skipif(os.environ.get("WEX_SCALE") != "1", reason="set WEX_SCALE=1 to load 6,000,000 rows")
def test_six_million_rows(tmp_path):
    conn = connect(str(tmp_path / "full.duckdb"))
    try:
        result = load_scale(conn, SCALE_MIN_ROWS)
        stored = conn.execute("SELECT count(*) FROM curated.transaction").fetchone()[0]
    finally:
        conn.close()
    assert result["inserted"] == SCALE_MIN_ROWS
    assert stored == SCALE_MIN_ROWS
