import pytest

from src.data.rows import landing_row
from tests.conftest import run


def test_t6_replay_returns_the_original_result(conn):
    rows = [landing_row("A", 1000), landing_row("B", 2000)]
    first = run(conn, rows, batch_id="same")
    second = run(conn, rows, batch_id="same")
    assert second["status"] == "committed"
    assert second["inserted"] == first["inserted"]
    assert second["run_id"] == first["run_id"]
    assert conn.execute("SELECT count(*) FROM curated.transaction").fetchone()[0] == 2


def test_t7_crash_before_commit_rolls_back(conn):
    rows = [landing_row("A", 1000), landing_row("B", 2000)]
    with pytest.raises(RuntimeError, match="injected"):
        run(conn, rows, batch_id="crash", fault="before_commit")
    assert conn.execute("SELECT count(*) FROM curated.transaction").fetchone()[0] == 0
    result = run(conn, rows, batch_id="crash")
    assert result["status"] == "committed"
    assert result["inserted"] == 2
    assert conn.execute("SELECT count(*) FROM curated.transaction").fetchone()[0] == 2


def test_t8_checksum_conflict_does_not_change_rows(conn):
    run(conn, [landing_row("A", 1000)], batch_id="once")
    rejected = run(conn, [landing_row("A", 5000)], batch_id="once")
    amount = conn.execute(
        "SELECT amount FROM curated.transaction WHERE source_event_id = 'A'"
    ).fetchone()[0]
    assert rejected["status"] == "rejected"
    assert float(amount) == 10.0
