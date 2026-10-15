from src.data.rows import landing_row
from tests.conftest import run


def _amount(conn, event_id):
    row = conn.execute(
        "SELECT amount, is_current FROM curated.transaction WHERE source_event_id = ?",
        [event_id],
    ).fetchone()
    return row


def test_t1_insert_keeps_latest_duplicate(conn):
    rows = [
        landing_row("K1", 100, event_time="2026-09-01T00:00:00Z", ingest_seq=1),
        landing_row("K2", 200, ingest_seq=1),
        landing_row("K3", 300, ingest_seq=1),
        landing_row("K1", 999, event_time="2026-09-02T00:00:00Z", ingest_seq=2),
    ]
    result = run(conn, rows, strategy="insert", batch_id="t1")
    assert result["status"] == "committed"
    assert result["inserted"] == 3
    assert result["deduped"] == 1
    assert conn.execute("SELECT count(*) FROM curated.transaction").fetchone()[0] == 3
    assert float(_amount(conn, "K1")[0]) == 9.99


def test_t2_update_skips_unknown_keys(conn):
    run(
        conn,
        [landing_row("A", 1000), landing_row("B", 2000), landing_row("C", 3000)],
        strategy="insert",
        batch_id="seed",
    )
    result = run(
        conn,
        [landing_row("A", 1500), landing_row("B", 2500), landing_row("Z", 999)],
        strategy="update",
        batch_id="upd",
    )
    assert result["updated"] == 2
    assert result["unmatched"] == 1
    assert result["inserted"] == 0
    assert conn.execute("SELECT count(*) FROM curated.transaction").fetchone()[0] == 3
    assert float(_amount(conn, "A")[0]) == 15.0
    assert _amount(conn, "Z") is None


def test_t3_merge_insert_update_and_skip(conn):
    run(
        conn,
        [landing_row("A", 1000), landing_row("B", 2000)],
        strategy="merge",
        batch_id="seed",
    )
    result = run(
        conn,
        [landing_row("A", 1000), landing_row("B", 4000), landing_row("C", 500)],
        strategy="merge",
        batch_id="merge",
    )
    assert result["inserted"] == 1
    assert result["updated"] == 1
    assert result["skipped"] == 1
    assert float(_amount(conn, "B")[0]) == 40.0
    assert float(_amount(conn, "C")[0]) == 5.0


def test_t4_delete_keeps_history_and_collapses_repeat(conn):
    run(
        conn,
        [landing_row("K1", 1000), landing_row("K2", 2000)],
        strategy="insert",
        batch_id="seed",
    )
    result = run(
        conn,
        [
            landing_row("K1", 1000, ingest_seq=1),
            landing_row("K1", 1000, event_time="2026-09-16T00:00:00Z", ingest_seq=2),
        ],
        strategy="delete",
        batch_id="del",
    )
    assert result["deleted"] == 1
    assert result["deduped"] == 1
    assert _amount(conn, "K1")[1] is False
    assert _amount(conn, "K2")[1] is True
    assert conn.execute("SELECT count(*) FROM curated.transaction_history").fetchone()[0] == 1
