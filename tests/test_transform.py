from src.data.rows import landing_row, pan_fingerprint
from tests.conftest import run


def test_t12_padded_id_amount_product_and_stable_token(conn):
    row = landing_row("  TX-12 ", 4599, "DSL", card_pan="4111111111111234")
    first = run(conn, [row], batch_id="t12")
    second = run(conn, [row], batch_id="t12")
    stored = conn.execute(
        """
        SELECT source_event_id, amount, product_family, pan_last4, card_token
        FROM curated.transaction
        WHERE source_event_id = 'TX-12'
        """
    ).fetchone()
    last4, digest = pan_fingerprint("4111111111111234")
    columns = [item[0] for item in conn.execute("DESCRIBE curated.transaction").fetchall()]
    assert stored[0] == "TX-12"
    assert float(stored[1]) == 45.99
    assert stored[2] == "diesel"
    assert stored[3] == last4 == "1234"
    assert stored[4] == "tok_" + digest[:16]
    assert "card_pan" not in columns
    assert second["status"] == "committed"
    assert second["inserted"] == first["inserted"]
    assert second["run_id"] == first["run_id"]


def test_t13_bad_timestamp_is_rejected_and_kept(conn):
    run(conn, [landing_row("TX-BAD", event_time="yesterday")], batch_id="t13")
    assert (
        conn.execute(
            "SELECT count(*) FROM curated.transaction WHERE source_event_id = 'TX-BAD'"
        ).fetchone()[0]
        == 0
    )
    reason = conn.execute(
        "SELECT reason, image_json FROM ops.reject WHERE source_event_id = 'TX-BAD'"
    ).fetchone()
    assert reason[0] == "unparseable_timestamp"
    assert "card_pan" not in reason[1]
