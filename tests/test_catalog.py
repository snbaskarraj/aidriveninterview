from src.data.rows import landing_row
from tests.conftest import run


def test_t5_catalog_and_ledger_after_commit(conn):
    result = run(conn, [landing_row("A", 1000)], batch_id="cat")
    klass = conn.execute(
        """
        SELECT class FROM catalog.column
        WHERE asset_id = 'transaction' AND column_name = 'card_token'
        """
    ).fetchone()[0]
    edge = conn.execute(
        """
        SELECT count(*) FROM catalog.lineage_edge
        WHERE asset_id = 'transaction' AND op = 'trim'
        """
    ).fetchone()[0]
    ledger = conn.execute(
        "SELECT status, checksum FROM ops.run_ledger WHERE run_id = ?",
        [result["run_id"]],
    ).fetchone()
    assert klass == "PCI"
    assert edge >= 1
    assert ledger[0] == "committed"
    assert ledger[1]
