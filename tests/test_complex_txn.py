from src.data.rows import planted_bad_group, planted_purchase
from src.pipeline.proof import prove
from tests.conftest import run


def test_t14_and_t15_purchase_proof(conn):
    result = run(conn, planted_purchase(), batch_id="grp")
    column = prove(conn, "grp", "column")
    purchase = prove(conn, "grp", "purchase", "GRP-77")
    assert result["inserted"] == 4
    assert result["rejected"] == 0
    assert column["broken"] == 0
    assert purchase["broken"] == 0
    row = purchase["rows"][0]
    assert row["events"] == 4
    assert row["auth_superseded"] == 1
    assert float(row["net_amount"]) == 45.99


def test_t16_bad_sibling_rejects_whole_purchase(conn):
    run(conn, planted_purchase() + planted_bad_group(), batch_id="mixed")
    grp77 = conn.execute(
        "SELECT count(*) FROM curated.transaction WHERE purchase_id = 'GRP-77'"
    ).fetchone()[0]
    grp88 = conn.execute(
        "SELECT count(*) FROM curated.transaction WHERE purchase_id = 'GRP-88'"
    ).fetchone()[0]
    reasons = {
        row[0]
        for row in conn.execute(
            "SELECT reason FROM ops.reject WHERE purchase_id = 'GRP-88'"
        ).fetchall()
    }
    assert grp77 == 4
    assert grp88 == 0
    assert "unknown_product" in reasons
    assert "purchase_group" in reasons
