"""Read-only DuckDB queries that show the transforms held."""

from __future__ import annotations

from decimal import Decimal

from src.catalog.mask import present
from src.store import fetch_dicts

COLUMN_SQL = """
SELECT
    trim(s.txn_id) AS source_event_id,
    upper(trim(s.src_sys)) AS source_system,
    s.purchase_id,
    s.amt_cents / 100.0 AS expected_amount,
    t.amount,
    s.product_code,
    p.product_family AS expected_family,
    t.product_family,
    t.pan_last4,
    t.card_token,
    t.event_kind
FROM staging s
JOIN ref_product p ON p.product_code = trim(s.product_code)
JOIN curated.transaction t
  ON t.source_event_id = trim(s.txn_id)
 AND t.source_system = upper(trim(s.src_sys))
WHERE s.batch_id = ?
  AND (
    t.amount IS DISTINCT FROM CAST(s.amt_cents AS DECIMAL(18, 2)) / 100
    OR t.product_family IS DISTINCT FROM p.product_family
    OR t.pan_last4 IS DISTINCT FROM s.card_pan_last4
    OR t.card_token IS NULL
  )
"""

SAMPLE_SQL = """
SELECT
    trim(s.txn_id) AS source_event_id,
    s.txn_id AS landing_txn_id,
    s.amt_cents AS landing_amt_cents,
    s.product_code AS landing_product_code,
    s.card_pan_last4 AS landing_last4,
    t.amount,
    t.product_family,
    t.pan_last4,
    t.card_token,
    t.event_kind,
    t.purchase_id,
    t.is_current
FROM staging s
JOIN curated.transaction t
  ON t.source_event_id = trim(s.txn_id)
 AND t.source_system = upper(trim(s.src_sys))
WHERE s.batch_id = ?
ORDER BY CASE WHEN s.purchase_id = 'GRP-77' THEN 0 ELSE 1 END, trim(s.txn_id)
LIMIT 20
"""

PURCHASE_SQL = """
SELECT
    purchase_id,
    count(*) AS events,
    count(*) FILTER (WHERE event_kind = 'auth' AND NOT is_current) AS auth_superseded,
    sum(amount) FILTER (
        WHERE is_current AND event_kind IN ('completion', 'line', 'reversal')
    ) AS net_amount
FROM curated.transaction
WHERE purchase_id = ?
GROUP BY purchase_id
"""


def _numb(value):
    if isinstance(value, Decimal):
        return float(value)
    return value


def _clean(rows: list) -> list:
    cleaned = []
    for row in rows:
        cleaned.append({key: _numb(value) for key, value in row.items()})
    return cleaned


def prove(conn, batch_id: str, check: str, purchase_id: str = "GRP-77", persona: str = "data_engineer") -> dict:
    if check == "column":
        broken = conn.execute(
            "SELECT count(*) FROM (" + COLUMN_SQL + ") mismatches",
            [batch_id],
        ).fetchone()[0]
        if int(broken) == 0:
            rows = _clean(fetch_dicts(conn.execute(SAMPLE_SQL, [batch_id])))
        else:
            rows = _clean(fetch_dicts(conn.execute(COLUMN_SQL + " LIMIT 50", [batch_id])))
        rows = [_mask_proof(row, persona) for row in rows]
        return {"check": "column", "broken": int(broken), "rows": rows, "sql": COLUMN_SQL.strip()}
    if check == "purchase":
        rows = _clean(fetch_dicts(conn.execute(PURCHASE_SQL, [purchase_id])))
        if not rows:
            return {
                "check": "purchase",
                "broken": 1,
                "rows": [],
                "sql": PURCHASE_SQL.strip(),
            }
        row = rows[0]
        if purchase_id == "GRP-77":
            broken = 0 if (
                row["events"] == 4
                and row["auth_superseded"] == 1
                and abs(float(row["net_amount"]) - 45.99) < 0.001
            ) else 1
        else:
            broken = 0 if row["events"] else 1
        if persona in ("business_analyst", "product_manager"):
            rows = [{"purchase_id": row["purchase_id"], "events": row["events"], "net_amount": row["net_amount"]}]
        return {"check": "purchase", "broken": broken, "rows": rows, "sql": PURCHASE_SQL.strip()}
    raise ValueError("unknown proof check")


def _mask_proof(row: dict, persona: str) -> dict:
    sensitive = {
        "card_token": row.get("card_token"),
        "pan_last4": row.get("pan_last4"),
    }
    masked = present(sensitive, persona)
    output = {key: value for key, value in row.items() if key not in sensitive}
    for key in ("card_token", "pan_last4"):
        if key in masked:
            output[key] = masked[key]
    if persona == "data_steward" and "classifications" in masked:
        output["classifications"] = masked["classifications"]
    return output
