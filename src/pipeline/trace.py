"""Source-to-target trace for one committed run."""

from __future__ import annotations

import re

from src.catalog.mask import present
from src.store import fetch_dicts

SAMPLE_SQL = """
SELECT
    s.txn_id AS before_txn_id,
    s.amt_cents AS before_amt_cents,
    s.product_code AS before_product_code,
    s.card_pan_last4 AS before_last4,
    t.source_event_id AS after_event_id,
    t.amount AS after_amount,
    t.product_family AS after_family,
    t.card_token AS after_token,
    t.pan_last4 AS after_last4,
    t.event_kind AS after_event_kind,
    t.purchase_id AS purchase_id
FROM staging s
JOIN curated.transaction t
  ON t.source_event_id = trim(s.txn_id)
 AND t.source_system = upper(trim(s.src_sys))
WHERE s.batch_id = ?
ORDER BY CASE WHEN s.purchase_id = 'GRP-77' THEN 0 ELSE 1 END, t.source_event_id
LIMIT 20
"""


def _node(name: str) -> str:
    return "n_" + re.sub(r"[^A-Za-z0-9]", "_", name)


def column_mermaid(pipeline) -> str:
    lines = ["flowchart LR"]
    for column in pipeline.columns:
        lines.append(
            '  %s["%s"] -->|%s| %s["%s"]'
            % (
                _node(column["source"]),
                column["source"],
                column["op"],
                _node(column["target"]),
                column["target"],
            )
        )
    return "\n".join(lines)


def lineage_mermaid(edges: list) -> str:
    lines = ["flowchart LR"]
    if not edges:
        lines.append("  empty[No lineage]")
        return "\n".join(lines)
    for edge in edges:
        lines.append(
            '  %s["%s"] -->|%s| %s["%s"]'
            % (
                _node(edge["source_field"]),
                edge["source_field"],
                edge["op"],
                _node(edge["target_field"]),
                edge["target_field"],
            )
        )
    return "\n".join(lines)


def _numb(value):
    from decimal import Decimal

    if isinstance(value, Decimal):
        return float(value)
    return value


def transform_view(conn, batch_id: str, pipeline, persona: str) -> dict:
    sample_rows = []
    if persona in ("data_steward", "data_engineer"):
        for row in fetch_dicts(conn.execute(SAMPLE_SQL, [batch_id])):
            cleaned = {key: _numb(value) for key, value in row.items()}
            masked = present(
                {
                    "card_token": cleaned.get("after_token"),
                    "pan_last4": cleaned.get("after_last4"),
                },
                persona,
            )
            cleaned.pop("after_token", None)
            cleaned.pop("after_last4", None)
            if "card_token" in masked:
                cleaned["after_token"] = masked["card_token"]
            if "pan_last4" in masked:
                cleaned["after_last4"] = masked["pan_last4"]
            sample_rows.append(cleaned)
    rejects = fetch_dicts(
        conn.execute(
            """
            SELECT source_event_id, purchase_id, reason
            FROM ops.reject
            WHERE batch_id = ?
            LIMIT 20
            """,
            [batch_id],
        )
    )
    return {
        "steps": list(pipeline.transforms),
        "columns": list(pipeline.columns),
        "sample": sample_rows,
        "rejects": rejects,
        "mermaid": column_mermaid(pipeline),
    }
