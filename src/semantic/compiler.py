"""The only component that produces SQL."""

from __future__ import annotations

import yaml

from src.paths import METRICS_PATH

NET_SQL = """
SELECT coalesce(sum(amount), 0) AS value
FROM curated.transaction
WHERE is_current
  AND event_kind IN ('completion', 'line', 'reversal')
  AND purchase_id IS NOT DISTINCT FROM coalesce($purchase, purchase_id)
  AND product_family IS NOT DISTINCT FROM coalesce($family, product_family)
"""

COUNT_SQL = """
SELECT count(*) AS value
FROM curated.transaction
WHERE is_current
  AND purchase_id IS NOT DISTINCT FROM coalesce($purchase, purchase_id)
  AND product_family IS NOT DISTINCT FROM coalesce($family, product_family)
"""

BENEFITS_SQL = """
SELECT count(*) AS value
FROM curated.benefits_claim
"""

SQL_BY_METRIC = {
    "net_settled_amount": NET_SQL,
    "transaction_count": COUNT_SQL,
    "benefits_claim_count": BENEFITS_SQL,
}


def load_metrics() -> list:
    payload = yaml.safe_load(METRICS_PATH.read_text())
    return list(payload["metrics"])


def compile_query(interpretation: dict) -> tuple:
    metric_id = interpretation.get("metric_id")
    if not metric_id or interpretation.get("clarification"):
        return "", {}
    filters = interpretation.get("filters") or {}
    params = {}
    if metric_id != "benefits_claim_count":
        params = {
            "purchase": filters.get("purchase_id"),
            "family": filters.get("product_family"),
        }
    return SQL_BY_METRIC[metric_id].strip(), params


def narrate(metric_id: str, value, filters: dict) -> str:
    if metric_id == "net_settled_amount":
        scope = ""
        if filters.get("purchase_id"):
            scope = " for " + filters["purchase_id"]
        if filters.get("product_family"):
            scope += " (" + filters["product_family"] + ")"
        return "Net settled amount%s is %.2f." % (scope, float(value))
    if metric_id == "transaction_count":
        return "There are %d current transaction events." % int(value)
    if metric_id == "benefits_claim_count":
        return "There are %d benefits claims." % int(value)
    return "No governed metric was selected."
