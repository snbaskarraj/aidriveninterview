"""Idempotent insert, update, merge, and delete against conformed rows."""

from __future__ import annotations

TARGET_COLUMNS = """
    source_system, source_event_id, purchase_id, event_kind, source_event_ts,
    txn_month, amount, quantity, product_family, auth_status, merchant_id,
    card_token, pan_last4, payload_hash, is_current, superseded_by,
    last_batch_id, cardholder_name
"""

SELECT_COLUMNS = """
    s.source_system, s.source_event_id, s.purchase_id, s.event_kind, s.source_event_ts,
    s.txn_month, s.amount, s.quantity, s.product_family, s.auth_status, s.merchant_id,
    s.card_token, s.pan_last4, s.payload_hash, s.is_current, s.superseded_by,
    s.last_batch_id, s.cardholder_name
"""


def count_plan(conn, batch_id: str) -> dict:
    good = conn.execute(
        """
        SELECT count(*) FROM staging_conformed
        WHERE batch_id = ? AND reject_reason IS NULL
        """,
        [batch_id],
    ).fetchone()[0]
    inserted = conn.execute(
        """
        SELECT count(*)
        FROM staging_conformed s
        WHERE s.batch_id = ? AND s.reject_reason IS NULL
          AND NOT EXISTS (
            SELECT 1 FROM curated.transaction t
            WHERE t.source_system = s.source_system
              AND t.source_event_id = s.source_event_id
          )
        """,
        [batch_id],
    ).fetchone()[0]
    updated = conn.execute(
        """
        SELECT count(*)
        FROM staging_conformed s
        JOIN curated.transaction t
          ON t.source_system = s.source_system
         AND t.source_event_id = s.source_event_id
        WHERE s.batch_id = ? AND s.reject_reason IS NULL
          AND t.payload_hash IS DISTINCT FROM s.payload_hash
        """,
        [batch_id],
    ).fetchone()[0]
    skipped = conn.execute(
        """
        SELECT count(*)
        FROM staging_conformed s
        JOIN curated.transaction t
          ON t.source_system = s.source_system
         AND t.source_event_id = s.source_event_id
        WHERE s.batch_id = ? AND s.reject_reason IS NULL
          AND NOT (t.payload_hash IS DISTINCT FROM s.payload_hash)
        """,
        [batch_id],
    ).fetchone()[0]
    current_matches = conn.execute(
        """
        SELECT count(*)
        FROM staging_conformed s
        JOIN curated.transaction t
          ON t.source_system = s.source_system
         AND t.source_event_id = s.source_event_id
        WHERE s.batch_id = ? AND s.reject_reason IS NULL
          AND t.is_current
        """,
        [batch_id],
    ).fetchone()[0]
    any_matches = conn.execute(
        """
        SELECT count(*)
        FROM staging_conformed s
        JOIN curated.transaction t
          ON t.source_system = s.source_system
         AND t.source_event_id = s.source_event_id
        WHERE s.batch_id = ? AND s.reject_reason IS NULL
        """,
        [batch_id],
    ).fetchone()[0]
    return {
        "good": int(good),
        "inserted": int(inserted),
        "updated": int(updated),
        "skipped": int(skipped),
        "current_matches": int(current_matches),
        "any_matches": int(any_matches),
    }


def _insert_missing(conn, batch_id: str) -> None:
    conn.execute(
        f"""
        INSERT INTO curated.transaction ({TARGET_COLUMNS})
        SELECT {SELECT_COLUMNS}
        FROM staging_conformed s
        WHERE s.batch_id = ? AND s.reject_reason IS NULL
          AND NOT EXISTS (
            SELECT 1 FROM curated.transaction t
            WHERE t.source_system = s.source_system
              AND t.source_event_id = s.source_event_id
          )
        """,
        [batch_id],
    )


def _update_changed(conn, batch_id: str) -> None:
    conn.execute(
        """
        UPDATE curated.transaction t
        SET
            purchase_id = s.purchase_id,
            event_kind = s.event_kind,
            source_event_ts = s.source_event_ts,
            txn_month = s.txn_month,
            amount = s.amount,
            quantity = s.quantity,
            product_family = s.product_family,
            auth_status = s.auth_status,
            merchant_id = s.merchant_id,
            card_token = s.card_token,
            pan_last4 = s.pan_last4,
            payload_hash = s.payload_hash,
            last_batch_id = s.last_batch_id,
            cardholder_name = s.cardholder_name
        FROM staging_conformed s
        WHERE t.source_system = s.source_system
          AND t.source_event_id = s.source_event_id
          AND s.batch_id = ?
          AND s.reject_reason IS NULL
          AND t.payload_hash IS DISTINCT FROM s.payload_hash
        """,
        [batch_id],
    )


def _delete_matched(conn, batch_id: str) -> None:
    conn.execute(
        """
        INSERT INTO curated.transaction_history
        SELECT
            coalesce((SELECT max(history_seq) FROM curated.transaction_history), 0)
                + row_number() OVER (),
            t.source_system,
            t.source_event_id,
            t.amount,
            t.payload_hash,
            'prior image',
            ?
        FROM curated.transaction t
        JOIN staging_conformed s
          ON t.source_system = s.source_system
         AND t.source_event_id = s.source_event_id
        WHERE s.batch_id = ?
          AND s.reject_reason IS NULL
          AND t.is_current
        """,
        [batch_id, batch_id],
    )
    conn.execute(
        """
        UPDATE curated.transaction t
        SET is_current = FALSE, last_batch_id = ?
        FROM staging_conformed s
        WHERE t.source_system = s.source_system
          AND t.source_event_id = s.source_event_id
          AND s.batch_id = ?
          AND s.reject_reason IS NULL
          AND t.is_current
        """,
        [batch_id, batch_id],
    )


def supersede_auths(conn, batch_id: str) -> None:
    conn.execute(
        """
        UPDATE curated.transaction AS auth
        SET is_current = FALSE, superseded_by = comp.source_event_id
        FROM staging_conformed AS comp
        WHERE comp.batch_id = ?
          AND comp.reject_reason IS NULL
          AND comp.event_kind = 'completion'
          AND auth.purchase_id = comp.purchase_id
          AND auth.product_family = comp.product_family
          AND auth.event_kind = 'auth'
          AND auth.source_system = comp.source_system
        """,
        [batch_id],
    )


def apply_strategy(conn, strategy: str, batch_id: str) -> dict:
    plan = count_plan(conn, batch_id)
    inserted = updated = deleted = skipped = unmatched = 0
    if strategy == "insert":
        _insert_missing(conn, batch_id)
        inserted = plan["inserted"]
        skipped = plan["any_matches"]
    elif strategy == "update":
        _update_changed(conn, batch_id)
        updated = plan["updated"]
        skipped = plan["skipped"]
        unmatched = plan["good"] - plan["any_matches"]
    elif strategy == "merge":
        _update_changed(conn, batch_id)
        _insert_missing(conn, batch_id)
        inserted = plan["inserted"]
        updated = plan["updated"]
        skipped = plan["skipped"]
    elif strategy == "delete":
        deleted = plan["current_matches"]
        unmatched = plan["good"] - plan["any_matches"]
        skipped = plan["any_matches"] - plan["current_matches"]
        _delete_matched(conn, batch_id)
    else:
        raise ValueError("unknown load strategy")
    if strategy != "delete":
        supersede_auths(conn, batch_id)
    return {
        "inserted": inserted,
        "updated": updated,
        "deleted": deleted,
        "skipped": skipped,
        "unmatched": unmatched,
        "applied": inserted + updated + deleted,
    }
