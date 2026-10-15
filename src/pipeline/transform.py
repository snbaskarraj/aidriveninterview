"""SQL transforms from landing shape to the conformed row."""

from __future__ import annotations


def dedup_staging(conn, batch_id: str) -> int:
    """Keep the latest event time for each trimmed source key. Return rows removed."""
    before = conn.execute(
        "SELECT count(*) FROM staging WHERE batch_id = ?",
        [batch_id],
    ).fetchone()[0]
    conn.execute("DELETE FROM staging_conformed WHERE batch_id = ?", [batch_id])
    conn.execute(
        """
        CREATE OR REPLACE TEMP TABLE staging_winners AS
        SELECT * EXCLUDE (rn)
        FROM (
            SELECT
                s.*,
                row_number() OVER (
                    PARTITION BY upper(trim(s.src_sys)), trim(s.txn_id)
                    ORDER BY coalesce(
                        try_strptime(replace(s.event_time, 'Z', ''), '%Y-%m-%dT%H:%M:%S'),
                        TIMESTAMP '1970-01-01'
                    ) DESC,
                    s.ingest_seq DESC
                ) AS rn
            FROM staging s
            WHERE s.batch_id = ?
        )
        WHERE rn = 1
        """,
        [batch_id],
    )
    conn.execute("DELETE FROM staging WHERE batch_id = ?", [batch_id])
    conn.execute(
        """
        INSERT INTO staging
        SELECT
            batch_id, txn_id, src_sys, event_time, amt_cents, qty, product_code,
            auth_cd, merchant_code, card_pan_last4, card_pan_hash, purchase_id,
            event_kind, ingest_seq, cardholder_name
        FROM staging_winners
        """
    )
    after = conn.execute(
        "SELECT count(*) FROM staging WHERE batch_id = ?",
        [batch_id],
    ).fetchone()[0]
    return int(before - after)


def build_conformed(conn, batch_id: str) -> None:
    conn.execute("DELETE FROM staging_conformed WHERE batch_id = ?", [batch_id])
    conn.execute(
        """
        INSERT INTO staging_conformed
        SELECT
            s.batch_id,
            upper(trim(s.src_sys)) AS source_system,
            trim(s.txn_id) AS source_event_id,
            nullif(trim(s.purchase_id), '') AS purchase_id,
            coalesce(nullif(lower(trim(s.event_kind)), ''), 'line') AS event_kind,
            try_strptime(replace(s.event_time, 'Z', ''), '%Y-%m-%dT%H:%M:%S') AS source_event_ts,
            CAST(date_trunc(
                'month',
                try_strptime(replace(s.event_time, 'Z', ''), '%Y-%m-%dT%H:%M:%S')
            ) AS DATE) AS txn_month,
            CAST(s.amt_cents AS DECIMAL(18, 2)) / 100 AS amount,
            CAST(s.qty AS DECIMAL(18, 4)) AS quantity,
            p.product_family,
            a.auth_status,
            m.merchant_id,
            CASE
                WHEN s.card_pan_hash IS NULL OR s.card_pan_hash = '' THEN NULL
                ELSE 'tok_' || substr(s.card_pan_hash, 1, 16)
            END AS card_token,
            s.card_pan_last4 AS pan_last4,
            NULL AS payload_hash,
            TRUE AS is_current,
            NULL AS superseded_by,
            s.batch_id AS last_batch_id,
            trim(s.cardholder_name) AS cardholder_name,
            NULL AS reject_reason
        FROM staging s
        LEFT JOIN ref_product p ON p.product_code = trim(s.product_code)
        LEFT JOIN ref_auth a ON a.auth_cd = trim(s.auth_cd)
        LEFT JOIN ref_merchant m ON m.merchant_code = trim(s.merchant_code)
        WHERE s.batch_id = ?
        """,
        [batch_id],
    )
    conn.execute(
        """
        UPDATE staging_conformed
        SET reject_reason = CASE
            WHEN source_event_ts IS NULL THEN 'unparseable_timestamp'
            WHEN source_event_id IS NULL OR source_event_id = '' THEN 'missing_key'
            WHEN product_family IS NULL THEN 'unknown_product'
            WHEN auth_status IS NULL THEN 'unknown_auth'
            WHEN merchant_id IS NULL THEN 'unknown_merchant'
            WHEN amount IS NULL THEN 'missing_amount'
            WHEN event_kind NOT IN ('auth', 'line', 'completion', 'reversal') THEN 'unknown_event_kind'
            ELSE NULL
        END
        WHERE batch_id = ?
        """,
        [batch_id],
    )
    conn.execute(
        """
        CREATE OR REPLACE TEMP TABLE bad_purchase AS
        SELECT DISTINCT purchase_id
        FROM staging_conformed
        WHERE batch_id = ?
          AND reject_reason IS NOT NULL
          AND purchase_id IS NOT NULL
        """,
        [batch_id],
    )
    conn.execute(
        """
        UPDATE staging_conformed
        SET reject_reason = 'purchase_group'
        WHERE batch_id = ?
          AND reject_reason IS NULL
          AND purchase_id IN (SELECT purchase_id FROM bad_purchase)
        """,
        [batch_id],
    )
    conn.execute(
        """
        UPDATE staging_conformed
        SET payload_hash = lower(hex(sha256(concat_ws(
            '|',
            printf('%.2f', CAST(amount AS DOUBLE)),
            coalesce(auth_status, ''),
            coalesce(CAST(merchant_id AS VARCHAR), ''),
            coalesce(product_family, ''),
            printf('%.4f', CAST(coalesce(quantity, 0) AS DOUBLE))
        ))))
        WHERE batch_id = ? AND reject_reason IS NULL
        """,
        [batch_id],
    )


def write_rejects(conn, batch_id: str) -> int:
    conn.execute("DELETE FROM ops.reject WHERE batch_id = ?", [batch_id])
    conn.execute(
        """
        INSERT INTO ops.reject
        SELECT
            batch_id,
            row_number() OVER (),
            purchase_id,
            source_event_id,
            'transform',
            reject_reason,
            printf(
                '{"event":"%s","reason":"%s","amount":"%s"}',
                coalesce(source_event_id, ''),
                reject_reason,
                coalesce(CAST(amount AS VARCHAR), '')
            )
        FROM staging_conformed
        WHERE batch_id = ? AND reject_reason IS NOT NULL
        """,
        [batch_id],
    )
    return int(
        conn.execute(
            "SELECT count(*) FROM ops.reject WHERE batch_id = ?",
            [batch_id],
        ).fetchone()[0]
    )
