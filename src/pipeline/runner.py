"""Batch runner. One DuckDB transaction publishes curated rows and the ledger."""

from __future__ import annotations

import hashlib
import json
import uuid

from src.data.rows import pan_fingerprint
from src.pipeline.strategies import apply_strategy
from src.pipeline.transform import build_conformed, dedup_staging, write_rejects
from src.store import fetch_dicts

RESULT_KEYS = (
    "run_id",
    "status",
    "strategy",
    "pipeline_name",
    "batch_id",
    "in_rows",
    "deduped",
    "rejected",
    "inserted",
    "updated",
    "deleted",
    "skipped",
    "unmatched",
    "applied",
)


def canonical_checksum(rows: list) -> str:
    materialized = []
    for row in rows:
        copy = {key: row[key] for key in sorted(row) if key != "card_pan"}
        copy["card_pan_hash"] = hashlib.sha256(str(row.get("card_pan") or "").encode()).hexdigest()
        materialized.append(copy)
    materialized.sort(key=lambda item: (str(item.get("txn_id")), int(item.get("ingest_seq") or 0)))
    payload = json.dumps(materialized, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(payload.encode()).hexdigest()


def _result_from_row(row: dict) -> dict:
    return {key: row[key] for key in RESULT_KEYS if key in row}


def _receipt(conn, pipeline_name: str, batch_id: str):
    rows = fetch_dicts(
        conn.execute(
            """
            SELECT * FROM ops.batch_receipt
            WHERE pipeline_name = ? AND batch_id = ?
            """,
            [pipeline_name, batch_id],
        )
    )
    return rows[0] if rows else None


def replay_decision(conn, pipeline, batch_id: str, checksum: str):
    existing = _receipt(conn, pipeline.name, batch_id)
    if not existing:
        return None
    if existing["status"] == "committed" and existing["checksum"] == checksum:
        return _result_from_row(existing)
    if existing["status"] == "committed" and existing["checksum"] != checksum:
        run_id = str(uuid.uuid4())
        conn.execute(
            """
            INSERT INTO ops.run_ledger VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                run_id,
                pipeline.name,
                batch_id,
                checksum,
                "rejected",
                pipeline.load_strategy,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
            ],
        )
        return {
            "run_id": run_id,
            "status": "rejected",
            "strategy": pipeline.load_strategy,
            "pipeline_name": pipeline.name,
            "batch_id": batch_id,
            "in_rows": 0,
            "deduped": 0,
            "rejected": 0,
            "inserted": 0,
            "updated": 0,
            "deleted": 0,
            "skipped": 0,
            "unmatched": 0,
            "applied": 0,
        }
    return None


def load_staging(conn, batch_id: str, rows: list) -> int:
    conn.execute("DELETE FROM staging WHERE batch_id = ?", [batch_id])
    payload = []
    for index, row in enumerate(rows):
        pan = row.get("card_pan") or ""
        last4, digest = pan_fingerprint(pan) if pan else ("", "")
        payload.append(
            (
                batch_id,
                row.get("txn_id"),
                row.get("src_sys") or "fleet",
                row.get("event_time"),
                None if row.get("amt_cents") is None else int(row["amt_cents"]),
                float(row.get("qty") or 0),
                row.get("product_code"),
                row.get("auth_cd") or "A",
                row.get("merchant_code"),
                last4,
                digest,
                row.get("purchase_id"),
                row.get("event_kind") or "line",
                int(row.get("ingest_seq") or index + 1),
                row.get("cardholder_name"),
            )
        )
    if payload:
        conn.executemany("INSERT INTO staging VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", payload)
    return len(payload)


def _execute(conn, pipeline, batch_id: str, checksum: str, in_rows: int, fault: str) -> dict:
    removed = dedup_staging(conn, batch_id)
    build_conformed(conn, batch_id)
    counts = apply_strategy(conn, pipeline.load_strategy, batch_id)
    rejected = write_rejects(conn, batch_id)
    if fault == "before_commit":
        raise RuntimeError("injected failure before commit")
    run_id = str(uuid.uuid4())
    values = [
        pipeline.name,
        batch_id,
        checksum,
        "committed",
        run_id,
        pipeline.load_strategy,
        int(in_rows),
        int(removed),
        int(rejected),
        counts["inserted"],
        counts["updated"],
        counts["deleted"],
        counts["skipped"],
        counts["unmatched"],
        counts["applied"],
    ]
    conn.execute(
        "INSERT INTO ops.batch_receipt VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        values,
    )
    conn.execute(
        "INSERT INTO ops.run_ledger VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        [
            run_id,
            pipeline.name,
            batch_id,
            checksum,
            "committed",
            pipeline.load_strategy,
            int(in_rows),
            int(removed),
            int(rejected),
            counts["inserted"],
            counts["updated"],
            counts["deleted"],
            counts["skipped"],
            counts["unmatched"],
            counts["applied"],
        ],
    )
    return {
        "run_id": run_id,
        "status": "committed",
        "strategy": pipeline.load_strategy,
        "pipeline_name": pipeline.name,
        "batch_id": batch_id,
        "in_rows": int(in_rows),
        "deduped": int(removed),
        "rejected": int(rejected),
        "inserted": counts["inserted"],
        "updated": counts["updated"],
        "deleted": counts["deleted"],
        "skipped": counts["skipped"],
        "unmatched": counts["unmatched"],
        "applied": counts["applied"],
    }


def _in_transaction(conn, work):
    conn.begin()
    try:
        result = work()
        conn.commit()
        return result
    except Exception:
        conn.rollback()
        raise


def apply_batch(conn, pipeline, batch_id: str, rows: list, fault: str = None) -> dict:
    checksum = canonical_checksum(rows)
    replay = replay_decision(conn, pipeline, batch_id, checksum)
    if replay:
        return replay

    def work():
        in_rows = load_staging(conn, batch_id, rows)
        return _execute(conn, pipeline, batch_id, checksum, in_rows, fault)

    return _in_transaction(conn, work)


def load_scale(conn, n: int, seed: int = 42) -> dict:
    from src.pipeline.spec import default_pipeline

    pipeline = default_pipeline("merge")
    count = int(n)
    batch_id = "scale-%s" % count
    checksum = hashlib.sha256(("%s:%s" % (seed, count)).encode()).hexdigest()
    replay = replay_decision(conn, pipeline, batch_id, checksum)
    if replay:
        return replay

    def work():
        conn.execute("DELETE FROM staging WHERE batch_id = ?", [batch_id])
        conn.execute(
            """
            INSERT INTO staging
            SELECT
                ?,
                'TXS-' || range::VARCHAR,
                'fleet',
                '2026-09-15T14:03:00Z',
                1000 + (range %% 50),
                1.0,
                CASE range %% 3 WHEN 0 THEN 'DSL' WHEN 1 THEN 'UNL' ELSE 'WASH' END,
                'A',
                'M-88',
                '4242',
                'abc123hashdigest00',
                'P-' || range::VARCHAR,
                'line',
                CAST(range AS INTEGER),
                'Rider ' || (range %% 20)::VARCHAR
            FROM range(%d)
            """
            % count,
            [batch_id],
        )
        return _execute(conn, pipeline, batch_id, checksum, count, None)

    return _in_transaction(conn, work)
