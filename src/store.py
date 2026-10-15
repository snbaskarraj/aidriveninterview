"""DuckDB schema, reference data, and catalog seed."""

from __future__ import annotations

import duckdb

from src.catalog.model import ASSETS, COLUMNS, POLICY
from src.pipeline.spec import default_pipeline

PRODUCTS = (
    ("DSL", "diesel"),
    ("UNL", "unleaded"),
    ("WASH", "car_wash"),
    ("EV", "ev"),
)
AUTHS = (
    ("A", "approved"),
    ("D", "declined"),
    ("R", "reversed"),
)
MERCHANTS = (
    ("M-88", 88, "Cedar Fuel", "fuel"),
    ("M-14", 14, "Harbor EV", "ev"),
    ("M-3", 3, "Plain Tolls", "tolls"),
)

SCHEMA = [
    "CREATE SCHEMA IF NOT EXISTS curated",
    "CREATE SCHEMA IF NOT EXISTS ops",
    "CREATE SCHEMA IF NOT EXISTS catalog",
    """
    CREATE TABLE IF NOT EXISTS ref_product (
        product_code VARCHAR PRIMARY KEY,
        product_family VARCHAR
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS ref_auth (
        auth_cd VARCHAR PRIMARY KEY,
        auth_status VARCHAR
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS ref_merchant (
        merchant_code VARCHAR PRIMARY KEY,
        merchant_id INTEGER,
        merchant_name VARCHAR,
        category VARCHAR
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS staging (
        batch_id VARCHAR,
        txn_id VARCHAR,
        src_sys VARCHAR,
        event_time VARCHAR,
        amt_cents BIGINT,
        qty DOUBLE,
        product_code VARCHAR,
        auth_cd VARCHAR,
        merchant_code VARCHAR,
        card_pan_last4 VARCHAR,
        card_pan_hash VARCHAR,
        purchase_id VARCHAR,
        event_kind VARCHAR,
        ingest_seq INTEGER,
        cardholder_name VARCHAR
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS staging_conformed (
        batch_id VARCHAR,
        source_system VARCHAR,
        source_event_id VARCHAR,
        purchase_id VARCHAR,
        event_kind VARCHAR,
        source_event_ts TIMESTAMP,
        txn_month DATE,
        amount DECIMAL(18, 2),
        quantity DECIMAL(18, 4),
        product_family VARCHAR,
        auth_status VARCHAR,
        merchant_id INTEGER,
        card_token VARCHAR,
        pan_last4 VARCHAR,
        payload_hash VARCHAR,
        is_current BOOLEAN,
        superseded_by VARCHAR,
        last_batch_id VARCHAR,
        cardholder_name VARCHAR,
        reject_reason VARCHAR
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS curated.transaction (
        source_system VARCHAR,
        source_event_id VARCHAR,
        purchase_id VARCHAR,
        event_kind VARCHAR,
        source_event_ts TIMESTAMP,
        txn_month DATE,
        amount DECIMAL(18, 2),
        quantity DECIMAL(18, 4),
        product_family VARCHAR,
        auth_status VARCHAR,
        merchant_id INTEGER,
        card_token VARCHAR,
        pan_last4 VARCHAR,
        payload_hash VARCHAR,
        is_current BOOLEAN,
        superseded_by VARCHAR,
        last_batch_id VARCHAR,
        cardholder_name VARCHAR,
        PRIMARY KEY (source_system, source_event_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS curated.transaction_history (
        history_seq INTEGER,
        source_system VARCHAR,
        source_event_id VARCHAR,
        amount DECIMAL(18, 2),
        payload_hash VARCHAR,
        image_note VARCHAR,
        deleted_by_batch_id VARCHAR
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS curated.cardholder (
        cardholder_id VARCHAR PRIMARY KEY,
        cardholder_name VARCHAR,
        email VARCHAR,
        ssn_token VARCHAR,
        state VARCHAR
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS curated.customer (
        customer_id VARCHAR PRIMARY KEY,
        customer_name VARCHAR,
        segment VARCHAR,
        region VARCHAR,
        state VARCHAR,
        fleet_size_band VARCHAR
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS curated.merchant (
        merchant_id INTEGER PRIMARY KEY,
        merchant_code VARCHAR,
        merchant_name VARCHAR,
        category VARCHAR
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS curated.benefits_claim (
        claim_id VARCHAR PRIMARY KEY,
        member_token VARCHAR,
        claim_category VARCHAR,
        claim_amount DECIMAL(18, 2),
        plan_type VARCHAR
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS curated.payment (
        payment_id VARCHAR PRIMARY KEY,
        client_id VARCHAR,
        rail VARCHAR,
        amount DECIMAL(18, 2),
        invoice_id VARCHAR
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS ops.batch_receipt (
        pipeline_name VARCHAR,
        batch_id VARCHAR,
        checksum VARCHAR,
        status VARCHAR,
        run_id VARCHAR,
        strategy VARCHAR,
        in_rows INTEGER,
        deduped INTEGER,
        rejected INTEGER,
        inserted INTEGER,
        updated INTEGER,
        deleted INTEGER,
        skipped INTEGER,
        unmatched INTEGER,
        applied INTEGER,
        PRIMARY KEY (pipeline_name, batch_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS ops.run_ledger (
        run_id VARCHAR PRIMARY KEY,
        pipeline_name VARCHAR,
        batch_id VARCHAR,
        checksum VARCHAR,
        status VARCHAR,
        strategy VARCHAR,
        in_rows INTEGER,
        deduped INTEGER,
        rejected INTEGER,
        inserted INTEGER,
        updated INTEGER,
        deleted INTEGER,
        skipped INTEGER,
        unmatched INTEGER,
        applied INTEGER
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS ops.reject (
        batch_id VARCHAR,
        reject_seq INTEGER,
        purchase_id VARCHAR,
        source_event_id VARCHAR,
        step VARCHAR,
        reason VARCHAR,
        image_json VARCHAR
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS ops.pipeline_spec (
        name VARCHAR PRIMARY KEY,
        version INTEGER,
        spec_json VARCHAR
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS catalog.meta (
        version VARCHAR
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS catalog.asset (
        asset_id VARCHAR PRIMARY KEY,
        schema_name VARCHAR,
        table_name VARCHAR,
        title VARCHAR,
        steward VARCHAR,
        owner VARCHAR
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS catalog.column (
        asset_id VARCHAR,
        column_name VARCHAR,
        data_type VARCHAR,
        class VARCHAR,
        mask VARCHAR
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS catalog.policy (
        persona VARCHAR,
        class VARCHAR,
        action VARCHAR
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS catalog.lineage_edge (
        asset_id VARCHAR,
        source_field VARCHAR,
        op VARCHAR,
        target_field VARCHAR
    )
    """,
]


def connect(path: str = ":memory:"):
    conn = duckdb.connect(path)
    init_schema(conn)
    return conn


def init_schema(conn) -> None:
    for statement in SCHEMA:
        conn.execute(statement)
    _seed_refs(conn)
    _seed_catalog(conn)


def _seed_refs(conn) -> None:
    if conn.execute("SELECT count(*) FROM ref_product").fetchone()[0] == 0:
        conn.executemany("INSERT INTO ref_product VALUES (?, ?)", PRODUCTS)
    if conn.execute("SELECT count(*) FROM ref_auth").fetchone()[0] == 0:
        conn.executemany("INSERT INTO ref_auth VALUES (?, ?)", AUTHS)
    if conn.execute("SELECT count(*) FROM ref_merchant").fetchone()[0] == 0:
        conn.executemany("INSERT INTO ref_merchant VALUES (?, ?, ?, ?)", MERCHANTS)
        conn.executemany(
            "INSERT INTO curated.merchant VALUES (?, ?, ?, ?)",
            [(merchant_id, code, name, category) for code, merchant_id, name, category in MERCHANTS],
        )


def _seed_catalog(conn) -> None:
    if conn.execute("SELECT count(*) FROM catalog.meta").fetchone()[0] == 0:
        conn.execute("INSERT INTO catalog.meta VALUES ('2026.10.15')")
    if conn.execute("SELECT count(*) FROM catalog.asset").fetchone()[0] == 0:
        conn.executemany(
            "INSERT INTO catalog.asset VALUES (?, ?, ?, ?, ?, ?)",
            [
                (
                    asset["asset_id"],
                    asset["schema_name"],
                    asset["table_name"],
                    asset["title"],
                    asset["steward"],
                    asset["owner"],
                )
                for asset in ASSETS
            ],
        )
        conn.executemany(
            "INSERT INTO catalog.column VALUES (?, ?, ?, ?, ?)",
            COLUMNS,
        )
        policy_rows = [
            (persona, kind, action)
            for persona, kinds in POLICY.items()
            for kind, action in kinds.items()
        ]
        conn.executemany("INSERT INTO catalog.policy VALUES (?, ?, ?)", policy_rows)
        edges = [
            ("transaction", column["source"], column["op"], column["target"])
            for column in default_pipeline().columns
        ]
        conn.executemany(
            "INSERT INTO catalog.lineage_edge VALUES (?, ?, ?, ?)",
            edges,
        )


def fetch_dicts(result) -> list:
    columns = [item[0] for item in result.description]
    return [dict(zip(columns, row)) for row in result.fetchall()]
