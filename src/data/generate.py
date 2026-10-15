"""Seeded synthetic WEX landing rows. The 6,000,000 row path is SQL in load_scale."""

from __future__ import annotations

import json
import random
from datetime import datetime, timedelta

from src.data.rows import landing_row, planted_bad_group, planted_purchase
from src.paths import LANDING_PATH

SCALE_MIN_ROWS = 6000000
DEMO_ROWS = 2000

NAMES = (
    "Ada Fleet",
    "Bea Miles",
    "Cara North",
    "Dean Holt",
    "Eden Park",
    "Finn Cole",
    "Gale West",
    "Hart Lane",
)


def generate_rows(n: int = DEMO_ROWS, seed: int = 42) -> list:
    rng = random.Random(seed)
    products = ("DSL", "UNL", "WASH", "EV")
    merchants = ("M-88", "M-14", "M-3")
    start = datetime(2025, 4, 1, 8, 0, 0)
    rows = []
    for index in range(int(n)):
        moment = start + timedelta(days=index % 540, minutes=index % 60)
        rows.append(
            landing_row(
                "TXG-%d" % index,
                amt_cents=rng.randint(500, 20000),
                product_code=products[index % 4],
                event_time=moment.strftime("%Y-%m-%dT%H:%M:%S") + "Z",
                qty=round(rng.uniform(1, 40), 1),
                merchant_code=merchants[index % 3],
                purchase_id="P-%d" % index,
                event_kind="line",
                ingest_seq=1,
                card_pan="400000000000%04d" % (index % 10000),
                cardholder_name=NAMES[index % len(NAMES)],
            )
        )
    rows.extend(
        [
            landing_row(
                "TX-DUP",
                1000,
                event_time="2026-08-01T10:00:00Z",
                ingest_seq=1,
                purchase_id="P-DUP",
                cardholder_name="Finn Cole",
            ),
            landing_row(
                "TX-DUP",
                2500,
                event_time="2026-08-02T10:00:00Z",
                ingest_seq=2,
                purchase_id="P-DUP",
                cardholder_name="Finn Cole",
            ),
        ]
    )
    rows.extend(planted_purchase())
    rows.extend(planted_bad_group())
    return rows


def write_landing(rows: list, path=LANDING_PATH) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(rows))
    return str(path)


def seed_dimensions(conn) -> None:
    if conn.execute("SELECT count(*) FROM curated.customer").fetchone()[0]:
        return
    customers = [
        ("C-%02d" % index, "Fleet Co %d" % index, "fleet", region, state, band)
        for index, (region, state, band) in enumerate(
            (
                ("Northeast", "MA", "50-99"),
                ("Northeast", "NY", "100-249"),
                ("Southeast", "TX", "250-499"),
                ("Southeast", "FL", "20-49"),
                ("Midwest", "IL", "100-249"),
                ("Midwest", "OH", "50-99"),
                ("West", "CA", "500+"),
                ("West", "WA", "100-249"),
                ("Central", "CO", "50-99"),
                ("Central", "MO", "20-49"),
                ("Northeast", "PA", "100-249"),
                ("West", "AZ", "50-99"),
            ),
            start=1,
        )
    ]
    conn.executemany(
        "INSERT INTO curated.customer VALUES (?, ?, ?, ?, ?, ?)",
        customers,
    )
    holders = []
    for index, name in enumerate(NAMES, start=1):
        holders.append(
            (
                "H-%02d" % index,
                name,
                name.split()[0].lower() + "@example.com",
                "tok_ssn_%02d" % index,
                "TX" if index % 2 == 0 else "CA",
            )
        )
    conn.executemany(
        "INSERT INTO curated.cardholder VALUES (?, ?, ?, ?, ?)",
        holders,
    )
    claims = []
    for index, plan in enumerate(("HSA", "FSA", "COBRA", "HSA", "FSA", "COBRA", "HSA", "FSA"), start=1):
        claims.append(
            (
                "CLM-%02d" % index,
                "tok_member_%02d" % index,
                ("Dental", "Vision", "Medical")[index % 3],
                25.0 * index,
                plan,
            )
        )
    conn.executemany(
        "INSERT INTO curated.benefits_claim VALUES (?, ?, ?, ?, ?)",
        claims,
    )
    payments = []
    for index, rail in enumerate(("virtual_card", "ach", "virtual_card", "ach", "virtual_card", "ach"), start=1):
        payments.append(("PAY-%02d" % index, "C-%02d" % index, rail, 100.0 * index, "INV-%02d" % index))
    conn.executemany(
        "INSERT INTO curated.payment VALUES (?, ?, ?, ?, ?)",
        payments,
    )


def main() -> None:
    rows = generate_rows()
    path = write_landing(rows)
    print("Wrote %d landing rows to %s" % (len(rows), path))
    print("Scale contract is %d transaction rows. Load it with WEX_SCALE=1 pytest -q -m scale" % SCALE_MIN_ROWS)


if __name__ == "__main__":
    main()
