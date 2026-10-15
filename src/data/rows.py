"""Landing-row helpers. Card numbers stay on the landing record only."""

from __future__ import annotations

import hashlib


def pan_fingerprint(pan: str) -> tuple:
    digits = "".join(ch for ch in str(pan or "") if ch.isdigit())
    digest = hashlib.sha256(digits.encode()).hexdigest()
    last4 = digits[-4:] if len(digits) >= 4 else digits
    return last4, digest


def landing_row(
    txn_id,
    amt_cents=4599,
    product_code="DSL",
    event_time="2026-09-15T14:03:00Z",
    src_sys="fleet",
    qty=12.4,
    auth_cd="A",
    merchant_code="M-88",
    card_pan="4111111111111234",
    purchase_id=None,
    event_kind="line",
    ingest_seq=1,
    cardholder_name="Ada Fleet",
):
    return {
        "txn_id": txn_id,
        "src_sys": src_sys,
        "event_time": event_time,
        "amt_cents": int(amt_cents),
        "qty": float(qty),
        "product_code": product_code,
        "auth_cd": auth_cd,
        "merchant_code": merchant_code,
        "card_pan": card_pan,
        "purchase_id": purchase_id if purchase_id is not None else str(txn_id).strip(),
        "event_kind": event_kind,
        "ingest_seq": int(ingest_seq),
        "cardholder_name": cardholder_name,
    }


def planted_purchase():
    return [
        landing_row(
            "TX-100",
            8000,
            "DSL",
            "2026-09-15T14:00:00Z",
            qty=0,
            purchase_id="GRP-77",
            event_kind="auth",
            ingest_seq=1,
            cardholder_name="Ada Fleet",
        ),
        landing_row(
            "TX-101",
            500,
            "WASH",
            "2026-09-15T14:01:00Z",
            qty=1,
            purchase_id="GRP-77",
            event_kind="line",
            ingest_seq=2,
            cardholder_name="Ada Fleet",
        ),
        landing_row(
            "TX-102",
            4599,
            "DSL",
            "2026-09-15T14:03:00Z",
            qty=12.4,
            purchase_id="GRP-77",
            event_kind="completion",
            ingest_seq=3,
            cardholder_name="Ada Fleet",
        ),
        landing_row(
            "TX-103",
            -500,
            "WASH",
            "2026-09-15T14:04:00Z",
            qty=1,
            auth_cd="R",
            purchase_id="GRP-77",
            event_kind="reversal",
            ingest_seq=4,
            cardholder_name="Ada Fleet",
        ),
    ]


def planted_bad_group():
    return [
        landing_row(
            "TX-200",
            1000,
            "DSL",
            "2026-09-16T10:00:00Z",
            purchase_id="GRP-88",
            event_kind="line",
            ingest_seq=1,
            cardholder_name="Bea Miles",
        ),
        landing_row(
            "TX-201",
            1000,
            "NOPE",
            "2026-09-16T10:05:00Z",
            purchase_id="GRP-88",
            event_kind="line",
            ingest_seq=2,
            cardholder_name="Bea Miles",
        ),
    ]
