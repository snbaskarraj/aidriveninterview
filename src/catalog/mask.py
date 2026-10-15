"""Persona presentation. The API masks. The UI only displays the result."""

from __future__ import annotations

import hashlib

from src.catalog.model import FIELD_CLASS, PERSONAS, POLICY


def mask_name(value) -> str:
    text = str(value or "").strip()
    if not text:
        return "REDACTED"
    return text.split()[0][0] + "***"


def mask_email(value) -> str:
    text = str(value or "")
    if "@" not in text:
        return "REDACTED"
    local, domain = text.split("@", 1)
    if not local:
        return "REDACTED"
    return local[0] + "***@" + domain


def tokenize(value) -> str:
    digest = hashlib.sha256(str(value).encode()).hexdigest()
    return "tok_" + digest[:12]


def present(record: dict, persona: str) -> dict:
    if persona not in PERSONAS:
        raise ValueError("unknown persona")
    hidden_classes = {}
    visible = {}
    for key, value in record.items():
        kind = FIELD_CLASS.get(key)
        if kind is None:
            visible[key] = value
            continue
        action = POLICY[persona][kind]
        hidden_classes[key] = kind
        if action == "clear":
            visible[key] = value
        elif action == "mask":
            if key == "email":
                visible[key] = mask_email(value)
            elif "name" in key:
                visible[key] = mask_name(value)
            else:
                visible[key] = "REDACTED"
        elif action == "last4":
            if key == "pan_last4":
                visible[key] = value
        elif action == "token":
            if key in ("card_token", "pan_last4", "ssn_token", "member_token"):
                visible[key] = value
            else:
                visible[key] = tokenize(value)
        elif action == "label":
            visible[key] = "[" + kind + "]"
        elif action in ("deny", "aggregate"):
            continue
    if persona == "data_steward":
        visible["classifications"] = hidden_classes
    return visible
