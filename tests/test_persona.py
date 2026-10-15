import json

from src.catalog.mask import present


def test_t9_persona_masks():
    row = {
        "cardholder_name": "Ada Fleet",
        "card_token": "tok_abcdef1234567890",
        "pan_last4": "1234",
        "amount": 45.99,
    }
    analyst = present(row, "business_analyst")
    manager = present(row, "product_manager")
    steward = present(row, "data_steward")
    engineer = present(row, "data_engineer")

    assert analyst["cardholder_name"] != "Ada Fleet"
    assert analyst["cardholder_name"].startswith("A")
    assert "card_token" not in analyst
    assert "pan_last4" not in analyst
    assert "amount" not in analyst

    assert "cardholder_name" not in manager
    assert "card_token" not in manager
    assert "pan_last4" not in manager

    assert steward["cardholder_name"] != "Ada Fleet"
    assert steward["pan_last4"] == "1234"
    assert "card_token" not in steward
    assert steward["classifications"]["pan_last4"] == "PCI"
    assert steward["classifications"]["cardholder_name"] == "PII_DIRECT"

    assert engineer["card_token"] == "tok_abcdef1234567890"
    assert "4111111111111234" not in json.dumps(engineer)
