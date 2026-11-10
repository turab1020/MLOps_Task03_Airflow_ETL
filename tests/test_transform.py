"""Unit tests for the pure transform helpers (run with `astro dev pytest`)."""
import pytest

from include.currency_etl.transform import REQUIRED_FIELDS, transform_payload

RETRIEVED_AT = "2026-11-05T07:15:00"

V2_PAYLOAD = [
    {"date": "2026-11-04", "base": "EUR", "quote": "USD", "rate": 1.15},
    {"date": "2026-11-04", "base": "EUR", "quote": "GBP", "rate": 0.8346},
    {"date": "2026-11-04", "base": "EUR", "quote": "JPY", "rate": 177.2},
]


def test_v2_payload_matches_required_schema():
    records = transform_payload(V2_PAYLOAD, "EUR", RETRIEVED_AT)
    assert len(records) == 3
    usd = next(r for r in records if r["target_currency"] == "USD")
    assert usd == {
        "base_currency": "EUR",
        "target_currency": "USD",
        "exchange_rate": 1.15,
        "rate_date": "2026-11-04",
        "source": "Frankfurter API",
        "retrieved_at": RETRIEVED_AT,
    }
    for r in records:
        assert set(r) == set(REQUIRED_FIELDS)


def test_v1_style_payload_is_supported():
    payload = {"base": "EUR", "date": "2026-11-04", "rates": {"USD": 1.15}}
    records = transform_payload(payload, "EUR", RETRIEVED_AT)
    assert records[0]["target_currency"] == "USD"


def test_duplicates_are_removed():
    records = transform_payload(V2_PAYLOAD + V2_PAYLOAD[:1], "EUR", RETRIEVED_AT)
    assert len(records) == 3


@pytest.mark.parametrize(
    "bad_row",
    [
        {"date": "2026-11-04", "base": "EUR", "quote": "USD", "rate": 0},
        {"date": "2026-11-04", "base": "EUR", "quote": "USD", "rate": -1.2},
        {"date": "04-11-2026", "base": "EUR", "quote": "USD", "rate": 1.1},
        {"date": "2026-11-04", "base": "EUR", "quote": "EUR", "rate": 1.0},
        {"date": "2026-11-04", "base": "EURO", "quote": "USD", "rate": 1.1},
    ],
)
def test_invalid_rows_are_rejected(bad_row):
    with pytest.raises(ValueError):
        transform_payload([bad_row], "EUR", RETRIEVED_AT)
