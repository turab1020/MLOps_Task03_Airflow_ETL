"""
Pure-Python helpers for the currency ETL pipeline.

Kept separate from the DAG file so they can be unit-tested without a running
Airflow scheduler (tests/test_transform.py).
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

SOURCE_NAME = "Frankfurter API"

# Keys every transformed record must contain (the schema given in the task).
REQUIRED_FIELDS = (
    "base_currency",
    "target_currency",
    "exchange_rate",
    "rate_date",
    "source",
    "retrieved_at",
)


def normalise_api_rows(payload: Any, base: str) -> list[dict]:
    """
    Turn a Frankfurter response into a flat list of
    {"date", "base", "quote", "rate"} rows.

    v2 (/v2/rates) returns a JSON array of rows:
        [{"date": "2026-11-09", "base": "EUR", "quote": "USD", "rate": 1.1196}, ...]
    v1 (/v1/latest) returned one object with a nested "rates" dict:
        {"base": "EUR", "date": "2026-11-09", "rates": {"USD": 1.1196, ...}}
    Both shapes are accepted so the pipeline keeps working if the endpoint changes.
    """
    if isinstance(payload, list):
        return [dict(row) for row in payload]

    if isinstance(payload, dict) and isinstance(payload.get("rates"), dict):
        return [
            {
                "date": payload.get("date"),
                "base": payload.get("base", base),
                "quote": quote,
                "rate": rate,
            }
            for quote, rate in payload["rates"].items()
        ]

    raise ValueError(f"Unexpected Frankfurter response shape: {type(payload).__name__}")


def transform_rate_row(row: dict, retrieved_at: str) -> dict:
    """Map one raw API row onto the target document schema and validate it."""
    base = str(row["base"]).strip().upper()
    target = str(row["quote"]).strip().upper()
    rate = float(row["rate"])
    rate_date = str(row["date"])

    if len(base) != 3 or len(target) != 3:
        raise ValueError(f"Invalid ISO-4217 currency code in row: {row}")
    if base == target:
        raise ValueError(f"Base and target currency are identical: {row}")
    if rate <= 0:
        raise ValueError(f"Exchange rate must be positive: {row}")
    date.fromisoformat(rate_date)  # raises ValueError if not YYYY-MM-DD
    datetime.fromisoformat(retrieved_at)  # same check for the timestamp

    return {
        "base_currency": base,
        "target_currency": target,
        "exchange_rate": round(rate, 6),
        "rate_date": rate_date,
        "source": SOURCE_NAME,
        "retrieved_at": retrieved_at,
    }


def transform_payload(payload: Any, base: str, retrieved_at: str) -> list[dict]:
    """Full transform step: normalise -> map -> validate -> de-duplicate."""
    records: dict[tuple, dict] = {}
    for row in normalise_api_rows(payload, base):
        record = transform_rate_row(row, retrieved_at)
        key = (record["base_currency"], record["target_currency"], record["rate_date"])
        records[key] = record  # last one wins if the API ever repeats a pair
    return sorted(records.values(), key=lambda r: r["target_currency"])
