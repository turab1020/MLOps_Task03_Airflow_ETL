"""
Currency Exchange-Rate ETL  (MLOps Task 3)
==========================================

Extract   -> daily exchange rates from the Frankfurter API (https://api.frankfurter.dev)
Transform -> one flat, validated document per currency pair
Load      -> upsert into MongoDB  (database: currency_db, collection: exchange_rates)

Data moves between the three tasks ONLY through *named* XComs: every push and
pull uses an explicit `key=` instead of the implicit `return_value` key.

    extract_exchange_rates   --push-->  key="raw_exchange_rates"
    transform_exchange_rates <--pull--  key="raw_exchange_rates"
                             --push-->  key="transformed_exchange_rates"
    load_exchange_rates      <--pull--  key="transformed_exchange_rates"
                             --push-->  key="load_summary"
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone

import pendulum
import requests

try:  # Airflow 3.x (Astro Runtime 3.x)
    from airflow.sdk import Param, dag, task
except ImportError:  # Airflow 2.x (Astro Runtime 12/13) fallback
    from airflow.decorators import dag, task
    from airflow.models.param import Param

from include.currency_etl.transform import REQUIRED_FIELDS, transform_payload

log = logging.getLogger(__name__)

# ----------------------------------------------------------------------------
# Configuration
# ----------------------------------------------------------------------------
# Overridable via env var (e.g. to point at a mirror); defaults to the public API.
FRANKFURTER_URL = os.getenv("FRANKFURTER_API_URL", "https://api.frankfurter.dev/v2/rates")
MONGO_CONN_ID = "mongo_default"          # defined in airflow_settings.yaml
MONGO_DB = "currency_db"
MONGO_COLLECTION = "exchange_rates"

# Named XCom keys (the contract between tasks)
XCOM_RAW = "raw_exchange_rates"
XCOM_TRANSFORMED = "transformed_exchange_rates"
XCOM_SUMMARY = "load_summary"

TASK_EXTRACT = "extract_exchange_rates"
TASK_TRANSFORM = "transform_exchange_rates"
TASK_LOAD = "load_exchange_rates"

default_args = {
    "owner": "23L0995",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
}


@dag(
    dag_id="currency_exchange_etl",
    description="Daily Frankfurter exchange rates -> transform -> MongoDB",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["mlops", "etl", "frankfurter", "mongodb", "task3"],
    params={
        "base_currency": Param("EUR", type="string", minLength=3, maxLength=3),
        "quote_currencies": Param(
            ["USD", "GBP", "JPY", "PKR", "CHF", "CAD", "AUD", "CNY"],
            type="array",
            description="Target currencies to fetch against the base currency",
        ),
    },
    doc_md=__doc__,
)
def currency_exchange_etl():

    # ------------------------------------------------------------------ E ---
    @task(task_id=TASK_EXTRACT)
    def extract_exchange_rates(**context) -> None:
        """Call the Frankfurter v2 API and push the raw JSON as a named XCom."""
        params = context["params"]
        base = params["base_currency"].upper()
        quotes = ",".join(q.upper() for q in params["quote_currencies"] if q.upper() != base)

        response = requests.get(
            FRANKFURTER_URL,
            params={"base": base, "quotes": quotes},
            headers={"Accept": "application/json"},
            timeout=30,
        )
        response.raise_for_status()  # non-2xx -> task fails -> Airflow retries
        payload = response.json()
        retrieved_at = datetime.now(timezone.utc).replace(tzinfo=None).isoformat(timespec="seconds")

        n_rows = len(payload) if isinstance(payload, list) else len(payload.get("rates", {}))
        if n_rows == 0:
            raise ValueError("Frankfurter API returned no rates")

        log.info("GET %s -> HTTP %s, %d rates", response.url, response.status_code, n_rows)

        context["ti"].xcom_push(
            key=XCOM_RAW,
            value={
                "base": base,
                "request_url": response.url,
                "retrieved_at": retrieved_at,
                "payload": payload,
            },
        )

    # ------------------------------------------------------------------ T ---
    @task(task_id=TASK_TRANSFORM)
    def transform_exchange_rates(**context) -> None:
        """Pull the raw XCom, reshape each pair into the required schema, push it."""
        ti = context["ti"]
        raw = ti.xcom_pull(task_ids=TASK_EXTRACT, key=XCOM_RAW)
        if not raw:
            raise ValueError(f"No XCom found for key '{XCOM_RAW}' from task '{TASK_EXTRACT}'")

        records = transform_payload(raw["payload"], raw["base"], raw["retrieved_at"])

        for record in records:  # final schema check
            missing = [f for f in REQUIRED_FIELDS if f not in record]
            if missing:
                raise ValueError(f"Record {record} missing fields {missing}")
            log.info("Transformed: %s", record)

        log.info("Transformed %d exchange-rate records", len(records))
        ti.xcom_push(key=XCOM_TRANSFORMED, value=records)

    # ------------------------------------------------------------------ L ---
    @task(task_id=TASK_LOAD)
    def load_exchange_rates(**context) -> None:
        """Pull the transformed XCom and upsert it into MongoDB."""
        from airflow.providers.mongo.hooks.mongo import MongoHook
        from pymongo import ASCENDING, ReplaceOne

        ti = context["ti"]
        records = ti.xcom_pull(task_ids=TASK_TRANSFORM, key=XCOM_TRANSFORMED)
        if not records:
            raise ValueError(f"No XCom found for key '{XCOM_TRANSFORMED}' from task '{TASK_TRANSFORM}'")

        hook = MongoHook(mongo_conn_id=MONGO_CONN_ID)
        client = hook.get_conn()
        try:
            collection = client[MONGO_DB][MONGO_COLLECTION]

            # One document per (base, target, date): re-running a day never duplicates rows.
            collection.create_index(
                [("base_currency", ASCENDING), ("target_currency", ASCENDING), ("rate_date", ASCENDING)],
                unique=True,
                name="uniq_pair_per_day",
            )

            operations = [
                ReplaceOne(
                    {
                        "base_currency": r["base_currency"],
                        "target_currency": r["target_currency"],
                        "rate_date": r["rate_date"],
                    },
                    r,
                    upsert=True,
                )
                for r in records
            ]
            result = collection.bulk_write(operations, ordered=False)

            summary = {
                "database": MONGO_DB,
                "collection": MONGO_COLLECTION,
                "records_received": len(records),
                "inserted": result.upserted_count,
                "updated": result.modified_count,
                "unchanged": result.matched_count - result.modified_count,
                "total_documents_in_collection": collection.count_documents({}),
            }
        finally:
            client.close()

        log.info("MongoDB load summary: %s", summary)
        ti.xcom_push(key=XCOM_SUMMARY, value=summary)

    # Data flows through named XComs, so the order is declared explicitly.
    extract_exchange_rates() >> transform_exchange_rates() >> load_exchange_rates()


currency_exchange_etl()
