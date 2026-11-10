# Currency Exchange-Rate ETL with Apache Airflow (Astro CLI) → MongoDB

MLOps Task 3 · 23L0995

A daily Airflow pipeline, built with the **Astro CLI**, that pulls currency exchange
rates from the **Frankfurter API**, reshapes them into a fixed document schema and
loads them into **MongoDB**. **Mongo Express** runs beside it as a web admin UI.

```
 Frankfurter API ──► extract_exchange_rates ──► transform_exchange_rates ──► load_exchange_rates ──► MongoDB
 (api.frankfurter.dev)   XCom key:                  XCom key:                    XCom key:            currency_db
                         raw_exchange_rates         transformed_exchange_rates   load_summary         .exchange_rates
                                                                                                        ▲
                                                                                  Mongo Express ────────┘
                                                                                  (localhost:8081)
```

## Project layout

```
currency-etl-airflow/
├── .astro/config.yaml              # Astro project name
├── Dockerfile                      # Astro Runtime 3.3-9 (Airflow 3.3) + image LABELs
├── requirements.txt                # mongo provider, pymongo, requests
├── docker-compose.override.yml     # MongoDB + Mongo Express services, networks, volumes
├── airflow_settings.yaml           # Airflow connection "mongo_default" (auto-loaded locally)
├── dags/currency_exchange_etl.py   # the DAG: 3 tasks linked by named XComs
├── include/currency_etl/transform.py  # pure transform + validation logic
└── tests/                          # unit tests + DAG integrity tests
```

## Transformed document schema

```json
{
  "base_currency": "EUR",
  "target_currency": "USD",
  "exchange_rate": 1.15,
  "rate_date": "2026-11-04",
  "source": "Frankfurter API",
  "retrieved_at": "2026-11-05T07:15:00"
}
```

## Run it

```bash
astro dev start          # builds the image, starts Airflow + MongoDB + Mongo Express
```

| Service       | URL                    | Login                          |
|---------------|------------------------|--------------------------------|
| Airflow UI    | http://localhost:8080  | admin / admin (Astro default)  |
| Mongo Express | http://localhost:8081  | admin / admin123               |
| MongoDB       | localhost:27017        | mongo_admin / mongo_pass_23L0995 (authSource=admin) |

Unpause and trigger `currency_exchange_etl` in the Airflow UI, or run
`astro dev run dags trigger currency_exchange_etl`.

Tests: `astro dev pytest`

## Design notes

* **Named XComs**: each task calls `ti.xcom_push(key=...)` and `ti.xcom_pull(task_ids=..., key=...)`
  with an explicit key; nothing travels on the implicit `return_value` key.
* **Docker networking**: MongoDB joins Astro's `airflow` network, so the scheduler can reach it
  at host `mongodb`. MongoDB and Mongo Express also share a private `mongo_net` bridge network,
  and Mongo Express is not on the Airflow network at all.
* **Persistent storage**: the named volumes `mongo_data` (`/data/db`) and `mongo_config`
  (`/data/configdb`) keep the data through `astro dev stop` / `restart`.
* **Idempotent load**: a unique index on `(base_currency, target_currency, rate_date)` plus
  `ReplaceOne(..., upsert=True)` means re-running a day updates rows rather than duplicating them.
* **Reliability**: 3 retries with exponential back-off, HTTP errors fail the task, and every
  record is validated (3-letter codes, positive rate, ISO dates) before loading.

> The credentials are local development values only. Use a secrets backend in production.
