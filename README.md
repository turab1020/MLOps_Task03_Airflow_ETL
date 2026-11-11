# Currency Exchange-Rate ETL with Apache Airflow (Astro CLI) → MongoDB

MLOps Task 3 · 23L-0995

A daily Airflow pipeline, built with the **Astro CLI**, that pulls currency exchange rates from the
**Frankfurter API**, reshapes them into a fixed document schema and loads them into **MongoDB**.
**Mongo Express** runs beside it as a web admin UI. The three tasks pass data only through **named XComs**.

```
 Frankfurter API ──► extract_exchange_rates ──► transform_exchange_rates ──► load_exchange_rates ──► MongoDB
 (api.frankfurter.dev)   XCom key:                  XCom key:                    XCom key:            currency_db
                         raw_exchange_rates         transformed_exchange_rates   load_summary         .exchange_rates
                                                                                                        ▲
                                                                                  Mongo Express ────────┘
                                                                                  (localhost:8081)
```

---

## Contents

1. [Project layout](#project-layout)
2. [Services & logins](#services--logins)
3. [Step-by-step run guide (with screenshot points)](#step-by-step-run-guide)
4. [Design notes](#design-notes)

---

## Project layout

```
currency-etl-airflow/
├── .astro/config.yaml                 # Astro project name
├── Dockerfile                         # Astro Runtime 3.3-9 (Airflow 3.3) + image LABELs
├── requirements.txt                   # mongo provider, pymongo, requests
├── docker-compose.override.yml        # MongoDB + Mongo Express services, networks, volumes
├── airflow_settings.yaml              # Airflow connection "mongo_default" (auto-loaded locally)
├── dags/currency_exchange_etl.py      # the DAG: 3 tasks linked by named XComs
├── include/currency_etl/transform.py  # pure transform + validation logic
└── tests/                             # unit tests + DAG integrity tests
```

Transformed document schema:

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

## Services & logins

| Service       | URL                    | Login                                                  |
|---------------|------------------------|--------------------------------------------------------|
| Airflow UI    | the URL `astro dev start` prints, e.g. http://currency-etl-airflow.localhost:6563 | none locally (or admin / admin if asked) |
| Mongo Express | http://127.0.0.1:8081  | admin / admin123                                       |
| MongoDB       | localhost:27017        | mongo_admin / mongo_pass_23L0995 (authSource = admin)  |

---

## Step-by-step run guide

**How to use this guide**

- Every grey box is **one step**: hover over it and click the **copy button** in its top-right corner, then paste it into Terminal and press Enter.
- 📂 **Run in** tells you which folder the command runs in. Every box that needs a folder starts with its own `cd`, so you can copy any box on its own.
- 📸 **Screenshot Sx** means: take the screenshot **right after that command finishes** (`Cmd + Shift + 4`, then drag over the Terminal or browser window). The number matches the **[Sx]** slot in the report.
- ✅ marks screenshots that are **already in the report** from the sandbox run. Retaking them on your Mac is optional.

> **Project folder on this Mac:** `~/Downloads/FAST/MLOps/TASK03/currency-etl-airflow`

---

### Phase 0 — Prerequisites (one-time)

Docker Desktop must be **open and running** (whale icon in the menu bar) before you start.

📂 **Run in:** any folder

```bash
brew install astro
```

📂 **Run in:** any folder

```bash
clear && astro version && docker --version && docker compose version
```

📸 **Screenshot S1**: the version output of the command above.

---

### Phase 1 — Open the project & show the structure

📂 **Run in:** `TASK03/currency-etl-airflow`

```bash
cd ~/Downloads/FAST/MLOps/TASK03/currency-etl-airflow && clear && find . -not -path "./.git*" -not -path "*/__pycache__*" | sort
```

📸 **Screenshot S2** ✅: one screenshot of this file list. You do **not** screenshot each file in it.

**Code screenshots (S3–S8) ✅ already in the report, so this is optional.** Only these 6 slots need code screenshots, not every file. To retake them in your own editor, open the project in VS Code:

📂 **Run in:** `TASK03/currency-etl-airflow`

```bash
cd ~/Downloads/FAST/MLOps/TASK03/currency-etl-airflow && code .
```

If it says `command not found: code`, use this instead (opens the VS Code app directly):

```bash
cd ~/Downloads/FAST/MLOps/TASK03/currency-etl-airflow && open -a "Visual Studio Code" .
```

| Slot | File | What to show |
|------|------|--------------|
| S3 | `dags/currency_exchange_etl.py` | `extract_exchange_rates` task (named `xcom_push`) |
| S4 | `dags/currency_exchange_etl.py` | `transform_exchange_rates` task (named `xcom_pull` / `xcom_push`) |
| S5 | `dags/currency_exchange_etl.py` | `load_exchange_rates` task (MongoHook + upsert) |
| S6 | `include/currency_etl/transform.py` | `transform_rate_row()` |
| S7 | `docker-compose.override.yml` | services, networks, volumes, labels |
| S8 | `Dockerfile`, `requirements.txt`, `airflow_settings.yaml` | all three |

---

### Phase 2 — Start everything with the Astro CLI

📂 **Run in:** `TASK03/currency-etl-airflow` (the folder that has the `Dockerfile`)

```bash
cd ~/Downloads/FAST/MLOps/TASK03/currency-etl-airflow && clear && astro dev start
```

The first run downloads images and takes a few minutes. Wait until it prints the Airflow UI URL and login.

📸 **Screenshot S9**: the end of the output (URL + credentials).

📂 **Run in:** any folder

```bash
clear && docker ps --format "table {{.Names}}\t{{.Image}}\t{{.Status}}\t{{.Ports}}"
```

📸 **Screenshot S10**: all containers `Up`: the Airflow components, `currency_mongodb` **(healthy)** and `currency_mongo_express`.

---

### Phase 3 — Docker networking & persistent storage

📂 **Run in:** any folder

```bash
clear && docker network ls --filter name=airflow --filter name=mongo_net && echo && echo "currency_mongodb networks:" && docker inspect currency_mongodb --format '{{range $k,$v := .NetworkSettings.Networks}}  {{$k}}{{println}}{{end}}' && echo "currency_mongo_express networks:" && docker inspect currency_mongo_express --format '{{range $k,$v := .NetworkSettings.Networks}}  {{$k}}{{println}}{{end}}'
```

📸 **Screenshot S11**: MongoDB is on **both** the `…_airflow` and `…_mongo_net` networks; Mongo Express is **only** on `…_mongo_net`.

📂 **Run in:** any folder

```bash
clear && docker volume ls --filter name=mongo && echo && echo "Mounts:" && docker inspect currency_mongodb --format '{{range .Mounts}}  {{.Name}} -> {{.Destination}}{{println}}{{end}}' && echo "Labels:" && docker inspect currency_mongodb --format '{{json .Config.Labels}}' | python3 -m json.tool | grep com.mlops
```

📸 **Screenshot S12**: the named volumes `mongo_data` → `/data/db` and `mongo_config` → `/data/configdb`, plus the `com.mlops.*` labels.

---

### Phase 4 — Run the DAG (browser)

1. Open the **Airflow UI URL printed by `astro dev start`** (e.g. http://currency-etl-airflow.localhost:6563). Log in with admin / admin only if it asks.
2. Find **currency_exchange_etl** and switch its toggle **on** (unpause).

📸 **Screenshot S13** ✅: the DAG list showing `currency_exchange_etl`, its tags and its schedule.

3. Open the DAG and click **Trigger** (keep the default params). Wait until all 3 tasks are green.
4. Click the run, then the **Graph** icon.

📸 **Screenshot S14** ✅: Graph view, `extract → transform → load`, all **success**.

5. Click each task → **Logs** tab:

📸 **Screenshot S15** ✅: `extract_exchange_rates` log (`GET https://api.frankfurter.dev/v2/rates?... -> HTTP 200, 8 rates`)
📸 **Screenshot S16** ✅: `transform_exchange_rates` log (one `Transformed: {...}` line per pair)
📸 **Screenshot S17** ✅: `load_exchange_rates` log (`MongoDB load summary: {...}`)

6. Click each task → **Storage** tab → **XCom** (expand the value):

📸 **Screenshot S18** ✅: key `raw_exchange_rates`
📸 **Screenshot S19** ✅: key `transformed_exchange_rates`
📸 **Screenshot S20** ✅: key `load_summary`

Optional: trigger from Terminal instead of the button.

📂 **Run in:** `TASK03/currency-etl-airflow` (`astro dev run` must run inside the project)

```bash
cd ~/Downloads/FAST/MLOps/TASK03/currency-etl-airflow && astro dev run dags trigger currency_exchange_etl
```

---

### Phase 5 — Verify the data in MongoDB

**Mongo Express (browser):** open it in Safari (Chrome extensions can block it with `ERR_TOO_MANY_RETRIES`), then log in with admin / admin123:

📂 **Run in:** any folder

```bash
open -a Safari http://127.0.0.1:8081
```

📸 **Screenshot S21** ✅: the database list showing `currency_db`.

Click `currency_db` → `exchange_rates`.

📸 **Screenshot S22** ✅: the documents with all six fields visible.

**mongosh (Terminal):**

📂 **Run in:** any folder

```bash
clear && docker exec -it currency_mongodb mongosh -u mongo_admin -p mongo_pass_23L0995 --authenticationDatabase admin currency_db --quiet --eval 'db.exchange_rates.find({}, {_id:0}).sort({target_currency:1}).toArray()'
```

📸 **Screenshot S23** ✅: the documents in the required schema.

---

### Phase 6 — Idempotency & persistence

**Idempotency:** run the DAG a second time, then count the documents.

📂 **Run in:** `TASK03/currency-etl-airflow`

```bash
cd ~/Downloads/FAST/MLOps/TASK03/currency-etl-airflow && astro dev run dags trigger currency_exchange_etl
```

Wait about 30 seconds (or until the new run is green in the UI), then:

📂 **Run in:** any folder

```bash
clear && docker exec currency_mongodb mongosh -u mongo_admin -p mongo_pass_23L0995 --authenticationDatabase admin currency_db --quiet --eval 'print("documents:", db.exchange_rates.countDocuments()); printjson(db.exchange_rates.getIndexes().map(i => ({name: i.name, unique: !!i.unique})))'
```

📸 **Screenshot S24** ✅: still **8** documents after 2 runs, with the unique index `uniq_pair_per_day`.

**Persistence:** restart the whole stack, then count again.

📂 **Run in:** `TASK03/currency-etl-airflow`

```bash
cd ~/Downloads/FAST/MLOps/TASK03/currency-etl-airflow && astro dev restart
```

📂 **Run in:** any folder

```bash
clear && docker exec currency_mongodb mongosh -u mongo_admin -p mongo_pass_23L0995 --authenticationDatabase admin currency_db --quiet --eval 'print("documents after restart:", db.exchange_rates.countDocuments())'
```

📸 **Screenshot S25** ✅: the data survived the restart (the `mongo_data` volume).

---

### Phase 7 — Tests

📂 **Run in:** `TASK03/currency-etl-airflow`

```bash
cd ~/Downloads/FAST/MLOps/TASK03/currency-etl-airflow && clear && astro dev pytest
```

📸 **Screenshot S26** ✅: **11 passed**.

---

### Phase 8 — Push to GitHub

The repo is already committed locally (author: Turab Haider). First create an **empty** repo on GitHub named
`MLOps_Task03_Airflow_ETL`, with no README, .gitignore or license. Then:

📂 **Run in:** `TASK03/currency-etl-airflow`

```bash
cd ~/Downloads/FAST/MLOps/TASK03/currency-etl-airflow && git remote add origin https://github.com/turab1020/MLOps_Task03_Airflow_ETL.git && git push -u origin main
```

If it says `remote origin already exists`, run this instead:

```bash
cd ~/Downloads/FAST/MLOps/TASK03/currency-etl-airflow && git remote set-url origin https://github.com/turab1020/MLOps_Task03_Airflow_ETL.git && git push -u origin main
```

📸 **Screenshot S27**: the GitHub repo page. Paste the repo link into the report.

---

### Shut down

📂 **Run in:** `TASK03/currency-etl-airflow`

```bash
cd ~/Downloads/FAST/MLOps/TASK03/currency-etl-airflow && astro dev stop
```

`astro dev stop` keeps the MongoDB volume (data persists). `astro dev kill` also removes the Airflow metadata DB.

### Troubleshooting

| Problem | Fix |
|---------|-----|
| `Cannot connect to the Docker daemon` | Open Docker Desktop and wait until it says *Running*. |
| `port 8081 / 27017 is already allocated` | Stop the other app using that port, or run `docker ps` and stop the old container. |
| `astro: command not found` | Run `brew install astro`, then open a new Terminal window. |
| `Error: not an Astro project` | You are in the wrong folder: run the `cd ~/Downloads/FAST/MLOps/TASK03/currency-etl-airflow` part first. |
| Chrome shows `ERR_TOO_MANY_RETRIES` on port 8081 | A Chrome extension is blocking it: use Safari or an Incognito window at http://127.0.0.1:8081. |
| Mongo Express shows *waiting for MongoDB* | Wait ~20 s for the MongoDB healthcheck, then refresh. |

---

## Design notes

* **Named XComs**: each task calls `ti.xcom_push(key=...)` and `ti.xcom_pull(task_ids=..., key=...)` with an explicit key; nothing travels on the implicit `return_value` key.
* **Docker networking**: MongoDB joins Astro's `airflow` network, so the scheduler reaches it at host `mongodb`. MongoDB and Mongo Express also share a private `mongo_net` bridge network, and Mongo Express is not on the Airflow network.
* **Persistent storage**: the named volumes `mongo_data` (`/data/db`) and `mongo_config` (`/data/configdb`) keep data through `astro dev stop` / `restart`.
* **Idempotent load**: a unique index on `(base_currency, target_currency, rate_date)` plus `ReplaceOne(..., upsert=True)`, so re-running a day updates rows instead of duplicating them.
* **Reliability**: 3 retries with exponential back-off, HTTP errors fail the task, and every record is validated (3-letter codes, positive rate, ISO dates) before loading.
* **Configurable API URL**: `FRANKFURTER_API_URL` (env var) overrides the default `https://api.frankfurter.dev/v2/rates`, for example to point at a mirror.

> The credentials are local development values only. Use a secrets backend in production.
