# Flight Price Analysis Pipeline

An Airflow pipeline that loads Bangladesh flight price data, checks it for
problems, computes KPIs, and publishes clean results to Postgres for
analysis.

## Architecture

```
CSV file
   |
   v
[ingest]  -> MySQL staging (stg_flight_prices) - raw copy, append-only
   |
   v
[validate] -> flags bad rows (stg_validation_results, stg_quality_alerts)
   |          nothing deleted, just flagged
   v
[transform] -> recomputes Total Fare, computes KPIs
   |           saves result to disk (data/transformed/{batch_id}.pkl)
   v
[load] -> Postgres (fact_flight_prices + KPI tables)
```

Two databases, two jobs:

| Database | Role |
|---|---|
| `mysql-staging` | Raw landing zone. Unvalidated copy of the source data. |
| `postgres-analytics` | Published, trustworthy tables - what gets queried for analysis. |

Everything is **append-only, batch-tracked**. Every pipeline run gets a
`batch_id`; nothing is ever overwritten. This mirrors how a real recurring
data source would work, even though the current source is one fixed CSV.

## Airflow DAG

One DAG: `flight_price_elt_dag`, scheduled `@daily` (fares change as
departure approaches, so a daily snapshot makes sense). 4 tasks, run in
order:

| Task | Does | Reads | Writes |
|---|---|---|---|
| `ingest` | Loads the CSV into MySQL | CSV file | `stg_flight_prices`, `ingestion_runs` |
| `validate` | Flags bad rows, doesn't delete any | `stg_flight_prices` | `stg_validation_results`, `stg_quality_alerts` |
| `transform` | Recomputes Total Fare, computes KPIs | valid rows only | a file on disk (handoff to `load`) |
| `load` | Writes clean data + KPIs to Postgres | that file | `fact_flight_prices`, KPI tables, `load_runs` |

`batch_id` is generated once by `ingest` and passed to every task after it,
so they all act on the exact same batch. `transform` and `load` hand data
off through a file, not Airflow's XCom - XCom is for small metadata, not
tens of thousands of rows.

**Failure alerts**: any task failure sends an email (via a local MailHog
mail-catcher for testing - swap in a real SMTP provider for production).

## KPIs

| KPI | Definition | Table |
|---|---|---|
| Average Fare by Airline | Mean Total Fare, grouped by airline | `kpi_avg_fare_by_airline` |
| Booking Count by Airline | Row count per airline | same table, `booking_count` column |
| Seasonal Fare Variation | Avg fare per season, flagged peak vs. non-peak (Eid/Winter Holidays/Hajj = peak, Regular = not) | `kpi_seasonal_fare` |
| Most Popular Routes | Top 10 source-destination pairs by booking count | `kpi_popular_routes` |

Total Fare is always recomputed as `Base Fare + Tax & Surcharge` before any
KPI is calculated - the source data's stored value isn't trusted (see
Challenges below).

## Data quality

Validation checks: missing required fields, negative fares, invalid or
duplicate route codes, non-positive duration. Bad rows are **flagged, not
deleted** - staging stays a faithful copy of the source. If a batch is more
than 5% invalid, it's flagged in `stg_quality_alerts` for a human to check.

## How to run

```
docker compose up -d
```

Open `http://localhost:8080` (`admin` / `admin`), find `flight_price_elt_dag`,
and either wait for its daily run or trigger it manually. Failure emails
show up at `http://localhost:8025`.

To run pieces standalone (no Airflow) for testing:

```
python -m src.ingestion.ingest_to_mysql
python -m src.validation.validate_staging
python -m src.transform.compute_kpis
python -m src.loading.load_to_postgres
```

## Tests

14 unit tests, pure pandas logic, no DB needed:

```
python -m pytest tests/ -v
```

| File | Covers |
|---|---|
| `tests/test_validation.py` | each validation check (missing fields, negative fares, invalid routes, bad duration) |
| `tests/test_transform.py` | Total Fare recompute, all 4 KPI calculations |

**CI** (`.github/workflows/ci.yml`) runs on every push, 3 jobs:

| Job | What it checks |
|---|---|
| `unit-tests` | the 14 tests above |
| `dag-integrity` | the DAG actually parses, no import errors |
| `integration` | the real pipeline (ingest → validate → transform → load) against live MySQL/Postgres containers, using a small fixture CSV (`tests/fixtures/sample_flights.csv`) instead of the real dataset |

## Repo structure

```
flight-price-airflow/
├── dags/
│   └── flight_price_elt_dag.py     # the DAG - 4 tasks, wires everything together
│
├── src/
│   ├── ingestion/
│   │   └── ingest_to_mysql.py      # CSV -> MySQL staging
│   ├── validation/
│   │   └── validate_staging.py     # flags bad rows, quality alerts
│   ├── transform/
│   │   └── compute_kpis.py         # recomputes Total Fare, computes KPIs
│   ├── loading/
│   │   └── load_to_postgres.py     # writes results to Postgres
│   └── utils/
│       └── settings.py             # all env var / DB config, one place
│
├── sql/
│   ├── mysql/mysql.sql             # staging schema
│   └── postgres/postgres.sql       # analytics schema
│
├── tests/
│   ├── test_validation.py          # unit tests
│   ├── test_transform.py           # unit tests
│   └── fixtures/sample_flights.csv # small CSV used by CI's integration test
│
├── script/
│   └── download_dataset.py         # pulls the dataset from Kaggle
│
├── notebooks/                      # exploratory analysis, not part of the pipeline
│
├── .github/workflows/ci.yml        # tests + integration run on every push
├── docker-compose.yml              # all services: MySQL, Postgres, Airflow, MailHog
├── Dockerfile.airflow              # custom Airflow image (adds our deps)
├── requirements.txt                # runtime deps
└── requirements-dev.txt            # + notebook/EDA/test tooling
```

## Challenges

| Challenge | Resolution |
|---|---|
| A validation check flagged 4% of rows as "invalid" because their stored Total Fare didn't match Base + Tax | Realized this wasn't bad data, just a stale derived field - moved the fix into `transform` (always recompute) instead of rejecting rows in `validate` |
| Re-running a Postgres load for the same batch failed with a duplicate-key error | The audit table needs to support legitimate retries of the same batch, unlike MySQL ingestion which always mints a new batch - switched to an upsert |
| Testing failure-alert emails needs a real triggered run - `airflow tasks test` silently skips alert callbacks | Used a throwaway test DAG triggered for real through the scheduler, confirmed the email arrived, then deleted it |

## Tech stack

Airflow (LocalExecutor) - MySQL 8.4 - PostgreSQL 16 - Python (pandas,
SQLAlchemy) - Docker Compose - MailHog (failure alerts) - pytest + GitHub
Actions (CI)
