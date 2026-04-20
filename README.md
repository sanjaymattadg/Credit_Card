# credit-card-transactions-lake

**PBVI Profile:** DATA_ACCELERATOR  
**Status:** In Progress — Session 1

A Medallion architecture data lake (Bronze → Silver → Gold) that ingests daily credit card transaction CSV extracts, enforces quality rules at each layer boundary, and produces analyst-facing Gold aggregations queryable via DuckDB. Implemented as a Docker Compose pipeline using dbt-core 1.7.x with dbt-duckdb adapter and pipeline.py as the orchestration entry point.

---

## Quick Start

```bash
# Historical load
docker compose run --rm pipeline python pipeline.py historical --start-date 2024-01-01 --end-date 2024-01-07

# Incremental load
docker compose run --rm pipeline python pipeline.py incremental
```

---

## Stack

| Concern | Choice |
|---|---|
| Container | Docker Compose |
| Base image | `python:3.11-slim` |
| Transformation | `dbt-core==1.7.*` + `dbt-duckdb==1.7.*` |
| Query engine | DuckDB (embedded, no server) |
| Storage format | Parquet on local filesystem |
| Source data | Read-only CSV files |

---

## Repository Structure

```
credit-card-transactions-lake/
├── pipeline.py                   # Orchestration entry point
├── Dockerfile
├── docker-compose.yml
├── dbt_project/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/
│       ├── silver/
│       │   ├── silver_transactions.sql
│       │   ├── silver_accounts.sql
│       │   ├── silver_transaction_codes.sql
│       │   └── silver_quarantine.sql
│       └── gold/
│           ├── gold_daily_summary.sql
│           └── gold_weekly_account_summary.sql
├── source/                       # Read-only CSV input files
├── data/                         # Generated output (gitignored)
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   └── pipeline/
├── brief/
├── docs/
├── sessions/
├── verification/
├── discovery/
└── enhancements/
```

---

## Data Layer Paths

| Layer | Entity | Path |
|---|---|---|
| Bronze | Transactions | `data/bronze/transactions/date=YYYY-MM-DD/data.parquet` |
| Bronze | Accounts | `data/bronze/accounts/date=YYYY-MM-DD/data.parquet` |
| Bronze | Transaction Codes | `data/bronze/transaction_codes/data.parquet` |
| Silver | Transactions | `data/silver/transactions/date=YYYY-MM-DD/data.parquet` |
| Silver | Accounts | `data/silver/accounts/data.parquet` |
| Silver | Transaction Codes | `data/silver/transaction_codes/data.parquet` |
| Silver | Quarantine | `data/silver/quarantine/date=YYYY-MM-DD/rejected.parquet` |
| Gold | Daily Summary | `data/gold/daily_summary/data.parquet` |
| Gold | Weekly Account Summary | `data/gold/weekly_account_summary/data.parquet` |
| Pipeline | Control Table | `data/pipeline/control.parquet` |
| Pipeline | Run Log | `data/pipeline/run_log.parquet` |

---

## Key Invariants

- **INV-03:** Every Bronze record has non-null `_source_file`, `_ingested_at`, `_pipeline_run_id`.
- **INV-06:** Every Bronze record routes to exactly Silver or Quarantine — no record exits without an outcome.
- **INV-35:** Each pipeline invocation has a unique UUID `run_id` generated before any model executes.

See `Documents/INVARIANTS.MD` for the full invariant specification.
