# PROJECT_MANIFEST.md — credit-card-transactions-lake

**PBVI Profile:** DATA_ACCELERATOR  
**Classification:** Training Demo System  
**Phase:** 1 — Repository Scaffold  
**Last Updated:** 2026-04-20

---

## Project Identity

| Field | Value |
|---|---|
| Project Name | credit-card-transactions-lake |
| PBVI Profile | DATA_ACCELERATOR |
| Owner | sanjay.matta@datagrokr.co |
| Repository | sanjaymattadg/Credit_Card |
| Branch Convention | `session/sNN` |

---

## System Description

A Medallion architecture batch pipeline (Bronze → Silver → Gold) that ingests daily credit card transaction CSV extracts, enforces data quality rules at each layer boundary, and produces analyst-facing Gold aggregations queryable via DuckDB. Implemented as a Docker Compose pipeline using dbt-core 1.7.x with dbt-duckdb adapter and pipeline.py as the orchestration entry point.

---

## Permitted Files (Scope Boundary)

| File | Status | Description |
|---|---|---|
| `pipeline.py` | PENDING | Orchestration entry points: `run_historical` and `run_incremental` |
| `dbt_project/models/silver/silver_transaction_codes.sql` | PENDING | Silver transaction codes loader model |
| `dbt_project/models/silver/silver_accounts.sql` | PENDING | Silver accounts upsert model |
| `dbt_project/models/silver/silver_transactions.sql` | PENDING | Silver transactions promotion model |
| `dbt_project/models/silver/silver_quarantine.sql` | PENDING | Silver quarantine write model |
| `dbt_project/models/gold/gold_daily_summary.sql` | PENDING | Gold daily summary aggregation |
| `dbt_project/models/gold/gold_weekly_account_summary.sql` | PENDING | Gold weekly account summary |
| `Dockerfile` | PENDING | Container definition |
| `docker-compose.yml` | PENDING | Service and mount configuration |
| `dbt_project/dbt_project.yml` | SCAFFOLD | dbt project configuration |
| `dbt_project/profiles.yml` | SCAFFOLD | DuckDB profile configuration |
| `scripts/` | PENDING | Verification scripts (Sessions S7–S9) |
| `README.md` | DONE | Project overview |
| `PROJECT_MANIFEST.md` | DONE | This file |

---

## Session Log

| Session | Branch | Goal | Status |
|---|---|---|---|
| S01 | `session/s01` | Project Scaffold and Environment | IN PROGRESS |

---

## Global Invariants

| ID | Description | Status |
|---|---|---|
| INV-03 | Every Bronze record has non-null `_source_file`, `_ingested_at`, `_pipeline_run_id` | PENDING — no data paths yet |
| INV-06 | Every Bronze record routes to exactly Silver or Quarantine | PENDING — no data paths yet |
| INV-35 | Each pipeline invocation has a unique UUID `run_id` | PENDING — no data paths yet |

---

## Fixed Stack

| Component | Fixed Value |
|---|---|
| Container base image | `python:3.11-slim` |
| dbt-core | `1.7.*` |
| dbt-duckdb | `1.7.*` |
| Python libraries | `duckdb`, `pandas`, `pyarrow`, `uuid`, `pathlib`, `argparse` |
| dbt profile name | `credit_card_lake` |
| dbt target | `dev` |
| dbt DuckDB path | `/app/data/dbt_duckdb.db` |
| dbt threads | `1` |
| Container service name | `pipeline` |

---

## Data Contracts

| Contract | Path | Status |
|---|---|---|
| Watermark | `pipeline/control.parquet` | PENDING |
| Run Log | `pipeline/run_log.parquet` | PENDING |
| dbt Artifact | `target/run_results.json` | PENDING |

---

*PROJECT_MANIFEST.md — v1.0 · Phase 1 · credit-card-transactions-lake · PBVI DATA_ACCELERATOR*
