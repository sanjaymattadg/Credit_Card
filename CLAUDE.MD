# Claude.md — Execution Contract
## Credit Card Financial Transactions Lake
**Status:** FROZEN at Phase 5 · **Classification:** Training Demo System
*This document is a contract, not documentation. If something is not here, it is not permitted.*

---

## Section 1 — System Intent

This system is a Medallion architecture batch pipeline (Bronze → Silver → Gold) that ingests daily credit card transaction CSV extracts, enforces data quality rules at each layer boundary, and produces analyst-facing Gold aggregations queryable via DuckDB. It does not perform risk scoring, credit decisions, backfill of unresolvable account references, streaming ingestion, schema evolution, or serve any API layer. Success means: all analysts query the same Gold layer, every output is reproducible and traceable to its Bronze source file, and re-running on identical input produces identical output at every layer.

---

## Section 2 — Hard Invariants (GLOBAL — apply to every task, no exceptions)

**INV-03:** Every Bronze record has non-null values for `_source_file`, `_ingested_at`, and `_pipeline_run_id`. These audit columns are added by the loader before write — they are never derived from source data. This is never negotiable.

**INV-06:** Every Bronze record, across all entities (transactions and accounts), is routed to exactly one outcome: Silver or Quarantine. No record exits the Bronze read path without entering either the Silver write path or the quarantine write path. Transaction codes have no quarantine path and are not subject to this invariant. This is never negotiable.

**INV-35:** Each pipeline invocation has a unique `run_id` shared across all run log rows and all layer audit columns for that invocation. No two pipeline invocations share a `run_id`. `run_id` is a UUID generated at pipeline start before any model executes. This is never negotiable.

---

## Section 3 — Scope Boundary

**Claude Code is permitted to build only the following files and components:**

| Permitted | Description |
|---|---|
| `pipeline.py` | Orchestration entry points: `run_historical(start_date, end_date)` and `run_incremental()` |
| `dbt_project/models/silver/silver_transaction_codes.sql` | Silver transaction codes loader model |
| `dbt_project/models/silver/silver_accounts.sql` | Silver accounts upsert model |
| `dbt_project/models/silver/silver_transactions.sql` | Silver transactions promotion model |
| `dbt_project/models/silver/silver_quarantine.sql` | Silver quarantine write model |
| `dbt_project/models/gold/gold_daily_summary.sql` | Gold daily summary aggregation model |
| `dbt_project/models/gold/gold_weekly_account_summary.sql` | Gold weekly account summary model |
| `Dockerfile` | Container definition |
| `docker-compose.yml` | Service and mount configuration |
| `dbt_project/dbt_project.yml` | dbt project configuration |
| `dbt_project/profiles.yml` | DuckDB profile configuration |
| `scripts/` | Verification scripts produced in sessions S7–S9 |
| PBVI structure directories and `.gitkeep` files | Per Task 1.1 scaffold |
| `README.md`, `PROJECT_MANIFEST.md` | Standard PBVI documents |

**Claude Code must not:**
- Modify any file under `source/` — read-only bind mount.
- Create any file not listed above or registered in `PROJECT_MANIFEST.md`.
- Add a database service, API service, or web server to `docker-compose.yml`.
- Implement SCD Type 2, streaming ingestion, schema evolution, or encryption.
- Compute risk scores, credit decisions, or any business logic beyond the aggregations defined in EXECUTION_PLAN.md.
- Resolve `_is_resolvable = false` records — this requires a backfill pipeline that is out of scope.
- Implement concurrent or parallel pipeline execution.

**Conflict rule:** If a task prompt conflicts with an invariant: the invariant wins. Flag the conflict — do not resolve it silently.

---

## Section 4 — Execution Contract

- **One task at a time.** Complete and verify one task before starting the next.
- **No scope expansion.** Do not build anything not specified in the current task prompt.
- **Flag deviations — do not resolve them silently.** If the task prompt is ambiguous, incomplete, or conflicts with this contract, stop and flag it. Do not fill the gap with judgment.
- **One commit per task.** Commit message format: `[task_id] — [Task Name]: brief description`.
- **Verification before commit.** Run the task's verification command and confirm it passes before committing.
- **TASK-SCOPED invariants** are embedded inline in each task's CC prompt in EXECUTION_PLAN.md. They apply only to that task. Read them before writing any code for that task.
- **GLOBAL invariants** (Section 2 above) apply to every task without exception. They are not repeated in task prompts.

---

## Section 5 — Fixed Stack

All of the following are fixed. Do not substitute, upgrade, or add dependencies without an explicit engineer decision.

| Component | Fixed Value |
|---|---|
| Container base image | `python:3.11-slim` |
| dbt-core | `1.7.*` |
| dbt-duckdb | `1.7.*` |
| Python libraries | `duckdb`, `pandas`, `pyarrow`, `uuid` (stdlib), `pathlib` (stdlib), `argparse` (stdlib) |
| dbt profile name | `credit_card_lake` |
| dbt target | `dev` |
| dbt DuckDB path | `/app/data/dbt_duckdb.db` |
| dbt threads | `1` |
| Container service name | `pipeline` |

**Fixed path constants (pipeline.py):**

| Constant | Value |
|---|---|
| `DATA_DIR` | `/app/data` |
| `SOURCE_DIR` | `/app/source` |
| `BRONZE_DIR` | `/app/data/bronze` |
| `SILVER_DIR` | `/app/data/silver` |
| `GOLD_DIR` | `/app/data/gold` |
| `PIPELINE_DIR` | `/app/data/pipeline` |
| `DBT_PROJECT_DIR` | `/app/dbt_project` |

**Fixed bind mounts:**

| Host path | Container path | Mode |
|---|---|---|
| `./source` | `/app/source` | read-only |
| `./data` | `/app/data` | read-write |
| `./dbt_project` | `/app/dbt_project` | read-write |

**Fixed data contracts:**
- `pipeline/control.parquet` — watermark file; field: `last_processed_date`
- `pipeline/run_log.parquet` — append-only; fields: `run_id`, `model_name`, `started_at`, `completed_at`, `status` (enum: `SUCCESS`, `FAILED`, `SKIPPED`), `records_processed`, `records_written`, `records_rejected`, `error_message`
- `run_results.json` — dbt artifact at `target/run_results.json`; field used: `rows_affected` (records_written), `message` (error_message on failure)
- Quarantine rejection reason codes (enum, no others permitted): `NULL_REQUIRED_FIELD`, `INVALID_AMOUNT`, `DUPLICATE_TRANSACTION_ID`, `INVALID_TRANSACTION_CODE`, `INVALID_CHANNEL`, `INVALID_ACCOUNT_STATUS`, `INVALID_TRANSACTION_DATE`

---

*Claude.md — v1.0 · Phase 5 Frozen · Credit Card Financial Transactions Lake · PBVI Data Engineering Training Vehicle*