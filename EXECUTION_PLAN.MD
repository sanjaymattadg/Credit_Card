# EXECUTION_PLAN.md
## Credit Card Financial Transactions Lake
**Version:** v1.0 · **Status:** Phase 3 Complete · **Classification:** Training Demo System

---

## Changelog

| Version | Date | Author | Change |
|---|---|---|---|
| v1.0 | 2026-04-17 | Engineer | Greenfield — Initial execution plan produced at Phase 3 |

---

## Resolved Open Questions

The following open questions from ARCHITECTURE.md Section 6 are resolved here. All must be resolved before tasks reference them.

| OQ | Question | Resolution |
|---|---|---|
| OQ-01 | How does pipeline.py extract records_processed, records_written, records_rejected from dbt model execution? | dbt-duckdb 1.7.x writes `target/run_results.json` after each `dbt run`. pipeline.py reads this file after each model invocation to extract `rows_affected` (records_written). records_processed is set equal to the Bronze row count passed in by pipeline.py. records_rejected = records_processed − records_written (Silver models only). error_message is taken from `run_results.json` `message` field on failure. |
| OQ-02 | How are run log entries appended to an existing Parquet file atomically? | DuckDB does not natively support append-to-Parquet without a full rewrite. The append-only guarantee is implemented at the application layer in pipeline.py: (1) read the existing run_log.parquet into a DuckDB in-memory table if it exists, (2) union with new rows, (3) write the combined result back as a single Parquet file. The file is never truncated without first reading existing rows. This satisfies INV-30 at the application layer. |
| OQ-03 | What is the exact guard in pipeline.py when the historical pipeline is re-run and a watermark already exists? | On historical pipeline invocation, pipeline.py reads pipeline/control.parquet if it exists. If last_processed_date >= end_date, the pipeline logs a skip and exits without executing any layer. If last_processed_date is between start_date and end_date (partial prior run), the pipeline resumes from the next unprocessed date using per-date run log checks per Decision 3. The watermark is never set backwards. |
| OQ-04 | Does a no-op incremental run produce any observable output? | No. A no-op run (watermark+1 file absent) produces no run log entries, no layer writes, and no watermark change. The pipeline exits with a logged INFO message only. No observable file system changes occur. This is testable by comparing file modification timestamps before and after the no-op invocation. |

---

## Session Overview

| # | Session Name | Goal | Tasks | Invariants Touched | Estimated Duration |
|---|---|---|---|---|---|
| S1 | Project Scaffold and Environment | Running Docker environment, dbt project wired, pipeline.py stub executable | 4 | None (scaffold only) | ~2h 00m |
| S2 | Bronze Loader — Transactions and Accounts | Bronze partitions written correctly for both entities, all Bronze invariants enforced | 5 | INV-01, INV-02, INV-03 (GLOBAL), INV-04, INV-05, INV-37 | ~3h 30m |
| S3 | Bronze Loader — Transaction Codes and Run Log Infrastructure | Transaction codes Bronze, run log write logic, watermark initialisation stub | 4 | INV-01, INV-02, INV-03 (GLOBAL), INV-04, INV-30, INV-31, INV-32, INV-35 (GLOBAL) | ~3h 10m |
| S4 | Silver — Transaction Codes and Accounts | Silver reference and dimension layers, accounts upsert idempotency | 4 | INV-17, INV-18, INV-33 | ~2h 40m |
| S5 | Silver — Transactions Promotion (Validation and Quarantine) | Full transaction quality gate: null checks, amount validation, code lookup, channel check, quarantine routing | 5 | INV-06 (GLOBAL), INV-07, INV-08, INV-09, INV-11, INV-12, INV-13, INV-33 | ~3h 50m |
| S6 | Silver — Transactions Deduplication and Account Resolution | Two-step deduplication, _is_resolvable flag, conservation check | 4 | INV-06 (GLOBAL), INV-07, INV-10, INV-14, INV-15, INV-16 | ~3h 20m |
| S7 | Gold Layer — Daily Summary and Weekly Account Summary | Both Gold dbt models, full overwrite, deterministic output | 4 | INV-19, INV-20, INV-21, INV-22, INV-23, INV-24, INV-25, INV-34 | ~3h 20m |
| S8 | Pipeline Orchestration — Historical Pipeline | pipeline.py historical entry point, watermark init, per-date sequencing, run log population | 5 | INV-28, INV-29, INV-30, INV-31, INV-32, INV-35 (GLOBAL), INV-36 | ~4h 00m |
| S9 | Pipeline Orchestration — Incremental Pipeline and No-Op | Incremental entry point, watermark-gated execution, no-op detection | 4 | INV-27, INV-28, INV-29, INV-30, INV-31, INV-32, INV-35 (GLOBAL) | ~3h 20m |
| S10 | System Sign-Off Verification | Full idempotency sweep, Gold correctness checks, audit trail verification — no new code | 4 | INV-20, INV-22, INV-26, INV-34 (Phase 8 scope) | ~2h 30m |

**Total estimated duration across all sessions: ~31h 40m**
All sessions are within the 4-hour limit. Session S8 is the densest at ~4h 00m; it sits at the boundary and may be split at task 8.3 if required.

---

## GLOBAL Invariants (embed in Claude.md Section 2 — not repeated per task)

These three invariants apply to every task in every session:

**INV-03:** Every Bronze record has non-null values for _source_file, _ingested_at, and _pipeline_run_id. Bronze audit columns are added by the loader before write — they are never derived from source data.

**INV-06:** Every Bronze record, across all entities (transactions and accounts), is routed to exactly one outcome: Silver or Quarantine. No record exits the Bronze read path without entering either the Silver write path or the quarantine write path. Transaction codes have no quarantine path and are not subject to this invariant.

**INV-35:** Each pipeline invocation has a unique run_id shared across all run log rows and all layer audit columns for that invocation. No two pipeline invocations share a run_id. run_id is a UUID generated at pipeline start before any model executes.

---

## Session 1 — Project Scaffold and Environment

**Session goal:** A running Docker environment with dbt project wired to DuckDB, all directory structure in place, pipeline.py executable stub that imports without error, and the project committed to the session branch. After this session: `docker compose run pipeline python pipeline.py --help` exits 0.

**Branch slug:** `session/1-scaffold-and-environment`

**Integration check:**
```bash
docker compose run --rm pipeline python pipeline.py --help
docker compose run --rm pipeline dbt debug --project-dir /app/dbt_project
```

---

### Task 1.1 — Repository and Directory Scaffold

**Inputs:** Companion scaffold repository (dbt_project.yml, profiles.yml, models/ stubs, source CSVs, Docker stubs, pipeline.py stub). PBVI standard directory structure.
**Outputs:** All PBVI directories created with .gitkeep, README.md, PROJECT_MANIFEST.md committed to main.

**CC Prompt:**
```
Initialise the PBVI project repository for the Credit Card Financial Transactions Lake.

PROJECT_NAME: credit-card-transactions-lake
PROFILE: DATA_ACCELERATOR
BRIEF_DESCRIPTION: A Medallion architecture data lake (Bronze → Silver → Gold) that ingests daily credit card transaction CSV extracts, enforces quality rules at each layer boundary, and produces analyst-facing Gold aggregations queryable via DuckDB. Implemented as a Docker Compose pipeline using dbt-core 1.7.x with dbt-duckdb adapter and pipeline.py as the orchestration entry point.

Create the following PBVI mandatory directories with .gitkeep files:
  brief/.gitkeep
  docs/.gitkeep
  docs/prompts/.gitkeep
  sessions/.gitkeep
  verification/.gitkeep
  discovery/.gitkeep
  discovery/components/.gitkeep
  enhancements/.gitkeep

Create the following data lake directories with .gitkeep files:
  data/bronze/.gitkeep
  data/silver/.gitkeep
  data/gold/.gitkeep
  data/pipeline/.gitkeep
  source/.gitkeep

Create README.md and PROJECT_MANIFEST.md per PBVI standard structure.

Copy the companion scaffold files into place:
  - dbt_project.yml → dbt_project/dbt_project.yml
  - profiles.yml → dbt_project/profiles.yml
  - models/ stubs → dbt_project/models/
  - source CSVs → source/
  - Docker stubs → Dockerfile, docker-compose.yml
  - pipeline.py stub → pipeline.py

Do not modify any companion scaffold file contents — copy only.

Commit to main:
  git add .
  git commit -m "1.1 — Repository and Directory Scaffold: PBVI structure + companion scaffold copied"
```

**Test cases:**
- Happy path: all directories present, .gitkeep files in place, PROJECT_MANIFEST.md contains all mandatory sections.
- Failure case: companion scaffold file missing → task stops, does not create placeholder; engineer resolves before next task.

**Verification command:**
```bash
ls -la brief/ docs/ sessions/ verification/ discovery/ data/bronze/ data/silver/ data/gold/ data/pipeline/ source/
ls source/*.csv
cat PROJECT_MANIFEST.md | grep -c "PENDING"
```

**Invariant enforcement:** None — scaffold task, no data paths touched.

---

### Task 1.2 — Docker Environment and Python Dependencies

**Inputs:** Dockerfile stub, docker-compose.yml stub, pipeline.py stub.
**Outputs:** Dockerfile installs Python 3.11, dbt-core 1.7.x, dbt-duckdb 1.7.x, duckdb, pandas, pyarrow. docker-compose.yml mounts source/, data/, dbt_project/ into the container. Container builds without error.

**CC Prompt:**
```
Complete the Dockerfile and docker-compose.yml for the Credit Card Transactions Lake pipeline.

Dockerfile requirements:
- Base image: python:3.11-slim
- Install: dbt-core==1.7.*, dbt-duckdb==1.7.*, duckdb, pandas, pyarrow, uuid (stdlib — no install needed)
- Working directory: /app
- Copy: pipeline.py, dbt_project/ into /app
- Default CMD: ["python", "pipeline.py"]

docker-compose.yml requirements:
- Service name: pipeline
- Build from Dockerfile
- Bind mounts:
    ./source:/app/source:ro        (read-only — source CSVs must never be modified)
    ./data:/app/data               (read-write — all layer outputs)
    ./dbt_project:/app/dbt_project (read-write — dbt artifacts written here)
- No ports needed — no server process

Do not create any other services. No database service. No web service.

Commit:
  git add Dockerfile docker-compose.yml
  git commit -m "1.2 — Docker Environment: Dockerfile and compose configuration complete"
```

**Test cases:**
- Happy path: `docker compose build` exits 0, image contains correct Python version.
- Failure case: pip install fails due to version conflict → task stops, engineer resolves version pin before proceeding.

**Verification command:**
```bash
docker compose build 2>&1 | tail -5
docker compose run --rm pipeline python --version
docker compose run --rm pipeline python -c "import dbt; import duckdb; import pandas; import pyarrow; print('OK')"
```

**Invariant enforcement:** None.

---

### Task 1.3 — dbt Project Configuration and Profile Wiring

**Inputs:** dbt_project/dbt_project.yml stub, dbt_project/profiles.yml stub.
**Outputs:** dbt debug passes inside the container. profiles.yml points DuckDB output path to /app/data/dbt_duckdb.db (ephemeral — only used for dbt internal state; Parquet files are the canonical layer outputs). All model stubs present and discovered by dbt.

**CC Prompt:**
```
Configure the dbt project for the Credit Card Transactions Lake.

dbt_project.yml requirements:
- name: credit_card_lake
- version: "1.0.0"
- model-paths: ["models"]
- target-path: "target"
- models:
    credit_card_lake:
      silver:
        +materialized: table
      gold:
        +materialized: table

profiles.yml requirements:
- Profile name: credit_card_lake
- Target: dev
- Type: duckdb
- Path: /app/data/dbt_duckdb.db
- threads: 1

Confirm that the following model stub files exist under dbt_project/models/ (create empty stubs if missing):
  models/silver/silver_transaction_codes.sql
  models/silver/silver_accounts.sql
  models/silver/silver_transactions.sql
  models/silver/silver_quarantine.sql
  models/gold/gold_daily_summary.sql
  models/gold/gold_weekly_account_summary.sql

Each stub must contain only a SQL comment: -- TODO: implement in Session [N]
Do not implement any model logic in this task.

Commit:
  git add dbt_project/
  git commit -m "1.3 — dbt Project Configuration: profiles wired, model stubs confirmed"
```

**Test cases:**
- Happy path: `dbt debug` passes all checks inside container.
- Failure case: DuckDB adapter not found → Dockerfile pip install from Task 1.2 is incomplete; return to 1.2.

**Verification command:**
```bash
docker compose run --rm pipeline dbt debug --project-dir /app/dbt_project --profiles-dir /app/dbt_project
docker compose run --rm pipeline dbt list --project-dir /app/dbt_project --profiles-dir /app/dbt_project
```

**Invariant enforcement:** None.

---

### Task 1.4 — pipeline.py Stub with Entry Points and run_id Generation

**Inputs:** pipeline.py stub from companion scaffold.
**Outputs:** pipeline.py defines two callable entry points: `run_historical(start_date, end_date)` and `run_incremental()`. Both are stubs that print "NOT IMPLEMENTED" and exit 0. CLI `--help` works. run_id is generated as UUID at top of each function (satisfying INV-35 structure, no data written yet). `python pipeline.py --help` exits 0.

**CC Prompt:**
```
Implement the pipeline.py stub for the Credit Card Transactions Lake.

Requirements:
1. CLI interface using argparse:
   - Subcommand: historical --start-date YYYY-MM-DD --end-date YYYY-MM-DD
   - Subcommand: incremental
   - --help must describe both subcommands

2. run_historical(start_date: str, end_date: str) function:
   - First line: generate run_id = str(uuid.uuid4())
   - Print: f"[HISTORICAL] run_id={run_id} start={start_date} end={end_date} — NOT IMPLEMENTED"
   - Exit 0

3. run_incremental() function:
   - First line: generate run_id = str(uuid.uuid4())
   - Print: f"[INCREMENTAL] run_id={run_id} — NOT IMPLEMENTED"
   - Exit 0

4. Constants block at top of file (to be used in later sessions):
   DATA_DIR = Path("/app/data")
   SOURCE_DIR = Path("/app/source")
   BRONZE_DIR = DATA_DIR / "bronze"
   SILVER_DIR = DATA_DIR / "silver"
   GOLD_DIR = DATA_DIR / "gold"
   PIPELINE_DIR = DATA_DIR / "pipeline"
   DBT_PROJECT_DIR = Path("/app/dbt_project")

5. Imports: uuid, pathlib.Path, argparse, sys — stdlib only at this stage.

INVARIANT — INV-35 (GLOBAL): Each pipeline invocation must have a unique run_id shared across all model-level run log rows and all layer audit columns for that invocation. No two invocations share a run_id. run_id must be a UUID generated at the start of each invocation, before any model executes. Confirm run_id is generated as the first operation in both entry point functions, before any other logic.

Commit:
  git add pipeline.py
  git commit -m "1.4 — pipeline.py Stub: CLI entry points and run_id generation structure"
```

**Test cases:**
- Happy path: `python pipeline.py --help` exits 0 and lists both subcommands.
- Happy path: `python pipeline.py historical --start-date 2024-01-15 --end-date 2024-01-21` exits 0, prints run_id.
- Happy path: `python pipeline.py incremental` exits 0, prints run_id.
- Failure case: Two invocations produce the same run_id → UUID generation is incorrect; must use uuid.uuid4().

**Verification command:**
```bash
docker compose run --rm pipeline python pipeline.py --help
docker compose run --rm pipeline python pipeline.py historical --start-date 2024-01-15 --end-date 2024-01-21
docker compose run --rm pipeline python pipeline.py incremental
# Confirm two separate incremental runs produce different run_ids:
docker compose run --rm pipeline python pipeline.py incremental 2>&1 | grep run_id
docker compose run --rm pipeline python pipeline.py incremental 2>&1 | grep run_id
```

**Invariant enforcement:** INV-35 — code review must confirm: (1) uuid.uuid4() is called, not a static string; (2) run_id is generated before any other logic in both functions; (3) no global run_id variable that could persist between invocations.

---

**Session 1 Integration Check:**
```bash
docker compose run --rm pipeline python pipeline.py --help
docker compose run --rm pipeline dbt debug --project-dir /app/dbt_project --profiles-dir /app/dbt_project
docker compose run --rm pipeline python -c "import dbt; import duckdb; print('deps OK')"
ls source/ | wc -l  # should show all 15 source CSVs (7 transactions + 7 accounts + 1 codes)
```

---

## Session 2 — Bronze Loader: Transactions and Accounts

**Session goal:** Bronze partitions for transactions and accounts are written correctly. INV-01 through INV-05 and INV-37 are enforced. Running the loader twice produces identical output. After this session: `docker compose run pipeline python pipeline.py historical --start-date 2024-01-15 --end-date 2024-01-15` writes a Bronze transactions partition and a Bronze accounts partition (Gold and Silver still NOT IMPLEMENTED).

**Branch slug:** `session/2-bronze-transactions-accounts`

**Integration check:**
```bash
docker compose run --rm pipeline python pipeline.py historical --start-date 2024-01-15 --end-date 2024-01-15
duckdb -c "SELECT count(*) FROM read_parquet('data/bronze/transactions/date=2024-01-15/data.parquet');"
duckdb -c "SELECT count(*) FROM read_parquet('data/bronze/accounts/date=2024-01-15/data.parquet');"
# Run again — counts must be identical (idempotency)
docker compose run --rm pipeline python pipeline.py historical --start-date 2024-01-15 --end-date 2024-01-15
duckdb -c "SELECT count(*) FROM read_parquet('data/bronze/transactions/date=2024-01-15/data.parquet');"
```

---

### Task 2.1 — Bronze Loader Base Class and Partition Path Utilities

**Inputs:** pipeline.py constants block (Task 1.4), source schema from requirements brief Section 2.
**Outputs:** `bronze_loader.py` module with: `partition_path(entity, date)` → `Path`, `partition_exists_and_valid(path)` → `bool` (file exists AND row count > 0), `read_csv_to_duckdb(filepath, schema)` → DuckDB relation.

**CC Prompt:**
```
Create bronze_loader.py in the project root containing Bronze loader utilities.

Requirements:
1. partition_path(entity: str, date: str) -> Path
   - Returns: BRONZE_DIR / entity / f"date={date}" / "data.parquet"
   - BRONZE_DIR is imported from pipeline.py constants
   - entity values for this system: "transactions", "accounts", "transaction_codes"

2. partition_exists_and_valid(path: Path) -> bool
   - Returns True only if: file exists AND DuckDB row count > 0
   - Uses: duckdb.connect().execute(f"SELECT count(*) FROM read_parquet('{path}')").fetchone()[0]
   - Returns False if file does not exist
   - Returns False if file exists but row count == 0 (partial write guard per Architecture Key Risk)
   - Never raises on missing file — catches FileNotFoundError and returns False

3. read_csv_to_duckdb(filepath: Path, schema: dict) -> duckdb.DuckDBPyRelation
   - schema is a dict mapping column name → DuckDB type string (e.g. {"transaction_id": "VARCHAR", "amount": "DECIMAL"})
   - Uses duckdb.read_csv(str(filepath), dtype=schema)
   - Returns the relation — does not execute or write

4. Imports: duckdb, pathlib.Path, typing.

No writes to disk in this task. No audit columns in this task.

Commit:
  git add bronze_loader.py
  git commit -m "2.1 — Bronze Loader Base: partition_path, exists_and_valid, read_csv utilities"
```

**Test cases:**
- Happy path: `partition_path("transactions", "2024-01-15")` returns `Path("/app/data/bronze/transactions/date=2024-01-15/data.parquet")`.
- Happy path: `partition_exists_and_valid(path)` returns False for nonexistent path without raising.
- Happy path: `partition_exists_and_valid(path)` returns False for a zero-byte file.
- Failure case: schema mismatch in read_csv → DuckDB raises; error surfaces to caller, not swallowed.

**Verification command:**
```bash
docker compose run --rm pipeline python -c "
from bronze_loader import partition_path, partition_exists_and_valid
p = partition_path('transactions', '2024-01-15')
print('path:', p)
print('exists_false:', partition_exists_and_valid(p))
"
```

**Invariant enforcement:** INV-04 — code review must confirm `partition_exists_and_valid` checks both file existence AND row count > 0, so a partial write cannot be silently accepted as a valid partition.

---

### Task 2.2 — Bronze Transactions Loader

**Inputs:** `bronze_loader.py` (Task 2.1), transactions source schema (brief Section 2.1), audit column spec (brief Section 4.1), source CSV at `source/transactions_YYYY-MM-DD.csv`.
**Outputs:** `load_bronze_transactions(date: str, run_id: str)` function in `bronze_loader.py`. Writes `data/bronze/transactions/date=YYYY-MM-DD/data.parquet`. Skips (no write, no error) if partition already exists and is valid. Adds three audit columns. Applies explicit type schema.

**CC Prompt:**
```
Add load_bronze_transactions(date: str, run_id: str) to bronze_loader.py.

Source file path: SOURCE_DIR / f"transactions_{date}.csv"
Output path: partition_path("transactions", date)

Schema (applied explicitly — no type inference):
  transaction_id: VARCHAR
  account_id: VARCHAR
  transaction_date: DATE
  amount: DECIMAL(18,4)
  transaction_code: VARCHAR
  merchant_name: VARCHAR
  channel: VARCHAR

Audit columns to add (not present in source — added by loader):
  _source_file: STRING — basename of the source CSV filename only (no path)
  _ingested_at: TIMESTAMP — current UTC timestamp at write time (datetime.utcnow())
  _pipeline_run_id: STRING — the run_id argument passed to this function

Logic:
1. Check if partition already exists and is valid using partition_exists_and_valid().
   If yes: log "SKIP bronze_transactions {date} — partition already valid" and return.
   Do not raise. Do not write.
2. Read CSV using read_csv_to_duckdb() with the schema above.
3. Add audit columns using DuckDB SQL: SELECT *, '{source_file}' AS _source_file, ...
4. Create output directory if it does not exist.
5. Write to output path using duckdb COPY ... TO ... (FORMAT PARQUET).
6. After write: call partition_exists_and_valid() to confirm. If False: raise RuntimeError.

INVARIANT — INV-01: Running the Bronze loader for date D twice on identical source input produces an identical Bronze partition — same row count, no new records added. Confirm the existence check in step 1 implements this: skip on valid partition, no second write.

INVARIANT — INV-02: All source fields preserve exact source values. Type mappings are applied explicitly per the schema dict above — no implicit inference. Confirm read_csv_to_duckdb uses the explicit dtype mapping and does not allow DuckDB to infer types.

INVARIANT — INV-04: A Bronze partition is never overwritten after initial write. Confirm the loader aborts (returns without writing) when partition_exists_and_valid() returns True. There must be no code path that writes to an existing valid partition.

INVARIANT — INV-05: A Bronze partition for date D contains only records from the source file for date D. Confirm partition path is derived from the date argument — not from record content. The loader must not read any file other than transactions_{date}.csv.

INVARIANT — INV-37: Every Bronze partition contains exactly the expected source columns plus the three audit columns. No other columns. Confirm the SELECT in step 3 names columns explicitly — no SELECT * that could admit unexpected columns from the CSV.

Commit:
  git add bronze_loader.py
  git commit -m "2.2 — Bronze Transactions Loader: write, skip, audit columns, invariant enforcement"
```

**Test cases:**
- Happy path: CSV present → partition written, row count matches CSV wc -l minus header.
- Happy path: partition exists and valid → function returns without writing (row count unchanged).
- Failure case: partition file exists but row count = 0 → treated as invalid, partition is rewritten.
- Failure case: source CSV missing → FileNotFoundError surfaces to caller (not swallowed).
- Audit check: `_source_file` = "transactions_2024-01-15.csv" (basename only, no path).
- Audit check: `_pipeline_run_id` = the run_id argument value, non-null.

**Verification command:**
```bash
# Write partition
docker compose run --rm pipeline python -c "
from bronze_loader import load_bronze_transactions
load_bronze_transactions('2024-01-15', 'test-run-001')
"
duckdb -c "SELECT count(*) FROM read_parquet('data/bronze/transactions/date=2024-01-15/data.parquet');"
duckdb -c "SELECT _source_file, _pipeline_run_id FROM read_parquet('data/bronze/transactions/date=2024-01-15/data.parquet') LIMIT 3;"
# Confirm column schema
duckdb -c "DESCRIBE SELECT * FROM read_parquet('data/bronze/transactions/date=2024-01-15/data.parquet');"
# Idempotency: run again, count must be identical
docker compose run --rm pipeline python -c "
from bronze_loader import load_bronze_transactions
load_bronze_transactions('2024-01-15', 'test-run-002')
"
duckdb -c "SELECT count(*) FROM read_parquet('data/bronze/transactions/date=2024-01-15/data.parquet');"
```

**Invariant enforcement (code review):**
- INV-01: Existence check before write — no second write path exists.
- INV-02: `dtype=schema` used in read_csv; no inference fallback.
- INV-04: No overwrite — confirm no `os.remove` or implicit overwrite in COPY TO.
- INV-05: Partition path uses date argument, not record content.
- INV-37: SELECT names all columns explicitly; DESCRIBE output matches spec exactly.

---

### Task 2.3 — Bronze Accounts Loader

**Inputs:** `bronze_loader.py` (Task 2.2), accounts source schema (brief Section 2.3), audit column spec (brief Section 4.1), source CSV at `source/accounts_YYYY-MM-DD.csv`.
**Outputs:** `load_bronze_accounts(date: str, run_id: str)` in `bronze_loader.py`. Writes `data/bronze/accounts/date=YYYY-MM-DD/data.parquet`. Same skip/write/audit logic as transactions. A missing accounts file for a date is a no-op (not an error) — accounts delta absence means no changes that day.

**CC Prompt:**
```
Add load_bronze_accounts(date: str, run_id: str) to bronze_loader.py.

Source file path: SOURCE_DIR / f"accounts_{date}.csv"
Output path: partition_path("accounts", date)

Schema (applied explicitly — no type inference):
  account_id: VARCHAR
  open_date: DATE
  credit_limit: DECIMAL(18,4)
  current_balance: DECIMAL(18,4)
  billing_cycle_start: INTEGER
  billing_cycle_end: INTEGER
  account_status: VARCHAR

Audit columns (same as transactions):
  _source_file: STRING — basename of source CSV only
  _ingested_at: TIMESTAMP — datetime.utcnow()
  _pipeline_run_id: STRING — run_id argument

Logic:
1. If source CSV does not exist: log "SKIP bronze_accounts {date} — no delta file" and return.
   This is the defined no-data behaviour — absence of accounts file means no changes. Not an error.
2. Check partition_exists_and_valid(). If True: skip and return (idempotency).
3. Read CSV with explicit schema. Add audit columns. Create dir. Write Parquet. Validate after write.

INVARIANT — INV-01: Same as transactions — skip on valid partition.
INVARIANT — INV-02: Explicit type schema — no inference.
INVARIANT — INV-04: No overwrite of existing valid partition.
INVARIANT — INV-05: Partition path derived from date argument — not from account_id or record content.
INVARIANT — INV-37: SELECT names all expected columns explicitly — no extra columns.

Note: The missing-file no-op behaviour (step 1) is distinct from idempotency (step 2). Both branches must log their outcome clearly.

Commit:
  git add bronze_loader.py
  git commit -m "2.3 — Bronze Accounts Loader: write, skip, no-delta no-op, invariant enforcement"
```

**Test cases:**
- Happy path: accounts CSV present → partition written correctly.
- Happy path: partition exists and valid → skip, no write.
- Happy path: accounts CSV absent → function returns without error (no-op).
- Audit check: all three audit columns present and non-null.
- Schema check: `billing_cycle_start` typed as INTEGER (not VARCHAR).

**Verification command:**
```bash
docker compose run --rm pipeline python -c "
from bronze_loader import load_bronze_accounts
load_bronze_accounts('2024-01-15', 'test-run-001')
"
duckdb -c "SELECT count(*) FROM read_parquet('data/bronze/accounts/date=2024-01-15/data.parquet');"
duckdb -c "DESCRIBE SELECT * FROM read_parquet('data/bronze/accounts/date=2024-01-15/data.parquet');"
duckdb -c "SELECT _source_file, _pipeline_run_id FROM read_parquet('data/bronze/accounts/date=2024-01-15/data.parquet') LIMIT 2;"
# Test missing file no-op
docker compose run --rm pipeline python -c "
from bronze_loader import load_bronze_accounts
load_bronze_accounts('2099-01-01', 'test-run-noop')
print('no-op returned without error')
"
```

**Invariant enforcement (code review):** Same checklist as Task 2.2 applied to accounts schema.

---

### Task 2.4 — Bronze Loader Integration into pipeline.py (Single Date)

**Inputs:** `bronze_loader.py` (Tasks 2.2–2.3), pipeline.py (Task 1.4).
**Outputs:** `run_historical` in pipeline.py calls `load_bronze_transactions` and `load_bronze_accounts` for a single date, passing `run_id`. Silver and Gold steps remain NOT IMPLEMENTED stubs.

**CC Prompt:**
```
Wire the Bronze loaders into pipeline.py's run_historical function for single-date operation.

In run_historical(start_date, end_date):
- For now, process only start_date (full date range iteration comes in Session 8).
- Import and call load_bronze_transactions(date=start_date, run_id=run_id).
- Import and call load_bronze_accounts(date=start_date, run_id=run_id).
- Silver and Gold: print "Silver/Gold — NOT IMPLEMENTED" and continue.
- run_id is already generated at the top of run_historical (Task 1.4).

The order is: Bronze transactions first, Bronze accounts second, then Silver (stub), then Gold (stub).

Do not implement date range iteration in this task. Single date only.
Do not implement the run log in this task.
Do not implement watermark write in this task.

INVARIANT — INV-35 (GLOBAL): run_id is passed to both Bronze loaders so that all records written in this invocation share the same run_id. Confirm run_id is generated once at the start of run_historical and passed through — not regenerated inside each loader call.

Commit:
  git add pipeline.py
  git commit -m "2.4 — Bronze Wiring: transactions and accounts loaders called from pipeline.py"
```

**Test cases:**
- Happy path: `python pipeline.py historical --start-date 2024-01-15 --end-date 2024-01-21` writes Bronze partitions for 2024-01-15 only (single date scope for this task).
- Happy path: second run produces identical row counts (idempotency via Task 2.2/2.3 guards).
- Audit check: `_pipeline_run_id` values in transactions and accounts partitions both equal the same run_id from that invocation.

**Verification command:**
```bash
docker compose run --rm pipeline python pipeline.py historical --start-date 2024-01-15 --end-date 2024-01-21
duckdb -c "SELECT count(*) FROM read_parquet('data/bronze/transactions/date=2024-01-15/data.parquet');"
duckdb -c "SELECT count(*) FROM read_parquet('data/bronze/accounts/date=2024-01-15/data.parquet');"
# Confirm run_id is the same across both entities
duckdb -c "SELECT DISTINCT _pipeline_run_id FROM read_parquet('data/bronze/transactions/date=2024-01-15/data.parquet');"
duckdb -c "SELECT DISTINCT _pipeline_run_id FROM read_parquet('data/bronze/accounts/date=2024-01-15/data.parquet');"
```

**Invariant enforcement (code review):** INV-35 — confirm run_id is generated once at the top of run_historical, not inside the loader functions.

---

### Task 2.5 — Bronze Conservation Utility (Row Count Reconciliation)

**Inputs:** `bronze_loader.py`, requirements brief Section 10.2 conservation check.
**Outputs:** `bronze_row_count(entity, date)` → int utility function in `bronze_loader.py`. Used later by Silver promotion to verify conservation (INV-07).

**CC Prompt:**
```
Add bronze_row_count(entity: str, date: str) -> int to bronze_loader.py.

Implementation:
  path = partition_path(entity, date)
  if not partition_exists_and_valid(path):
      return 0
  return duckdb.connect().execute(f"SELECT count(*) FROM read_parquet('{path}')").fetchone()[0]

No writes. No side effects. Pure read utility.

Also add bronze_accounts_row_count(date: str) -> int that calls bronze_row_count("accounts", date).
Also add bronze_transactions_row_count(date: str) -> int that calls bronze_row_count("transactions", date).

Commit:
  git add bronze_loader.py
  git commit -m "2.5 — Bronze Conservation Utility: row count functions for INV-07 enforcement"
```

**Test cases:**
- Happy path: returns correct row count for existing partition.
- Happy path: returns 0 for nonexistent partition (no exception).

**Verification command:**
```bash
docker compose run --rm pipeline python -c "
from bronze_loader import bronze_transactions_row_count, bronze_accounts_row_count
print('tx count:', bronze_transactions_row_count('2024-01-15'))
print('acct count:', bronze_accounts_row_count('2024-01-15'))
print('nonexistent:', bronze_transactions_row_count('2099-01-01'))
"
# Compare against CSV line count
wc -l source/transactions_2024-01-15.csv
wc -l source/accounts_2024-01-15.csv
```

**Invariant enforcement:** INV-07 — this utility will be used at Silver promotion time to enforce conservation. Code review must confirm it returns 0 (not raises) for missing partitions, so conservation checks do not fail on genuinely absent accounts files.

---

**Session 2 Integration Check:**
```bash
# Clean state
rm -rf data/bronze/transactions data/bronze/accounts
docker compose run --rm pipeline python pipeline.py historical --start-date 2024-01-15 --end-date 2024-01-21
duckdb -c "SELECT count(*) FROM read_parquet('data/bronze/transactions/date=2024-01-15/data.parquet');"
duckdb -c "SELECT count(*) FROM read_parquet('data/bronze/accounts/date=2024-01-15/data.parquet');"
# Idempotency
docker compose run --rm pipeline python pipeline.py historical --start-date 2024-01-15 --end-date 2024-01-21
duckdb -c "SELECT count(*) FROM read_parquet('data/bronze/transactions/date=2024-01-15/data.parquet');"
```

---

## Session 3 — Bronze Loader: Transaction Codes and Run Log Infrastructure

**Session goal:** Transaction codes land in Bronze. The run log write infrastructure is implemented and tested. Watermark read/write utilities are in place. After this session: transaction codes Bronze partition exists, run log Parquet can be written and re-read with correct append behaviour.

**Branch slug:** `session/3-bronze-codes-runlog`

**Integration check:**
```bash
docker compose run --rm pipeline python -c "
from bronze_loader import load_bronze_transaction_codes
load_bronze_transaction_codes('test-run-003')
"
duckdb -c "SELECT count(*) FROM read_parquet('data/bronze/transaction_codes/data.parquet');"
docker compose run --rm pipeline python -c "
from pipeline_control import append_run_log_entry, read_run_log
append_run_log_entry(run_id='r1', pipeline_type='HISTORICAL', model_name='bronze_transactions', layer='BRONZE', started_at='2024-01-15T10:00:00', completed_at='2024-01-15T10:00:05', status='SUCCESS', records_processed=100, records_written=100, records_rejected=None, error_message=None)
df = read_run_log()
print(df.shape)
"
```

---

### Task 3.1 — Bronze Transaction Codes Loader

**Inputs:** `bronze_loader.py`, transaction_codes schema (brief Section 2.2), audit columns spec.
**Outputs:** `load_bronze_transaction_codes(run_id: str)` in `bronze_loader.py`. Writes `data/bronze/transaction_codes/data.parquet` (not date-partitioned). Skip if already valid.

**CC Prompt:**
```
Add load_bronze_transaction_codes(run_id: str) to bronze_loader.py.

Source file path: SOURCE_DIR / "transaction_codes.csv"
Output path: BRONZE_DIR / "transaction_codes" / "data.parquet"  (not date-partitioned)

Schema (applied explicitly):
  transaction_code: VARCHAR
  transaction_type: VARCHAR
  description: VARCHAR
  debit_credit_indicator: VARCHAR
  affects_balance: BOOLEAN

Audit columns:
  _source_file: STRING — "transaction_codes.csv"
  _ingested_at: TIMESTAMP — datetime.utcnow()
  _pipeline_run_id: STRING — run_id argument

Logic:
1. Check if output file exists and row count > 0 (reuse partition_exists_and_valid logic — adapt for non-partitioned path).
   If valid: log "SKIP bronze_transaction_codes — already loaded" and return.
2. Read CSV, add audit columns, write Parquet.
3. Post-write validation: row count > 0, confirm.

INVARIANT — INV-01: Skip if already valid — same guard as other loaders.
INVARIANT — INV-02: Explicit schema — no inference. BOOLEAN type for affects_balance must not be cast to INTEGER or VARCHAR.
INVARIANT — INV-04: No overwrite — skip is the only response to an existing valid file.
INVARIANT — INV-37: All five source columns plus three audit columns present. Confirm with DESCRIBE after write.

Commit:
  git add bronze_loader.py
  git commit -m "3.1 — Bronze Transaction Codes Loader: write, skip, audit columns"
```

**Test cases:**
- Happy path: `transaction_codes.csv` present → Parquet written, row count matches CSV.
- Happy path: re-run → skip, file unchanged.
- Schema check: `affects_balance` is BOOLEAN type in output Parquet.
- Audit check: `_source_file` = "transaction_codes.csv", `_pipeline_run_id` non-null.

**Verification command:**
```bash
docker compose run --rm pipeline python -c "
from bronze_loader import load_bronze_transaction_codes
load_bronze_transaction_codes('test-run-003')
"
duckdb -c "SELECT count(*) FROM read_parquet('data/bronze/transaction_codes/data.parquet');"
duckdb -c "DESCRIBE SELECT * FROM read_parquet('data/bronze/transaction_codes/data.parquet');"
duckdb -c "SELECT _source_file, _pipeline_run_id FROM read_parquet('data/bronze/transaction_codes/data.parquet') LIMIT 2;"
# Idempotency
docker compose run --rm pipeline python -c "
from bronze_loader import load_bronze_transaction_codes
load_bronze_transaction_codes('test-run-004')
"
duckdb -c "SELECT count(*) FROM read_parquet('data/bronze/transaction_codes/data.parquet');"
```

**Invariant enforcement (code review):** INV-01, INV-02, INV-04, INV-37 — same checklist as Task 2.2.

---

### Task 3.2 — pipeline_control.py: Watermark Read and Write

**Inputs:** pipeline.py constants, requirements brief Section 6.1 control table schema.
**Outputs:** `pipeline_control.py` module with: `read_watermark()` → `date | None`, `write_watermark(date, run_id)`. Watermark write enforces monotonicity (INV-29). Watermark read returns None if control file absent.

**CC Prompt:**
```
Create pipeline_control.py with watermark read and write functions.

Control table schema (pipeline/control.parquet):
  last_processed_date: DATE
  updated_at: TIMESTAMP
  updated_by_run_id: STRING

1. read_watermark() -> date | None
   - If PIPELINE_DIR / "control.parquet" does not exist: return None
   - Read with duckdb, return last_processed_date as a Python date object.

2. write_watermark(new_date: date, run_id: str)
   - Read current watermark with read_watermark().
   - If current watermark is not None AND new_date <= current watermark: raise ValueError(
       f"Watermark monotonicity violation: new_date={new_date} <= current={current}")
   - Write control.parquet with: last_processed_date=new_date, updated_at=datetime.utcnow(), updated_by_run_id=run_id
   - Full overwrite of control.parquet (single-row file — overwrite is correct here).

INVARIANT — INV-29: The watermark never decreases. The check in write_watermark (new_date <= current) must raise, not log-and-continue. The check uses <=, not <, to prevent writing the same date twice.

INVARIANT — INV-28: write_watermark is called only after all three layers complete successfully. This is an orchestration-level concern enforced in pipeline.py (Sessions 8–9). Here, write_watermark must have no logic that auto-triggers or self-initiates — it must be explicitly called by the orchestrator.

Commit:
  git add pipeline_control.py
  git commit -m "3.2 — Watermark Read/Write: control table with monotonicity guard"
```

**Test cases:**
- Happy path: `read_watermark()` returns None when control.parquet absent.
- Happy path: `write_watermark(date(2024,1,15), 'r1')` → file created, read_watermark() returns 2024-01-15.
- Happy path: `write_watermark(date(2024,1,16), 'r2')` → advances correctly.
- Failure case: `write_watermark(date(2024,1,15), 'r3')` when current = 2024-01-15 → raises ValueError (same date).
- Failure case: `write_watermark(date(2024,1,14), 'r4')` when current = 2024-01-15 → raises ValueError (decrease).

**Verification command:**
```bash
docker compose run --rm pipeline python -c "
from pipeline_control import read_watermark, write_watermark
from datetime import date
print('initial:', read_watermark())
write_watermark(date(2024,1,15), 'r1')
print('after write:', read_watermark())
write_watermark(date(2024,1,16), 'r2')
print('after advance:', read_watermark())
"
# Monotonicity guard
docker compose run --rm pipeline python -c "
from pipeline_control import write_watermark
from datetime import date
try:
    write_watermark(date(2024,1,15), 'r3')
    print('ERROR: should have raised')
except ValueError as e:
    print('PASS monotonicity guard:', e)
"
```

**Invariant enforcement (code review):** INV-29 — confirm the condition is `new_date <= current` not `<`; confirm it raises ValueError not logs-and-continues; confirm no code path bypasses the check.

---

### Task 3.3 — pipeline_control.py: Run Log Append and Read

**Inputs:** `pipeline_control.py` (Task 3.2), requirements brief Section 6.2 run log schema.
**Outputs:** `append_run_log_entry(...)` and `read_run_log() -> pd.DataFrame` in `pipeline_control.py`. Append-only implementation via read-union-write. INV-30 enforced.

**CC Prompt:**
```
Add run log functions to pipeline_control.py.

Run log schema (pipeline/run_log.parquet):
  run_id: VARCHAR
  pipeline_type: VARCHAR (HISTORICAL or INCREMENTAL)
  model_name: VARCHAR
  layer: VARCHAR (BRONZE, SILVER, or GOLD)
  started_at: TIMESTAMP
  completed_at: TIMESTAMP
  status: VARCHAR (SUCCESS, FAILED, or SKIPPED — no other values permitted)
  records_processed: INTEGER (nullable)
  records_written: INTEGER (nullable)
  records_rejected: INTEGER (nullable — Silver only)
  error_message: VARCHAR (nullable)

1. append_run_log_entry(run_id, pipeline_type, model_name, layer, started_at, completed_at,
                         status, records_processed, records_written, records_rejected, error_message)
   - Validate: status must be one of {"SUCCESS", "FAILED", "SKIPPED"} — raise ValueError if not.
   - Validate: run_id, model_name, started_at, completed_at must be non-null — raise ValueError if any null.
   - Read existing run_log.parquet if it exists (into pandas DataFrame).
   - Append new row to DataFrame (or create new DataFrame if file absent).
   - Write combined DataFrame back to run_log.parquet (full overwrite of the file, but preserving all rows).
   - This implements the append-only guarantee at the application layer — no rows are ever removed.

2. read_run_log() -> pd.DataFrame
   - Returns full run log as pandas DataFrame.
   - Returns empty DataFrame (with correct columns) if file absent.

INVARIANT — INV-30: The run log is append-only. Existing rows are never modified or deleted. Confirm append_run_log_entry reads all existing rows before writing, and the write includes those rows. Confirm no UPDATE or DELETE logic exists anywhere in this function.

INVARIANT — INV-31: Each model execution produces exactly one run log row with non-null run_id, model_name, started_at, completed_at, and status. This is enforced here by the null validation in step (validate). The caller (pipeline.py) is responsible for calling this once per model execution.

INVARIANT — INV-32: status is set from the enumerated set {SUCCESS, FAILED, SKIPPED} — no null status, no free-text. Confirm the validation raises before write if status is not in the set.

Commit:
  git add pipeline_control.py
  git commit -m "3.3 — Run Log Append/Read: append-only implementation with status validation"
```

**Test cases:**
- Happy path: first append → file created with one row.
- Happy path: second append → file has two rows, first row unchanged.
- Happy path: read_run_log on absent file → empty DataFrame with correct columns.
- Failure case: status = "RUNNING" → ValueError raised before write.
- Failure case: run_id = None → ValueError raised.
- Append-only check: read before and after append, confirm first row fields are byte-identical.

**Verification command:**
```bash
docker compose run --rm pipeline python -c "
from pipeline_control import append_run_log_entry, read_run_log
from datetime import datetime
append_run_log_entry('r1','HISTORICAL','bronze_transactions','BRONZE',
  datetime(2024,1,15,10,0,0), datetime(2024,1,15,10,0,5),
  'SUCCESS', 100, 100, None, None)
append_run_log_entry('r1','HISTORICAL','bronze_accounts','BRONZE',
  datetime(2024,1,15,10,0,5), datetime(2024,1,15,10,0,8),
  'SUCCESS', 50, 50, None, None)
df = read_run_log()
print(df.shape)  # (2, 11)
print(df[['run_id','model_name','status']])
"
# Status validation
docker compose run --rm pipeline python -c "
from pipeline_control import append_run_log_entry
from datetime import datetime
try:
    append_run_log_entry('r1','HISTORICAL','test','BRONZE',
      datetime.utcnow(), datetime.utcnow(), 'RUNNING', 0, 0, None, None)
    print('ERROR: should have raised')
except ValueError as e:
    print('PASS:', e)
"
```

**Invariant enforcement (code review):** INV-30 — confirm read-before-write pattern; INV-32 — confirm status validation uses a set literal, not a conditional chain.

---

### Task 3.4 — Run Log Wiring into pipeline.py (Bronze Models Only)

**Inputs:** `pipeline_control.py` (Task 3.3), pipeline.py (Task 2.4).
**Outputs:** pipeline.py's `run_historical` wraps each Bronze loader call in a run log entry: started_at before call, completed_at after, status = SUCCESS or FAILED. Silver and Gold remain NOT IMPLEMENTED stubs but still write a SKIPPED run log row as a placeholder.

**CC Prompt:**
```
Wire run log entries into pipeline.py for Bronze loader calls in run_historical.

For each Bronze loader call (load_bronze_transaction_codes, load_bronze_transactions, load_bronze_accounts):
1. Record started_at = datetime.utcnow() before the loader call.
2. Call the loader inside a try/except block.
3. On success: record completed_at = datetime.utcnow(), call append_run_log_entry with status="SUCCESS".
4. On exception: record completed_at = datetime.utcnow(), call append_run_log_entry with status="FAILED",
   error_message = str(exception). Re-raise the exception after logging.

Model names to use (must match exactly — these are the audit keys):
  bronze_transaction_codes
  bronze_transactions
  bronze_accounts

records_processed for Bronze loaders = bronze_row_count(entity, date) called AFTER the loader write completes.
records_written = same value as records_processed (Bronze writes all source records).
records_rejected = None (Bronze has no rejection path).

Do not implement Silver or Gold run log entries in this task.
Silver/Gold stubs write a SKIPPED entry: status="SKIPPED", all record counts None.

INVARIANT — INV-31: Each Bronze loader execution produces exactly one run log row with non-null run_id, model_name, started_at, completed_at, and status. Confirm the try/except wraps only the loader call, not the log write itself — the log write must execute on both success and failure paths.

INVARIANT — INV-32: status is "SUCCESS" on clean completion, "FAILED" on any exception. Confirm there is no default value for status — it is always set from the try/except outcome.

INVARIANT — INV-35 (GLOBAL): run_id passed to append_run_log_entry must be the same run_id generated at the top of run_historical. Confirm no new UUID is generated inside the log wiring.

Commit:
  git add pipeline.py
  git commit -m "3.4 — Run Log Wiring: Bronze loader entries in pipeline.py"
```

**Test cases:**
- Happy path: after a run, run log has entries for bronze_transaction_codes, bronze_transactions, bronze_accounts with status=SUCCESS.
- Failure case: if a loader raises (e.g. missing source file) → FAILED entry written, exception re-raised.
- Audit check: all run log rows for one invocation share the same run_id.

**Verification command:**
```bash
rm -f data/pipeline/run_log.parquet
docker compose run --rm pipeline python pipeline.py historical --start-date 2024-01-15 --end-date 2024-01-21
duckdb -c "SELECT run_id, model_name, status, records_processed FROM read_parquet('data/pipeline/run_log.parquet') ORDER BY started_at;"
duckdb -c "SELECT count(DISTINCT run_id) FROM read_parquet('data/pipeline/run_log.parquet');"
```

**Invariant enforcement (code review):** INV-31 — confirm try/except structure; INV-32 — confirm no default status; INV-35 — run_id not regenerated inside wiring.

---

**Session 3 Integration Check:**
```bash
rm -rf data/bronze/transaction_codes data/pipeline/
docker compose run --rm pipeline python pipeline.py historical --start-date 2024-01-15 --end-date 2024-01-21
duckdb -c "SELECT count(*) FROM read_parquet('data/bronze/transaction_codes/data.parquet');"
duckdb -c "SELECT model_name, status FROM read_parquet('data/pipeline/run_log.parquet') ORDER BY started_at;"
```

---

## Session 4 — Silver: Transaction Codes and Accounts

**Session goal:** Silver transaction_codes reference file exists and is queryable. Silver accounts upsert is implemented and idempotent. Both dbt models pass `dbt run` inside the container. After this session: Silver is populated for transaction codes and accounts, INV-17, INV-18, INV-33 are enforced.

**Branch slug:** `session/4-silver-codes-accounts`

**Integration check:**
```bash
docker compose run --rm pipeline dbt run --project-dir /app/dbt_project --profiles-dir /app/dbt_project --select silver_transaction_codes silver_accounts
duckdb -c "SELECT count(*) FROM read_parquet('data/silver/transaction_codes/data.parquet');"
duckdb -c "SELECT count(*) FROM read_parquet('data/silver/accounts/data.parquet');"
duckdb -c "SELECT count(*) - count(DISTINCT account_id) AS duplicates FROM read_parquet('data/silver/accounts/data.parquet');"
```

---

### Task 4.1 — Silver Transaction Codes dbt Model

**Inputs:** Bronze transaction_codes partition (Session 3), dbt model stub, Silver audit columns spec (brief Section 4.2.3).
**Outputs:** `models/silver/silver_transaction_codes.sql` promotes Bronze transaction codes to Silver with audit columns. No quarantine path for this entity.

**CC Prompt:**
```
Implement dbt_project/models/silver/silver_transaction_codes.sql.

Source: read_parquet('/app/data/bronze/transaction_codes/data.parquet') using dbt source() or direct path.
Output path: silver/transaction_codes/data.parquet

Use a dbt custom materialisation or a pre-hook to write directly to Parquet.
If dbt-duckdb supports COPY TO from a model, use that. Otherwise, implement as a dbt macro that writes to Parquet.

Output columns (source columns carried forward verbatim):
  transaction_code, transaction_type, description, debit_credit_indicator, affects_balance

Silver audit columns added by this model:
  _source_file: carried from Bronze _source_file
  _bronze_ingested_at: carried from Bronze _ingested_at
  _pipeline_run_id: {{ var('run_id') }} — passed as a dbt variable at runtime

No filtering. No quarantine. All Bronze transaction code records are promoted.
No transformation beyond adding audit columns.

INVARIANT — INV-33: Silver transaction_codes must contain at least one record. After the model runs, confirm row count > 0. Add a post-hook that raises if count = 0.

Commit:
  git add dbt_project/models/silver/silver_transaction_codes.sql
  git commit -m "4.1 — Silver Transaction Codes Model: Bronze promotion with audit columns"
```

**Test cases:**
- Happy path: model runs, Silver row count = Bronze row count.
- Happy path: re-run produces identical output (dbt table materialisation = full replace).
- Failure case: Bronze file absent → dbt model fails clearly (no silent empty output).
- INV-33 check: Silver row count > 0 after run.

**Verification command:**
```bash
docker compose run --rm pipeline dbt run --project-dir /app/dbt_project --profiles-dir /app/dbt_project --select silver_transaction_codes --vars '{"run_id": "test-run-s4"}'
duckdb -c "SELECT count(*) FROM read_parquet('data/silver/transaction_codes/data.parquet');"
duckdb -c "SELECT _source_file, _pipeline_run_id FROM read_parquet('data/silver/transaction_codes/data.parquet') LIMIT 2;"
duckdb -c "SELECT count(*) FROM read_parquet('data/bronze/transaction_codes/data.parquet');"
```

**Invariant enforcement (code review):** INV-33 — confirm post-hook or post-model check raises on zero rows; confirm no filtering logic that could reduce row count below Bronze.

---

### Task 4.2 — Silver Accounts dbt Model (Upsert Logic)

**Inputs:** Bronze accounts partition for a given date, existing Silver accounts file (if any), Silver audit columns spec (brief Section 4.2.2).
**Outputs:** `models/silver/silver_accounts.sql` upserts delta records into Silver. account_id is the upsert key. Existing records replaced by newer version. Single non-partitioned output file.

**CC Prompt:**
```
Implement dbt_project/models/silver/silver_accounts.sql.

Source: bronze accounts partition for the date being processed.
  Path: /app/data/bronze/accounts/date={{ var('process_date') }}/data.parquet
Output: /app/data/silver/accounts/data.parquet (single non-partitioned file)

Upsert logic (account_id is the key):
  1. Read existing Silver accounts (empty relation if file absent).
  2. Read incoming Bronze accounts delta for process_date.
  3. Filter incoming delta: only promote records that pass INV validation (NULL_REQUIRED_FIELD and INVALID_ACCOUNT_STATUS checks are in Task 5 — for now, promote all records and add a TODO comment).
  4. Merge: for any account_id in the incoming delta, replace the existing record. For account_ids not in the delta, keep the existing record.
  5. Output: union of (existing records not in delta) + (incoming delta records).

Silver audit columns:
  _source_file: carried from Bronze _source_file
  _bronze_ingested_at: carried from Bronze _ingested_at
  _pipeline_run_id: {{ var('run_id') }}
  _record_valid_from: current_timestamp (when this version became current in Silver)

Note: Quarantine routing for accounts is added in Session 5. This task implements the upsert merge only.

INVARIANT — INV-17: Silver Accounts contains exactly one record per account_id. Confirm the merge logic produces no duplicates — the output must have count(*) = count(DISTINCT account_id). Add a post-hook that asserts this condition.

INVARIANT — INV-18: Running Silver Accounts promotion twice on identical input state produces identical output. Confirm the merge is deterministic — no timestamp-based tie-breaking that differs between runs, no ORDER BY RANDOM().

Commit:
  git add dbt_project/models/silver/silver_accounts.sql
  git commit -m "4.2 — Silver Accounts Model: upsert merge with uniqueness assertion"
```

**Test cases:**
- Happy path (first run): Silver accounts written with correct row count.
- Happy path (delta): new account_id in delta → added to Silver.
- Happy path (update): existing account_id in delta with changed credit_limit → Silver record updated.
- Idempotency: run twice on same input → same row count, same field values.
- INV-17 check: `count(*) = count(DISTINCT account_id)` in Silver after any run.

**Verification command:**
```bash
docker compose run --rm pipeline dbt run --project-dir /app/dbt_project --profiles-dir /app/dbt_project --select silver_accounts --vars '{"run_id": "test-s4", "process_date": "2024-01-15"}'
duckdb -c "SELECT count(*) AS total, count(DISTINCT account_id) AS unique_accts FROM read_parquet('data/silver/accounts/data.parquet');"
# Run again — counts must be identical
docker compose run --rm pipeline dbt run --project-dir /app/dbt_project --profiles-dir /app/dbt_project --select silver_accounts --vars '{"run_id": "test-s4-b", "process_date": "2024-01-15"}'
duckdb -c "SELECT count(*) AS total, count(DISTINCT account_id) AS unique_accts FROM read_parquet('data/silver/accounts/data.parquet');"
```

**Invariant enforcement (code review):** INV-17 — confirm post-hook assertion; INV-18 — confirm no non-deterministic expressions (CURRENT_TIMESTAMP in a tie-break, random sorts).

---

### Task 4.3 — Silver Accounts Quarantine Routing

**Inputs:** `silver_accounts.sql` (Task 4.2), quality rules (brief Section 5.2), quarantine schema (brief Section 4.2.4).
**Outputs:** `silver_accounts.sql` updated to route NULL_REQUIRED_FIELD and INVALID_ACCOUNT_STATUS records to quarantine before upsert. `models/silver/silver_accounts_quarantine.sql` writes quarantine records partitioned by date.

**CC Prompt:**
```
Add quarantine routing to silver_accounts.sql and create silver_accounts_quarantine.sql.

Before the upsert merge in silver_accounts.sql, apply these rejection rules to incoming Bronze delta records:

Rejection rules for accounts (brief Section 5.2):
  NULL_REQUIRED_FIELD: account_id, open_date, credit_limit, current_balance,
                       billing_cycle_start, billing_cycle_end, or account_status is null or empty string.
  INVALID_ACCOUNT_STATUS: account_status not in ('ACTIVE', 'SUSPENDED', 'CLOSED').

Routing logic:
  - Records failing any rule → quarantine (do not upsert into Silver).
  - Records passing all rules → upsert merge (existing Silver Accounts logic).
  - A record failing multiple rules gets only one rejection_reason — the first that matches in this order:
      1. NULL_REQUIRED_FIELD, 2. INVALID_ACCOUNT_STATUS.

silver_accounts_quarantine.sql:
  Output path: /app/data/silver/quarantine/date={{ var('process_date') }}/rejected.parquet
  Columns: all source columns from Bronze + quarantine audit columns:
    _source_file (carried from Bronze)
    _pipeline_run_id: {{ var('run_id') }}
    _rejected_at: current_timestamp
    _rejection_reason: the rejection code string

INVARIANT — INV-06 (GLOBAL): Every Bronze accounts record is routed to exactly one outcome: Silver upsert or quarantine. Confirm there is no third path — no record can be both promoted and quarantined, and no record can be silently dropped (lost without being routed anywhere).

INVARIANT — INV-08: Every quarantine record has _source_file matching the Bronze partition and all source field values are identical to the Bronze record (no transformation).

INVARIANT — INV-09: _rejection_reason is one of: NULL_REQUIRED_FIELD, INVALID_ACCOUNT_STATUS. No other value is permitted. Confirm the routing SQL uses string literals matching exactly these codes.

Commit:
  git add dbt_project/models/silver/silver_accounts.sql dbt_project/models/silver/silver_accounts_quarantine.sql
  git commit -m "4.3 — Silver Accounts Quarantine: NULL and status rejection routing"
```

**Test cases:**
- Happy path: valid accounts promoted, invalid accounts quarantined.
- Null account_id: quarantined with NULL_REQUIRED_FIELD.
- Invalid status "PENDING": quarantined with INVALID_ACCOUNT_STATUS.
- Conservation check: Bronze row count = Silver upserted + Quarantine.
- Quarantine audit: `_source_file` matches Bronze, `_rejection_reason` from valid set.

**Verification command:**
```bash
docker compose run --rm pipeline dbt run --project-dir /app/dbt_project --profiles-dir /app/dbt_project --select silver_accounts silver_accounts_quarantine --vars '{"run_id": "test-s4", "process_date": "2024-01-15"}'
duckdb -c "
WITH bronze AS (SELECT count(*) AS n FROM read_parquet('data/bronze/accounts/date=2024-01-15/data.parquet')),
     silver AS (SELECT count(*) AS n FROM read_parquet('data/silver/accounts/data.parquet')),
     quarantine AS (SELECT count(*) AS n FROM read_parquet('data/silver/quarantine/date=2024-01-15/rejected.parquet') WHERE _source_file LIKE '%accounts%')
SELECT bronze.n AS bronze, silver.n AS silver, quarantine.n AS quarantine FROM bronze, silver, quarantine;
"
duckdb -c "SELECT DISTINCT _rejection_reason FROM read_parquet('data/silver/quarantine/date=2024-01-15/rejected.parquet');"
```

**Invariant enforcement (code review):** INV-06 — confirm no record can be in both Silver and quarantine output; INV-09 — rejection codes are string literals from the valid set.

---

### Task 4.4 — Silver Model Integration: pipeline.py wiring (Codes and Accounts)

**Inputs:** `pipeline_control.py`, pipeline.py, dbt models from Tasks 4.1–4.3.
**Outputs:** pipeline.py `run_historical` calls `dbt run --select silver_transaction_codes` and `dbt run --select silver_accounts silver_accounts_quarantine` via `subprocess`. Run log entries written for each. INV-33 pre-condition check added before silver_transactions (placeholder only — silver_transactions not yet implemented).

**CC Prompt:**
```
Wire Silver transaction codes and accounts dbt models into pipeline.py run_historical.

After the Bronze loader calls, add these dbt invocations using subprocess.run():
  1. dbt run --select silver_transaction_codes --vars '{"run_id": "<run_id>"}' 
  2. dbt run --select silver_accounts silver_accounts_quarantine --vars '{"run_id": "<run_id>", "process_date": "<date>"}'

For each dbt model execution:
- Capture stdout/stderr from subprocess.
- Parse target/run_results.json after each dbt run to extract:
    rows_affected → records_written
    error message if status != "pass"
- Call append_run_log_entry with model_name matching the dbt model name, appropriate layer="SILVER".

INV-33 pre-condition check (placeholder): After silver_transaction_codes runs, read Silver transaction_codes row count. If count = 0: raise RuntimeError("INV-33 violated: Silver transaction_codes is empty before transaction promotion"). Silver transactions are not yet implemented — this check is a structural placeholder for Session 5.

Subprocess command template:
  subprocess.run(["dbt", "run", "--project-dir", str(DBT_PROJECT_DIR),
                  "--profiles-dir", str(DBT_PROJECT_DIR),
                  "--select", "<model>",
                  "--vars", f'{{"run_id": "{run_id}", "process_date": "{date}"}}'],
                 check=True, capture_output=True, text=True)

INVARIANT — INV-33: Silver transaction_codes row count must be > 0 before any silver_transactions promotion begins. Confirm the check is present and raises on violation.

Commit:
  git add pipeline.py
  git commit -m "4.4 — Silver Codes/Accounts Wiring: dbt calls, run log entries, INV-33 pre-check"
```

**Test cases:**
- Happy path: pipeline run produces Silver codes and accounts, run log has Silver entries with status=SUCCESS.
- Failure case: dbt model fails → run log entry status=FAILED, exception re-raised.
- INV-33: Silver codes row count 0 → RuntimeError raised before any Silver transactions step.

**Verification command:**
```bash
rm -rf data/silver data/pipeline
docker compose run --rm pipeline python pipeline.py historical --start-date 2024-01-15 --end-date 2024-01-21
duckdb -c "SELECT count(*) FROM read_parquet('data/silver/transaction_codes/data.parquet');"
duckdb -c "SELECT count(*) FROM read_parquet('data/silver/accounts/data.parquet');"
duckdb -c "SELECT model_name, status FROM read_parquet('data/pipeline/run_log.parquet') WHERE layer='SILVER' ORDER BY started_at;"
```

**Invariant enforcement (code review):** INV-33 check location must be after silver_transaction_codes write and before any silver_transactions invocation, not after.

---

**Session 4 Integration Check:**
```bash
docker compose run --rm pipeline dbt run --project-dir /app/dbt_project --profiles-dir /app/dbt_project --select silver_transaction_codes silver_accounts silver_accounts_quarantine --vars '{"run_id": "s4-check", "process_date": "2024-01-15"}'
duckdb -c "SELECT count(*) FROM read_parquet('data/silver/transaction_codes/data.parquet');"
duckdb -c "SELECT count(*) AS total, count(DISTINCT account_id) AS unique_accts FROM read_parquet('data/silver/accounts/data.parquet');"
```

---

## Session 5 — Silver: Transactions Promotion (Validation and Quarantine)

**Session goal:** Silver transactions promotion validates all quality rules, routes failed records to quarantine with correct codes, and promotes passing records with all audit columns. Conservation check passes. After this session: `dbt run --select silver_transactions silver_quarantine` processes one date correctly.

**Branch slug:** `session/5-silver-transactions-validation`

**Integration check:**
```bash
docker compose run --rm pipeline dbt run --project-dir /app/dbt_project --profiles-dir /app/dbt_project --select silver_transactions silver_quarantine --vars '{"run_id": "s5-check", "process_date": "2024-01-15"}'
duckdb -c "
WITH b AS (SELECT count(*) n FROM read_parquet('data/bronze/transactions/date=2024-01-15/data.parquet')),
     s AS (SELECT count(*) n FROM read_parquet('data/silver/transactions/date=2024-01-15/data.parquet')),
     q AS (SELECT count(*) n FROM read_parquet('data/silver/quarantine/date=2024-01-15/rejected.parquet') WHERE _source_file LIKE '%transactions%')
SELECT b.n bronze, s.n silver, q.n quarantine, b.n - s.n - q.n AS gap FROM b, s, q;
"
```
Gap must be 0.

---

### Task 5.1 — Silver Transactions: NULL and Amount Validation

**Inputs:** Bronze transactions partition, Silver transaction_codes (prerequisite), requirements brief Section 5.1.
**Outputs:** `silver_transactions.sql` stub with NULL_REQUIRED_FIELD and INVALID_AMOUNT rejection logic. Rejected records written to quarantine. Passing records forwarded to next validation step (still in the same model).

**CC Prompt:**
```
Begin implementing dbt_project/models/silver/silver_transactions.sql — NULL and amount validation only.

Source: /app/data/bronze/transactions/date={{ var('process_date') }}/data.parquet
Output (Silver): /app/data/silver/transactions/date={{ var('process_date') }}/data.parquet
Output (Quarantine): /app/data/silver/quarantine/date={{ var('process_date') }}/rejected.parquet

Structure the model as a series of CTEs:

CTE 1 — bronze_src: read all Bronze transactions for process_date.

CTE 2 — null_check: classify each record.
  NULL_REQUIRED_FIELD condition: transaction_id IS NULL OR account_id IS NULL OR
    transaction_date IS NULL OR amount IS NULL OR transaction_code IS NULL OR channel IS NULL OR
    TRIM(transaction_id) = '' OR TRIM(account_id) = '' OR TRIM(transaction_code) = '' OR TRIM(channel) = ''
  Records failing: _rejection_reason = 'NULL_REQUIRED_FIELD', goes to quarantine.
  Records passing: carry forward to next CTE.

CTE 3 — amount_check: applied only to records passing null_check.
  INVALID_AMOUNT condition: amount <= 0 OR amount IS NULL (belt-and-suspenders after null_check)
  Records failing: _rejection_reason = 'INVALID_AMOUNT', goes to quarantine.
  Records passing: carry forward.

Final output for this task: two disjoint sets — quarantine_candidates (NULL or invalid amount) and valid_so_far (passing both checks). Do not write final Silver or quarantine yet — those come in Tasks 5.2–5.4.

INVARIANT — INV-06 (GLOBAL): Every Bronze record must be routed to exactly one outcome. Structure CTEs so that every record from bronze_src appears in exactly one of quarantine_candidates or valid_so_far.

INVARIANT — INV-09: _rejection_reason values used: 'NULL_REQUIRED_FIELD', 'INVALID_AMOUNT'. No other values in this task.

Commit:
  git add dbt_project/models/silver/silver_transactions.sql
  git commit -m "5.1 — Silver Transactions: NULL and amount validation CTEs"
```

**Test cases:**
- Null transaction_id → NULL_REQUIRED_FIELD bucket.
- Empty string transaction_id → NULL_REQUIRED_FIELD bucket (TRIM check).
- Amount = 0 → INVALID_AMOUNT bucket.
- Amount = -5.00 → INVALID_AMOUNT bucket.
- Valid record → valid_so_far bucket, not in quarantine.
- Conservation: count(bronze_src) = count(quarantine_candidates) + count(valid_so_far).

**Verification command:**
```bash
docker compose run --rm pipeline dbt compile --project-dir /app/dbt_project --profiles-dir /app/dbt_project --select silver_transactions
# Inspect compiled SQL to verify CTE structure
cat dbt_project/target/compiled/credit_card_lake/models/silver/silver_transactions.sql | head -80
```

**Invariant enforcement (code review):** INV-06 — confirm every record from bronze_src appears in exactly one output CTE — no LEFT JOIN that could silently drop records.

---

### Task 5.2 — Silver Transactions: Transaction Code and Channel Validation

**Inputs:** `silver_transactions.sql` (Task 5.1), Silver transaction_codes file.
**Outputs:** Two more CTEs added: `code_check` (INVALID_TRANSACTION_CODE) and `channel_check` (INVALID_CHANNEL). Deduplication and account resolution are added in Task 5.3.

**CC Prompt:**
```
Extend silver_transactions.sql with transaction code and channel validation CTEs.

Continue from valid_so_far CTE produced in Task 5.1.

CTE 4 — code_check: join valid_so_far against Silver transaction_codes.
  Source: /app/data/silver/transaction_codes/data.parquet
  INVALID_TRANSACTION_CODE condition: no matching row in Silver transaction_codes for transaction_code.
  Use LEFT JOIN on transaction_code, then filter WHERE tc.transaction_code IS NULL.
  Records failing: _rejection_reason = 'INVALID_TRANSACTION_CODE', added to quarantine set.
  Records passing: carry forward with debit_credit_indicator from the join.

CTE 5 — channel_check: applied to records passing code_check.
  INVALID_CHANNEL condition: channel NOT IN ('ONLINE', 'IN_STORE')
  Records failing: _rejection_reason = 'INVALID_CHANNEL', added to quarantine set.
  Records passing: carry forward.

INVARIANT — INV-11: Every Silver transaction references a valid transaction_code present in Silver transaction_codes. Confirm the rejection of non-matching codes ensures no Silver record has an invalid code.

INVARIANT — INV-33: Silver transaction_codes must contain at least one record before this model runs. This is enforced by pipeline.py pre-condition check (Task 4.4). Here, confirm the model will fail (not produce empty output) if Silver transaction_codes is absent — the LEFT JOIN will produce NULL tc fields for all records, causing all to be quarantined as INVALID_TRANSACTION_CODE.

INVARIANT — INV-09: _rejection_reason values in this task: 'INVALID_TRANSACTION_CODE', 'INVALID_CHANNEL'. Confirm these are string literals matching exactly.

Commit:
  git add dbt_project/models/silver/silver_transactions.sql
  git commit -m "5.2 — Silver Transactions: code and channel validation CTEs"
```

**Test cases:**
- Unknown transaction_code → INVALID_TRANSACTION_CODE quarantine.
- channel = "ATM" → INVALID_CHANNEL quarantine.
- Valid code + valid channel → passes, debit_credit_indicator populated from join.
- Conservation: quarantine_so_far + valid_so_far = bronze_src count (cumulative check).

**Verification command:**
```bash
docker compose run --rm pipeline dbt compile --project-dir /app/dbt_project --profiles-dir /app/dbt_project --select silver_transactions
# Check compiled SQL shows correct join and filter logic
grep -A 5 "INVALID_TRANSACTION_CODE\|INVALID_CHANNEL" dbt_project/target/compiled/credit_card_lake/models/silver/silver_transactions.sql
```

**Invariant enforcement (code review):** INV-11 — confirm LEFT JOIN + NULL filter is used, not a subquery that could pass a non-matching code; INV-09 — string literals exact match.

---

### Task 5.3 — Silver Transactions: _signed_amount Derivation and Audit Columns

**Inputs:** `silver_transactions.sql` (Task 5.2), debit_credit_indicator from transaction code join.
**Outputs:** `_signed_amount` derived per INV-13. All Silver audit columns added. Final Silver-bound records assembled (before deduplication, which is added in Session 6).

**CC Prompt:**
```
Extend silver_transactions.sql to derive _signed_amount and add all Silver audit columns.

Continue from records passing channel_check (valid channel, valid code, non-null, valid amount).

CTE 6 — sign_assignment:
  _signed_amount derivation:
    IF debit_credit_indicator = 'DR' THEN _signed_amount = amount (positive — DR increases balance)
    IF debit_credit_indicator = 'CR' THEN _signed_amount = -amount (negative — CR decreases balance)
    ELSE: this case should never occur (code_check already validated the code exists in transaction_codes,
          and transaction_codes.debit_credit_indicator is only DR or CR per the source data contract).
          Add a CASE WHEN NULL to make the SQL complete — any other value produces NULL _signed_amount.

  Post-derivation assertion: _signed_amount IS NOT NULL AND _signed_amount != 0
  Records where assertion fails: _rejection_reason = 'INVALID_AMOUNT' (zero or null derived amount).
  Route to quarantine. This handles the edge case of a valid source amount that somehow produces zero.

Silver audit columns to add (carried or computed):
  _source_file: carried from Bronze _source_file
  _bronze_ingested_at: carried from Bronze _ingested_at
  _pipeline_run_id: {{ var('run_id') }}
  _promoted_at: current_timestamp
  _is_resolvable: TRUE as default — account resolution is in Session 6 (Task 6.1)

INVARIANT — INV-12: Every Silver transaction has a non-null _signed_amount. The post-derivation assertion enforces this. Confirm the assertion routes null _signed_amount to quarantine, not to Silver.

INVARIANT — INV-13: debit_credit_indicator is 'DR' or 'CR'. _signed_amount is non-null, non-zero. If DR then _signed_amount > 0. If CR then _signed_amount < 0. Confirm the CASE statement produces these sign rules with no exceptions.

Commit:
  git add dbt_project/models/silver/silver_transactions.sql
  git commit -m "5.3 — Silver Transactions: signed amount derivation and audit columns"
```

**Test cases:**
- DR record with amount=50.00 → _signed_amount = +50.00.
- CR record with amount=50.00 → _signed_amount = -50.00.
- Valid source amount that produces 0 after derivation → INVALID_AMOUNT quarantine.
- All Silver audit columns present and non-null.

**Verification command:**
```bash
docker compose run --rm pipeline dbt compile --project-dir /app/dbt_project --profiles-dir /app/dbt_project --select silver_transactions
grep -A 3 "_signed_amount\|debit_credit" dbt_project/target/compiled/credit_card_lake/models/silver/silver_transactions.sql | head -30
```

**Invariant enforcement (code review):** INV-12 — null _signed_amount routes to quarantine, not Silver; INV-13 — CASE covers only DR and CR; no ELSE produces a valid amount.

---

### Task 5.4 — Silver Quarantine Write and Conservation Check

**Inputs:** `silver_transactions.sql` (Task 5.3), quarantine schema (brief Section 4.2.4).
**Outputs:** All quarantine records from all validation CTEs are unioned and written to `silver/quarantine/date=YYYY-MM-DD/rejected.parquet`. Conservation assertion implemented. This completes the validation half of silver_transactions — deduplication is Session 6.

**CC Prompt:**
```
Complete the quarantine write in silver_transactions.sql and add the conservation check.

Quarantine union CTE — quarantine_all:
  UNION ALL all quarantine candidate sets from CTEs 2, 3, 4, 5, and 6:
    null_check rejects (NULL_REQUIRED_FIELD)
    amount_check rejects (INVALID_AMOUNT)
    code_check rejects (INVALID_TRANSACTION_CODE)
    channel_check rejects (INVALID_CHANNEL)
    sign_assignment rejects (INVALID_AMOUNT — zero derived)

Quarantine output columns:
  All original source columns from Bronze (transaction_id, account_id, transaction_date, amount, transaction_code, merchant_name, channel)
  _source_file: carried from Bronze
  _pipeline_run_id: {{ var('run_id') }}
  _rejected_at: current_timestamp
  _rejection_reason: the rejection code from each CTE

Write quarantine to: /app/data/silver/quarantine/date={{ var('process_date') }}/rejected.parquet

Conservation assertion (add as a dbt post-hook or a separate test):
  bronze_count = SELECT count(*) FROM read_parquet('/app/data/bronze/transactions/date=.../data.parquet')
  silver_count = SELECT count(*) FROM silver output (pre-dedup — all passing records)
  quarantine_count = SELECT count(*) FROM quarantine output
  ASSERT bronze_count = silver_count + quarantine_count — raise on violation

INVARIANT — INV-07: Bronze row count = Silver promoted + Quarantine. Confirm the conservation check is a hard assertion (raises, not logs) and covers all quarantine records from all CTEs.

INVARIANT — INV-08: Every quarantine record has _source_file matching a Bronze partition. Confirm _source_file is carried verbatim from Bronze, not re-derived.

INVARIANT — INV-09: _rejection_reason for all quarantine records is in the valid set. Confirm the quarantine_all union uses only valid code strings.

Commit:
  git add dbt_project/models/silver/silver_transactions.sql
  git commit -m "5.4 — Silver Quarantine Write: union, write, conservation assertion"
```

**Test cases:**
- Conservation: Bronze count = Silver (pre-dedup) count + Quarantine count. Gap = 0.
- Quarantine `_source_file` non-null and matches Bronze partition filename.
- All quarantine `_rejection_reason` values are from the valid set.

**Verification command:**
```bash
docker compose run --rm pipeline dbt run --project-dir /app/dbt_project --profiles-dir /app/dbt_project --select silver_transactions silver_quarantine --vars '{"run_id": "s5-check", "process_date": "2024-01-15"}'
duckdb -c "
WITH b AS (SELECT count(*) n FROM read_parquet('data/bronze/transactions/date=2024-01-15/data.parquet')),
     s AS (SELECT count(*) n FROM read_parquet('data/silver/transactions/date=2024-01-15/data.parquet')),
     q AS (SELECT count(*) n FROM read_parquet('data/silver/quarantine/date=2024-01-15/rejected.parquet') WHERE _source_file LIKE '%transactions%')
SELECT b.n bronze, s.n silver, q.n quarantine, b.n - s.n - q.n gap FROM b, s, q;
"
duckdb -c "SELECT DISTINCT _rejection_reason FROM read_parquet('data/silver/quarantine/date=2024-01-15/rejected.parquet');"
```

**Invariant enforcement (code review):** INV-07 — confirm the conservation assertion is a hard raise, not a log; INV-08 — _source_file is carried, not re-derived; INV-09 — all rejection code strings match the valid set exactly.

---

### Task 5.5 — Silver Transactions Run Log Wiring

**Inputs:** pipeline.py (Task 4.4), subprocess call pattern for dbt.
**Outputs:** pipeline.py logs one run log entry for silver_transactions and one for silver_quarantine. records_rejected populated from the quarantine count after dbt run.

**CC Prompt:**
```
Add run log entries for silver_transactions and silver_accounts_quarantine to pipeline.py.

After the dbt run for silver_transaction_codes and silver_accounts, add:
  dbt run --select silver_transactions silver_quarantine --vars ...

For each dbt model (silver_transactions, silver_quarantine):
  - Parse run_results.json for rows_affected and status.
  - For silver_transactions: records_processed = bronze_transactions_row_count(date), records_written = Silver row count, records_rejected = quarantine row count for that date.
  - For silver_quarantine: records_processed = quarantine row count, records_written = quarantine row count, records_rejected = None.
  - Call append_run_log_entry with layer = "SILVER".

Commit:
  git add pipeline.py
  git commit -m "5.5 — Silver Transactions Run Log: records_processed, written, rejected populated"
```

**Test cases:**
- Run log has silver_transactions entry with correct records_rejected count.
- Records_processed = Bronze row count for that date.
- Records_written = Silver row count.

**Verification command:**
```bash
rm -f data/pipeline/run_log.parquet
docker compose run --rm pipeline python pipeline.py historical --start-date 2024-01-15 --end-date 2024-01-21
duckdb -c "SELECT model_name, records_processed, records_written, records_rejected FROM read_parquet('data/pipeline/run_log.parquet') WHERE model_name='silver_transactions';"
```

**Invariant enforcement:** INV-31 — one run log row per model execution, all required fields non-null.

---

**Session 5 Integration Check:** (see top of session — conservation check command)

---

## Session 6 — Silver: Deduplication and Account Resolution

**Session goal:** Two-step deduplication fully implemented. _is_resolvable flag set correctly. INV-10, INV-14, INV-15, INV-16 all enforced. After this session: Silver transactions are globally unique and correctly flagged for account resolvability.

**Branch slug:** `session/6-silver-dedup-account-resolution`

**Integration check:**
```bash
docker compose run --rm pipeline dbt run --project-dir /app/dbt_project --profiles-dir /app/dbt_project --select silver_transactions silver_quarantine --vars '{"run_id": "s6-check", "process_date": "2024-01-15"}'
duckdb -c "SELECT count(*) - count(DISTINCT transaction_id) AS duplicates FROM read_parquet('data/silver/transactions/**/*.parquet');"
duckdb -c "SELECT count(*) FILTER (WHERE _rejection_reason = 'UNRESOLVABLE_ACCOUNT_ID') AS bad_quarantine FROM read_parquet('data/silver/quarantine/**/*.parquet');"
```
duplicates must be 0. bad_quarantine must be 0.

---

### Task 6.1 — Within-Batch Deduplication

**Inputs:** `silver_transactions.sql` (Task 5.4), requirements brief Section 4.2.1 deduplication spec.
**Outputs:** Within-batch dedup CTE added before Silver write. Duplicate transaction_ids within the Bronze source file for date D → second occurrence quarantined as DUPLICATE_TRANSACTION_ID.

**CC Prompt:**
```
Add within-batch deduplication to silver_transactions.sql.

Insert a new CTE after sign_assignment (Task 5.3 output) and before the quarantine union:

CTE — within_batch_dedup:
  Input: all records passing validation so far (not yet quarantined).
  Use ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY _bronze_ingested_at) AS rn.
  Records where rn = 1: proceed to cross-partition check (first occurrence kept).
  Records where rn > 1: _rejection_reason = 'DUPLICATE_TRANSACTION_ID', added to quarantine_all.

Add these records to the quarantine_all union in Task 5.4.

INVARIANT — INV-10: Each transaction_id appears at most once across all Silver partitions. This CTE handles intra-file duplicates. Cross-partition deduplication is Task 6.2.

INVARIANT — INV-09: 'DUPLICATE_TRANSACTION_ID' must be the exact rejection reason string for within-batch duplicates.

Commit:
  git add dbt_project/models/silver/silver_transactions.sql
  git commit -m "6.1 — Within-Batch Deduplication: ROW_NUMBER duplicate detection and quarantine"
```

**Test cases:**
- Two records with same transaction_id in same source file → second quarantined as DUPLICATE_TRANSACTION_ID, first promoted.
- Unique transaction_ids → all pass within-batch step without quarantine.
- Conservation still holds after adding this step.

**Verification command:**
```bash
docker compose run --rm pipeline dbt run --project-dir /app/dbt_project --profiles-dir /app/dbt_project --select silver_transactions silver_quarantine --vars '{"run_id": "s6-1", "process_date": "2024-01-15"}'
duckdb -c "SELECT _rejection_reason, count(*) FROM read_parquet('data/silver/quarantine/date=2024-01-15/rejected.parquet') GROUP BY 1;"
```

**Invariant enforcement (code review):** INV-10 — ROW_NUMBER partition is on transaction_id only; ORDER BY is deterministic (not random); rn > 1 records routed to quarantine.

---

### Task 6.2 — Cross-Partition Deduplication

**Inputs:** `silver_transactions.sql` (Task 6.1), all existing Silver transaction partitions.
**Outputs:** Records surviving within-batch dedup are checked against all existing Silver partitions. Any transaction_id already present in Silver → quarantined as DUPLICATE_TRANSACTION_ID.

**CC Prompt:**
```
Add cross-partition deduplication to silver_transactions.sql.

After within_batch_dedup CTE, insert:

CTE — existing_silver_ids:
  SELECT DISTINCT transaction_id FROM read_parquet('/app/data/silver/transactions/**/*.parquet')
  Use glob pattern to scan all existing Silver partitions.
  If no partitions exist (first ever run): this query returns empty — the anti-join produces no quarantine records. Handle empty result gracefully (LEFT ANTI JOIN naturally handles this).

CTE — cross_partition_dedup:
  Input: records where rn = 1 from within_batch_dedup.
  LEFT ANTI JOIN on existing_silver_ids.transaction_id.
  Records with a match in existing_silver_ids: _rejection_reason = 'DUPLICATE_TRANSACTION_ID', quarantine.
  Records with no match: proceed to _is_resolvable check (Task 6.3).

Add cross-partition rejects to quarantine_all union.

INVARIANT — INV-10: Combined with within-batch dedup (Task 6.1), this ensures no transaction_id appears more than once across all Silver partitions after promotion.

INVARIANT — INV-16: Silver partition date isolation — partition path for survivors is derived from transaction_date field, not from process_date argument. Add explicit check: if transaction_date != process_date for a record, route to quarantine. Note: this edge case is unlikely given source isolation (INV-05), but the Silver model must not rely on the Bronze source to have enforced it.

Commit:
  git add dbt_project/models/silver/silver_transactions.sql
  git commit -m "6.2 — Cross-Partition Dedup: anti-join against all Silver partitions"
```

**Test cases:**
- First run on a date: no existing Silver partitions → cross-partition step produces no rejects, all survivors promoted.
- Re-run after promotion: all transaction_ids already in Silver → all incoming records quarantined as DUPLICATE_TRANSACTION_ID.
- Record with transaction_date != process_date → quarantined (INV-16 guard).

**Verification command:**
```bash
# First run
docker compose run --rm pipeline dbt run --project-dir /app/dbt_project --profiles-dir /app/dbt_project --select silver_transactions silver_quarantine --vars '{"run_id": "s6-2a", "process_date": "2024-01-15"}'
FIRST_COUNT=$(duckdb -csv -noheader -c "SELECT count(*) FROM read_parquet('data/silver/transactions/date=2024-01-15/data.parquet');")
# Second run — should not add new Silver records
docker compose run --rm pipeline dbt run --project-dir /app/dbt_project --profiles-dir /app/dbt_project --select silver_transactions silver_quarantine --vars '{"run_id": "s6-2b", "process_date": "2024-01-15"}'
SECOND_COUNT=$(duckdb -csv -noheader -c "SELECT count(*) FROM read_parquet('data/silver/transactions/date=2024-01-15/data.parquet');")
echo "First: $FIRST_COUNT, Second: $SECOND_COUNT — must be equal"
duckdb -c "SELECT count(*) - count(DISTINCT transaction_id) AS duplicates FROM read_parquet('data/silver/transactions/**/*.parquet');"
```

**Invariant enforcement (code review):** INV-10 — anti-join covers ALL Silver partitions via glob, not just current date; INV-16 — partition path uses transaction_date from record, not process_date variable.

---

### Task 6.3 — Account Resolution and _is_resolvable Flag

**Inputs:** `silver_transactions.sql` (Task 6.2), Silver Accounts file.
**Outputs:** `_is_resolvable` flag set correctly. Records with unknown account_id → Silver with _is_resolvable = false (not quarantined). INV-14 and INV-15 enforced.

**CC Prompt:**
```
Add account resolution to silver_transactions.sql.

After cross_partition_dedup (surviving records — not quarantined), insert:

CTE — account_resolution:
  LEFT JOIN Silver Accounts (/app/data/silver/accounts/data.parquet) on account_id.
  _is_resolvable = TRUE if Silver Accounts has a matching account_id.
  _is_resolvable = FALSE if no match (account unknown at promotion time).

These records go to Silver regardless of _is_resolvable value.
A record with _is_resolvable = FALSE must NOT go to quarantine.
UNRESOLVABLE_ACCOUNT_ID is NOT a valid quarantine reason code — confirm it does not appear anywhere in the quarantine routing.

All surviving records (resolvable and unresolvable) enter the final Silver write CTE with their correct _is_resolvable value.

INVARIANT — INV-14: Every Silver transaction with an account_id not found in Silver Accounts at promotion time has _is_resolvable = false. Confirm LEFT JOIN produces NULL for unresolved accounts and _is_resolvable is set to FALSE for those.

INVARIANT — INV-15: No quarantine record has _rejection_reason = 'UNRESOLVABLE_ACCOUNT_ID'. Confirm this code does not appear in any CTE. Confirm unresolvable transactions route to Silver, not quarantine.

Commit:
  git add dbt_project/models/silver/silver_transactions.sql
  git commit -m "6.3 — Account Resolution: _is_resolvable flag, unresolvable to Silver not quarantine"
```

**Test cases:**
- Transaction with known account_id → Silver with _is_resolvable = true.
- Transaction with unknown account_id → Silver with _is_resolvable = false.
- No quarantine record has UNRESOLVABLE_ACCOUNT_ID as rejection reason.
- Conservation still holds (unresolvable records count toward Silver, not quarantine).

**Verification command:**
```bash
docker compose run --rm pipeline dbt run --project-dir /app/dbt_project --profiles-dir /app/dbt_project --select silver_transactions silver_quarantine --vars '{"run_id": "s6-3", "process_date": "2024-01-15"}'
duckdb -c "SELECT _is_resolvable, count(*) FROM read_parquet('data/silver/transactions/date=2024-01-15/data.parquet') GROUP BY 1;"
duckdb -c "SELECT count(*) FILTER (WHERE _rejection_reason='UNRESOLVABLE_ACCOUNT_ID') FROM read_parquet('data/silver/quarantine/date=2024-01-15/rejected.parquet');"
```

**Invariant enforcement (code review):** INV-14 — LEFT JOIN with NULL check, not INNER JOIN; INV-15 — grep the entire silver_transactions.sql for 'UNRESOLVABLE_ACCOUNT_ID' — it must not appear.

---

### Task 6.4 — Full Conservation and Silver Partition Date Isolation Check

**Inputs:** `silver_transactions.sql` (Task 6.3), bronze_row_count utility (Task 2.5).
**Outputs:** Final conservation assertion added as dbt test or post-hook. Partition path confirmed to use transaction_date from record. INV-07 and INV-16 fully enforced.

**CC Prompt:**
```
Add the final conservation assertion and partition path confirmation to silver_transactions.sql.

Conservation assertion (post-hook or schema test):
  bronze_count = count of /app/data/bronze/transactions/date={{ var('process_date') }}/data.parquet
  silver_count = count of /app/data/silver/transactions/date={{ var('process_date') }}/data.parquet
  quarantine_count = count of /app/data/silver/quarantine/date={{ var('process_date') }}/rejected.parquet WHERE _source_file LIKE '%transactions%'
  ASSERT: bronze_count = silver_count + quarantine_count

If assertion fails: raise RuntimeError with counts in message.

Partition path: confirm the Silver transactions COPY TO path uses transaction_date from the record,
not process_date from the dbt variable. Records where transaction_date != process_date were already
quarantined in Task 6.2 (INV-16 guard), so all survivors should have transaction_date = process_date.
Confirm the path is derived from the field, not the variable, for correctness.

INVARIANT — INV-07: The conservation assertion is a hard raise, not a log. It fires after every Silver promotion run, not only on first run.

INVARIANT — INV-16: Silver partition date isolation — survivors' partition path derives from transaction_date field. Confirmed by the INV-16 guard in Task 6.2 and the path derivation here.

Commit:
  git add dbt_project/models/silver/silver_transactions.sql
  git commit -m "6.4 — Conservation Assertion and Partition Isolation: INV-07 and INV-16 final"
```

**Test cases:**
- Conservation check: gap = 0 on clean run.
- Conservation check: fails hard (raises) if a record was silently dropped.

**Verification command:**
```bash
docker compose run --rm pipeline dbt run --project-dir /app/dbt_project --profiles-dir /app/dbt_project --select silver_transactions silver_quarantine --vars '{"run_id": "s6-4", "process_date": "2024-01-15"}'
duckdb -c "
WITH b AS (SELECT count(*) n FROM read_parquet('data/bronze/transactions/date=2024-01-15/data.parquet')),
     s AS (SELECT count(*) n FROM read_parquet('data/silver/transactions/date=2024-01-15/data.parquet')),
     q AS (SELECT count(*) n FROM read_parquet('data/silver/quarantine/date=2024-01-15/rejected.parquet') WHERE _source_file LIKE '%transactions%')
SELECT b.n bronze, s.n silver, q.n quarantine, b.n - s.n - q.n gap FROM b, s, q;
"
duckdb -c "SELECT count(*) - count(DISTINCT transaction_id) AS duplicates FROM read_parquet('data/silver/transactions/**/*.parquet');"
```

**Invariant enforcement (code review):** INV-07 — conservation is a hard raise; INV-16 — partition path variable vs field derivation.

---

**Session 6 Integration Check:** (see top of session)

---

## Session 7 — Gold Layer

**Session goal:** Both Gold dbt models implemented, fully overwrite on each run, deterministic output, INV-19 through INV-25 and INV-34 enforced. After this session: `dbt run --select gold_daily_summary gold_weekly_account_summary` produces queryable Gold output.

**Branch slug:** `session/7-gold-models`

**Integration check:**
```bash
docker compose run --rm pipeline dbt run --project-dir /app/dbt_project --profiles-dir /app/dbt_project --select gold_daily_summary gold_weekly_account_summary --vars '{"run_id": "s7-check"}'
duckdb -c "SELECT count(*) FROM read_parquet('data/gold/daily_summary/data.parquet');"
duckdb -c "SELECT count(*) FROM read_parquet('data/gold/weekly_account_summary/data.parquet');"
duckdb -c "SELECT count(*) FILTER (WHERE _is_resolvable=false) FROM read_parquet('data/silver/transactions/**/*.parquet') WHERE transaction_date IN (SELECT DISTINCT transaction_date FROM read_parquet('data/gold/daily_summary/data.parquet'));"
```
Last query should not match any Gold records (unresolvable excluded from Gold).

---

### Task 7.1 — Gold Daily Summary Model

**Inputs:** Silver transactions (all partitions), Gold daily_summary schema (brief Section 4.3.1).
**Outputs:** `gold_daily_summary.sql` — one row per transaction_date where _is_resolvable = true. Full overwrite on each run.

**CC Prompt:**
```
Implement dbt_project/models/gold/gold_daily_summary.sql.

Source: read_parquet('/app/data/silver/transactions/**/*.parquet') — all Silver transaction partitions.
Output: /app/data/gold/daily_summary/data.parquet — single file, full overwrite.

Filter: WHERE _is_resolvable = true (unresolvable transactions excluded from all Gold).

Aggregation (GROUP BY transaction_date):
  total_transactions: count(*)
  total_signed_amount: sum(_signed_amount)
  online_transactions: count(*) FILTER (WHERE channel = 'ONLINE')
  instore_transactions: count(*) FILTER (WHERE channel = 'IN_STORE')
  transactions_by_type: not a STRUCT in Parquet — store as separate columns per transaction_type:
    purchases_count, payments_count, fees_count, interest_count, refunds_count
    purchases_amount, payments_amount, fees_amount, interest_amount, refunds_amount

Audit columns:
  _computed_at: current_timestamp
  _pipeline_run_id: {{ var('run_id') }}
  _source_period_start: min(transaction_date) across all Silver
  _source_period_end: max(transaction_date) across all Silver

No source() or ref() to Bronze — only Silver. Confirm no Bronze path exists.

INVARIANT — INV-19: Gold reads only from Silver. Confirm the source path is silver/transactions/**/*.parquet — no bronze/ path anywhere in this model.

INVARIANT — INV-21: Exactly one row per transaction_date. GROUP BY transaction_date with no additional grouping keys.

INVARIANT — INV-24: Full overwrite — dbt model configured with materialized: table (full replace). No append.

INVARIANT — INV-34: WHERE _is_resolvable = true applied in the source query. No Gold record is backed by an unresolvable transaction.

Commit:
  git add dbt_project/models/gold/gold_daily_summary.sql
  git commit -m "7.1 — Gold Daily Summary: aggregation, resolvable-only filter, full overwrite"
```

**Test cases:**
- One row per distinct transaction_date where resolvable Silver transactions exist.
- total_signed_amount matches SUM(_signed_amount) from Silver for that date where _is_resolvable = true.
- No row for a date with only unresolvable transactions.
- Re-run produces identical row count and values.

**Verification command:**
```bash
docker compose run --rm pipeline dbt run --project-dir /app/dbt_project --profiles-dir /app/dbt_project --select gold_daily_summary --vars '{"run_id": "s7-1"}'
duckdb -c "SELECT count(*) FROM read_parquet('data/gold/daily_summary/data.parquet');"
duckdb -c "
SELECT g.transaction_date, g.total_signed_amount AS gold_amount,
       sum(s._signed_amount) AS silver_amount,
       g.total_signed_amount - sum(s._signed_amount) AS diff
FROM read_parquet('data/gold/daily_summary/data.parquet') g
JOIN read_parquet('data/silver/transactions/**/*.parquet') s
  ON g.transaction_date = s.transaction_date AND s._is_resolvable = true
GROUP BY g.transaction_date, g.total_signed_amount
ORDER BY g.transaction_date;
"
```

**Invariant enforcement (code review):** INV-19 — grep gold_daily_summary.sql for "bronze" — must not appear; INV-34 — WHERE _is_resolvable = true present in source CTE.

---

### Task 7.2 — Gold Weekly Account Summary Model

**Inputs:** Silver transactions (all partitions), Silver accounts, Gold weekly_account_summary schema (brief Section 4.3.2).
**Outputs:** `gold_weekly_account_summary.sql` — one row per (account_id, ISO week). Only accounts with at least one resolvable transaction in the week included. closing_balance from Silver Accounts.

**CC Prompt:**
```
Implement dbt_project/models/gold/gold_weekly_account_summary.sql.

Sources (Silver only — no Bronze):
  Silver transactions: read_parquet('/app/data/silver/transactions/**/*.parquet')
  Silver accounts: read_parquet('/app/data/silver/accounts/data.parquet')

Filter: WHERE _is_resolvable = true on Silver transactions.

ISO week derivation:
  week_start_date: DATE_TRUNC('week', transaction_date)  (Monday of the ISO week)
  week_end_date: week_start_date + INTERVAL 6 DAYS       (Sunday)

GROUP BY account_id, week_start_date, week_end_date.

Aggregations:
  total_purchases: count(*) FILTER (WHERE transaction_type = 'PURCHASE')
  avg_purchase_amount: avg(_signed_amount) FILTER (WHERE transaction_type = 'PURCHASE')
  total_payments: sum(_signed_amount) FILTER (WHERE transaction_type = 'PAYMENT')
  total_fees: sum(_signed_amount) FILTER (WHERE transaction_type = 'FEE')
  total_interest: sum(_signed_amount) FILTER (WHERE transaction_type = 'INTEREST')

closing_balance:
  JOIN Silver Accounts on account_id.
  Use current_balance from Silver Accounts (latest available per design).
  If account_id not in Silver Accounts: this case is prevented by _is_resolvable = true filter
  (INV-14 ensures _is_resolvable = true only if account_id in Silver Accounts). Use INNER JOIN.

Audit columns:
  _computed_at: current_timestamp
  _pipeline_run_id: {{ var('run_id') }}

INVARIANT — INV-19: Source is Silver only. No Bronze path.
INVARIANT — INV-24: Full overwrite — materialized: table.
INVARIANT — INV-25: Every account_id in Gold weekly summary exists in Silver Accounts. The INNER JOIN on Silver Accounts enforces this — no unresolvable account can appear in output.
INVARIANT — INV-34: WHERE _is_resolvable = true in source query.

Commit:
  git add dbt_project/models/gold/gold_weekly_account_summary.sql
  git commit -m "7.2 — Gold Weekly Account Summary: ISO week grouping, closing balance, Silver-only source"
```

**Test cases:**
- One row per (account_id, week). No duplicates.
- total_purchases count matches COUNT(*) from Silver for PURCHASE type, same account, same week, _is_resolvable = true.
- closing_balance populated from Silver Accounts for every row.
- Only accounts with at least one resolvable transaction in the week are included.

**Verification command:**
```bash
docker compose run --rm pipeline dbt run --project-dir /app/dbt_project --profiles-dir /app/dbt_project --select gold_weekly_account_summary --vars '{"run_id": "s7-2"}'
duckdb -c "SELECT count(*) FROM read_parquet('data/gold/weekly_account_summary/data.parquet');"
duckdb -c "SELECT count(*) - count(DISTINCT account_id || cast(week_start_date AS VARCHAR)) AS duplicates FROM read_parquet('data/gold/weekly_account_summary/data.parquet');"
duckdb -c "SELECT count(*) FILTER (WHERE closing_balance IS NULL) AS null_balance FROM read_parquet('data/gold/weekly_account_summary/data.parquet');"
```

**Invariant enforcement (code review):** INV-25 — INNER JOIN on Silver Accounts, not LEFT JOIN; INV-34 — _is_resolvable filter present in source.

---

### Task 7.3 — Gold Run Log Wiring and pipeline.py Integration

**Inputs:** pipeline.py (Task 5.5), Gold dbt models (Tasks 7.1–7.2).
**Outputs:** pipeline.py calls Gold models after Silver completes. Run log entries for gold_daily_summary and gold_weekly_account_summary with layer = "GOLD".

**CC Prompt:**
```
Wire Gold dbt models into pipeline.py run_historical.

After all Silver model calls, add:
  dbt run --select gold_daily_summary gold_weekly_account_summary --vars '{"run_id": "<run_id>"}'

Run log entries for each Gold model:
  model_name: gold_daily_summary / gold_weekly_account_summary
  layer: GOLD
  records_processed: Silver transactions row count (total across all partitions, _is_resolvable=true)
  records_written: Gold output row count
  records_rejected: None (Gold has no quarantine)
  status: SUCCESS or FAILED

INVARIANT — INV-31: Each Gold model execution produces exactly one run log row.
INVARIANT — INV-32: status = SUCCESS only if dbt exits 0.

Commit:
  git add pipeline.py
  git commit -m "7.3 — Gold Run Log Wiring: Gold model calls and run log entries in pipeline.py"
```

**Test cases:**
- Run log has two Gold entries with layer = "GOLD" and status = "SUCCESS".
- records_written matches Gold output row count.

**Verification command:**
```bash
rm -f data/pipeline/run_log.parquet
docker compose run --rm pipeline python pipeline.py historical --start-date 2024-01-15 --end-date 2024-01-21
duckdb -c "SELECT model_name, layer, status, records_written FROM read_parquet('data/pipeline/run_log.parquet') WHERE layer='GOLD';"
```

**Invariant enforcement:** INV-31, INV-32.

---

### Task 7.4 — Gold Determinism and Overwrite Verification

**Inputs:** Gold models as implemented, Silver data from prior sessions.
**Outputs:** Determinism test: two runs of Gold on identical Silver state produce byte-equivalent output. Overwrite confirmed: _computed_at timestamp from second run is newer than first, no stale records.

**CC Prompt:**
```
Implement a Gold determinism and overwrite verification script: scripts/verify_gold_determinism.py

Steps:
1. Run gold_daily_summary dbt model twice with different run_ids.
2. Compare output row counts — must be identical.
3. Compare all non-timestamp columns (exclude _computed_at, _pipeline_run_id) — must be identical.
4. Confirm _computed_at in second run is >= _computed_at in first run (overwrite occurred).
5. Print PASS or FAIL with detail.

Use DuckDB to read both outputs and compare.

This script is a verification artifact only — not called from pipeline.py.

INVARIANT — INV-20: Gold outputs are deterministic given the same Silver input. Two runs must produce identical aggregate values.
INVARIANT — INV-24: Gold is fully overwritten. No record from first run survives into second run unchanged. _computed_at timestamps must update.

Commit:
  git add scripts/verify_gold_determinism.py
  git commit -m "7.4 — Gold Determinism Script: INV-20 and INV-24 verification artifact"
```

**Test cases:**
- Two runs on identical Silver: all aggregate columns identical.
- _computed_at updates on second run (overwrite occurred).

**Verification command:**
```bash
docker compose run --rm pipeline python scripts/verify_gold_determinism.py
```

**Invariant enforcement:** INV-20 and INV-24 — this task IS the verification for these two invariants.

---

**Session 7 Integration Check:** (see top of session)

---

## Session 8 — Pipeline Orchestration: Historical Pipeline

**Session goal:** `run_historical` processes all dates in a range sequentially, handles per-date idempotency via run log, loads transaction codes once, initialises watermark to end_date on success. All INV-28, INV-29, INV-36 enforced. After this session: `python pipeline.py historical --start-date 2024-01-15 --end-date 2024-01-21` processes all 7 days.

**Branch slug:** `session/8-historical-pipeline`

**Integration check:**
```bash
rm -rf data/
docker compose run --rm pipeline python pipeline.py historical --start-date 2024-01-15 --end-date 2024-01-21
duckdb -c "SELECT last_processed_date FROM read_parquet('data/pipeline/control.parquet');"
duckdb -c "SELECT count(DISTINCT date) FROM read_parquet('data/bronze/transactions/**/*.parquet');"
duckdb -c "SELECT count(DISTINCT transaction_date) FROM read_parquet('data/silver/transactions/**/*.parquet');"
duckdb -c "SELECT count(*) FROM read_parquet('data/gold/daily_summary/data.parquet');"
```
last_processed_date must be 2024-01-21. Bronze distinct dates = 7. Silver distinct dates ≥ 1. Gold row count ≥ 1.

---

### Task 8.1 — Transaction Codes Idempotency Check in Historical Pipeline

**Inputs:** pipeline.py (Task 7.3), bronze and Silver transaction codes loaders (Tasks 3.1, 4.1), run log (Task 3.3).
**Outputs:** Historical pipeline checks run log for bronze_transaction_codes SUCCESS before re-loading. Also validates Silver transaction_codes row count > 0. Reloads only if either check fails.

**CC Prompt:**
```
Implement transaction codes idempotency check in pipeline.py run_historical.

Before calling load_bronze_transaction_codes and silver_transaction_codes dbt model:
1. Check run log for an existing entry where model_name='bronze_transaction_codes' AND status='SUCCESS'.
2. Check Silver transaction_codes row count > 0 (read file if it exists).
3. If BOTH checks pass: log "SKIP transaction_codes — already loaded and Silver valid" and skip both loads.
4. If EITHER check fails: load Bronze transaction_codes, then run Silver transaction_codes dbt model.

This implements Architecture Decision 3 (special case for transaction codes): run log SUCCESS alone is not
sufficient — Silver file must also be valid. This prevents a false-skip when Silver was somehow lost
after a prior run logged SUCCESS.

INVARIANT — INV-33: Silver transaction_codes row count must be > 0 before any Silver transactions promotion.
The check here (step 2) is the second layer of enforcement — the first is the pre-condition check in pipeline.py (Task 4.4).

Commit:
  git add pipeline.py
  git commit -m "8.1 — Transaction Codes Idempotency: run log + Silver state dual check"
```

**Test cases:**
- First run: run log empty → loads Bronze + Silver transaction codes.
- Re-run: run log has SUCCESS + Silver has rows → skip both loads.
- Re-run with Silver deleted: run log has SUCCESS but Silver empty → reload (not skip).

**Verification command:**
```bash
# First run
docker compose run --rm pipeline python pipeline.py historical --start-date 2024-01-15 --end-date 2024-01-21
duckdb -c "SELECT status FROM read_parquet('data/pipeline/run_log.parquet') WHERE model_name='bronze_transaction_codes';"
# Second run — observe SKIP log
docker compose run --rm pipeline python pipeline.py historical --start-date 2024-01-15 --end-date 2024-01-21 2>&1 | grep "SKIP transaction_codes"
```

**Invariant enforcement (code review):** INV-33 — confirm BOTH conditions are required to skip (AND logic, not OR).

---

### Task 8.2 — Date Range Iteration and Per-Date Idempotency

**Inputs:** pipeline.py (Task 8.1), run log (Task 3.3).
**Outputs:** `run_historical` iterates from start_date to end_date inclusive. For each date, checks run log for existing SUCCESS entries for that date's Bronze and Silver models before processing. Skips completed dates.

**CC Prompt:**
```
Implement full date range iteration in pipeline.py run_historical.

Date iteration:
  from datetime import date, timedelta
  current_date = date.fromisoformat(start_date)
  end = date.fromisoformat(end_date)
  while current_date <= end:
      process_date(current_date, run_id)
      current_date += timedelta(days=1)

Per-date idempotency (process_date function):
  For each date:
  1. Check run log: if entries exist for bronze_transactions, bronze_accounts, silver_transactions,
     silver_accounts, silver_accounts_quarantine, silver_quarantine, gold_daily_summary, gold_weekly_account_summary
     all with status = SUCCESS for this date (model_name + started_at date match):
     log "SKIP date={date} — all models SUCCESS" and return.
  2. Otherwise: run the full Bronze → Silver → Gold sequence for this date.

Note: Gold models recompute all dates on every run (full overwrite per Decision 5). The per-date skip
for Gold should only skip if Gold has already been computed and Silver is unchanged. For simplicity in
this implementation: Gold is always recomputed (not date-skipped) — skip applies to Bronze and Silver only.

INVARIANT — INV-35 (GLOBAL): run_id is generated once at the start of run_historical, shared across all
dates in the range. Confirm the while loop does not regenerate run_id per date.

Commit:
  git add pipeline.py
  git commit -m "8.2 — Date Range Iteration: sequential processing with per-date run log skip check"
```

**Test cases:**
- Full run from 2024-01-15 to 2024-01-21: 7 Bronze transaction partitions created.
- Re-run: all 7 dates skipped (run log shows SUCCESS for all models on all dates).
- Partial prior run (dates 1-3 complete, 4-7 not): re-run processes only dates 4-7.

**Verification command:**
```bash
rm -rf data/
docker compose run --rm pipeline python pipeline.py historical --start-date 2024-01-15 --end-date 2024-01-21
duckdb -c "SELECT count(DISTINCT date) FROM read_parquet('data/bronze/transactions/**/*.parquet');"
# Re-run — all should be skipped
docker compose run --rm pipeline python pipeline.py historical --start-date 2024-01-15 --end-date 2024-01-21 2>&1 | grep -c "SKIP date"
```

**Invariant enforcement (code review):** INV-35 — run_id assigned before while loop, used throughout; not reassigned per iteration.

---

### Task 8.3 — Watermark Initialisation After Historical Run

**Inputs:** pipeline.py (Task 8.2), `pipeline_control.py` (Task 3.2), Architecture OQ-03 resolution.
**Outputs:** After all dates in the range complete successfully, `write_watermark(end_date, run_id)` is called. If watermark already >= end_date (re-run of completed historical range): guard from OQ-03 — log and exit without writing. Watermark write is the last step.

**CC Prompt:**
```
Implement watermark initialisation at the end of pipeline.py run_historical.

After the while loop completes (all dates processed):
1. Read current watermark.
2. If current watermark >= end_date: log "SKIP watermark — already at or past end_date={end_date}" and return.
   (This handles re-run of a previously completed historical range — OQ-03 resolution.)
3. If current watermark is None or < end_date: call write_watermark(end_date, run_id).

The watermark write is the absolute last operation in run_historical. It must not execute if any date
in the loop failed (exception re-raised before reaching this point).

INVARIANT — INV-36: After a successful historical pipeline run, last_processed_date = end_date.
Confirm write_watermark(end_date) is called as the final step and only if the loop completed without exception.

INVARIANT — INV-29: write_watermark already enforces monotonicity (Task 3.2). The OQ-03 guard here
(step 2) is a pre-check that prevents a ValueError from write_watermark on re-run — it logs and skips
rather than raising.

INVARIANT — INV-28: Watermark advances only after all layers complete for all dates. Confirm write_watermark
is positioned after the while loop closes, not inside the loop.

Commit:
  git add pipeline.py
  git commit -m "8.3 — Watermark Initialisation: end_date write after full historical loop"
```

**Test cases:**
- Full run completes: watermark = end_date = 2024-01-21.
- Re-run of same range: watermark unchanged (skip guard activates).
- Run fails mid-range (exception on date 3): watermark not written (exception propagates before watermark step).

**Verification command:**
```bash
rm -rf data/
docker compose run --rm pipeline python pipeline.py historical --start-date 2024-01-15 --end-date 2024-01-21
duckdb -c "SELECT last_processed_date FROM read_parquet('data/pipeline/control.parquet');"
# Re-run
docker compose run --rm pipeline python pipeline.py historical --start-date 2024-01-15 --end-date 2024-01-21
duckdb -c "SELECT last_processed_date FROM read_parquet('data/pipeline/control.parquet');"
```

**Invariant enforcement (code review):** INV-36 — watermark positioned after while loop; INV-28 — no watermark write inside the per-date loop body.

---

### Task 8.4 — Historical Pipeline: Partial Failure Recovery Test

**Inputs:** pipeline.py (Task 8.3), existing test data.
**Outputs:** Verification that a partial failure leaves the pipeline in a recoverable state — re-run completes the remaining dates without re-processing completed dates or corrupting Bronze.

**CC Prompt:**
```
Create a partial failure recovery test script: scripts/test_partial_recovery.py

Steps:
1. Clean data/ directory.
2. Run historical pipeline for dates 2024-01-15 to 2024-01-17 only.
3. Verify: 3 Bronze transaction partitions, watermark = 2024-01-17.
4. Run historical pipeline for dates 2024-01-15 to 2024-01-21.
5. Verify: 7 Bronze transaction partitions (dates 15-17 skipped, 18-21 processed).
6. Verify: watermark = 2024-01-21.
7. Verify: Bronze partitions for 15-17 are unchanged (row counts identical to step 3).
8. Print PASS or FAIL per check.

This script is a verification artifact — not called from pipeline.py.

Commit:
  git add scripts/test_partial_recovery.py
  git commit -m "8.4 — Partial Recovery Test Script: historical pipeline resume verification"
```

**Test cases:**
- All PASS checks in the script output.

**Verification command:**
```bash
docker compose run --rm pipeline python scripts/test_partial_recovery.py
```

**Invariant enforcement:** INV-01 (idempotency), INV-04 (no overwrite), INV-29 (monotonicity) — this test is the combined runtime verification of all three for the historical pipeline.

---

### Task 8.5 — Historical Pipeline End-to-End Integration Smoke Test

**Inputs:** All sessions up to this point.
**Outputs:** Full clean-state run produces correct Bronze, Silver, Gold, control table, and run log. All PBVI session integration check commands pass.

**CC Prompt:**
```
Create a full smoke test script: scripts/smoke_test_historical.py

Steps:
1. Assert data/ directory is empty (or clean it).
2. Run: python pipeline.py historical --start-date 2024-01-15 --end-date 2024-01-21
3. Assert:
   a. Bronze: 7 transaction partitions exist and each has row count > 0.
   b. Bronze: 7 accounts partitions exist (or fewer if some dates had no delta — not an error).
   c. Bronze transaction_codes: row count > 0.
   d. Silver transaction_codes: row count > 0.
   e. Silver accounts: row count > 0, count(*) = count(DISTINCT account_id).
   f. Silver transactions: count(DISTINCT transaction_id) = count(*) across all partitions.
   g. Quarantine: count(*) >= 0 (may be zero if seed data has no intentional quality issues).
   h. Gold daily_summary: row count >= 1.
   i. Gold weekly_account_summary: row count >= 1.
   j. Watermark: last_processed_date = 2024-01-21.
   k. Run log: row count >= 8 (one per model per run at minimum).
   l. All run log entries have status = SUCCESS (no FAILED entries).
4. Print PASS / FAIL per assertion.

Commit:
  git add scripts/smoke_test_historical.py
  git commit -m "8.5 — Historical Smoke Test: end-to-end clean-state verification"
```

**Verification command:**
```bash
docker compose run --rm pipeline python scripts/smoke_test_historical.py
```

**Invariant enforcement:** This test exercises INV-01, INV-04, INV-10, INV-17, INV-28, INV-29, INV-31, INV-36 in combination.

---

**Session 8 Integration Check:** (see top of session)

---

## Session 9 — Pipeline Orchestration: Incremental Pipeline and No-Op

**Session goal:** `run_incremental` reads watermark, processes watermark+1, advances watermark on success. No-op when no file available. INV-27, INV-28, INV-29 fully enforced for incremental path. After this session: `python pipeline.py incremental` correctly processes the next available date or exits without side effects.

**Branch slug:** `session/9-incremental-pipeline`

**Integration check:**
```bash
# Prereq: historical run complete, watermark = 2024-01-21
# No new files exist beyond 2024-01-21 in seed data — no-op expected
docker compose run --rm pipeline python pipeline.py incremental 2>&1 | grep "no-op\|NOOP\|no new"
# Verify no layer changes
duckdb -c "SELECT last_processed_date FROM read_parquet('data/pipeline/control.parquet');"
```

---

### Task 9.1 — Incremental Pipeline: Watermark Read and Next Date Derivation

**Inputs:** pipeline.py (Task 8.5), `pipeline_control.py` (Task 3.2).
**Outputs:** `run_incremental()` reads watermark, derives next_date = watermark + 1. Raises if no watermark exists (historical pipeline has not been run). Logs next_date before any processing.

**CC Prompt:**
```
Implement watermark read and next date derivation in pipeline.py run_incremental.

Steps:
1. read_watermark() from pipeline_control.
2. If watermark is None: raise RuntimeError("Incremental pipeline cannot run — no watermark. Run historical pipeline first.")
3. next_date = watermark + timedelta(days=1)
4. Log: f"[INCREMENTAL] run_id={run_id} next_date={next_date}"
5. Rest of function still NOT IMPLEMENTED (stub) — returns after logging.

INVARIANT — INV-28: Watermark advance happens only after all layers complete. The watermark read here is a read-only step. The write comes at the end (Task 9.3). Confirm no write_watermark call in this task.

Commit:
  git add pipeline.py
  git commit -m "9.1 — Incremental Watermark Read: next_date derivation and error guard"
```

**Test cases:**
- After historical run: next_date = 2024-01-22 (watermark + 1).
- No watermark: RuntimeError raised.

**Verification command:**
```bash
docker compose run --rm pipeline python pipeline.py incremental 2>&1 | grep "next_date"
# No watermark case
cp data/pipeline/control.parquet /tmp/control_backup.parquet
rm data/pipeline/control.parquet
docker compose run --rm pipeline python pipeline.py incremental 2>&1 | grep "RuntimeError\|cannot run"
cp /tmp/control_backup.parquet data/pipeline/control.parquet
```

**Invariant enforcement:** INV-28 — no write_watermark in this task.

---

### Task 9.2 — Incremental Pipeline: No-Op Detection

**Inputs:** pipeline.py (Task 9.1), source directory structure.
**Outputs:** If no transactions file exists for next_date: pipeline logs "NOOP" and exits. No layer writes, no run log entries, no watermark change. INV-27 enforced.

**CC Prompt:**
```
Implement no-op detection in pipeline.py run_incremental.

After deriving next_date:
  source_file = SOURCE_DIR / f"transactions_{next_date.isoformat()}.csv"
  If not source_file.exists():
      log f"[INCREMENTAL] NOOP — no transactions file for {next_date}. No layers written. No watermark advance."
      return  # exit cleanly, no side effects

If file exists: continue to Bronze → Silver → Gold processing (Task 9.3).

INVARIANT — INV-27: A no-op run produces no changes to any layer. All file modification timestamps, row counts, and content are identical before and after. No new run log entry is produced. No watermark change.
Confirm: the return after the no-op log is a clean exit — no Bronze, Silver, Gold, or run log calls are made after this point in the no-op branch.

Commit:
  git add pipeline.py
  git commit -m "9.2 — Incremental No-Op: absent file detection, clean exit, no side effects"
```

**Test cases:**
- Next date has no source file → NOOP log, all layers unchanged, watermark unchanged.
- Next date has source file → processing continues (covered in Task 9.3).

**Verification command:**
```bash
# Seed data ends at 2024-01-21; next date is 2024-01-22 which has no file
BEFORE_WATERMARK=$(duckdb -csv -noheader -c "SELECT last_processed_date FROM read_parquet('data/pipeline/control.parquet');")
BEFORE_RUNLOG=$(duckdb -csv -noheader -c "SELECT count(*) FROM read_parquet('data/pipeline/run_log.parquet');")
docker compose run --rm pipeline python pipeline.py incremental
AFTER_WATERMARK=$(duckdb -csv -noheader -c "SELECT last_processed_date FROM read_parquet('data/pipeline/control.parquet');")
AFTER_RUNLOG=$(duckdb -csv -noheader -c "SELECT count(*) FROM read_parquet('data/pipeline/run_log.parquet');")
echo "Watermark before: $BEFORE_WATERMARK, after: $AFTER_WATERMARK — must match"
echo "Run log before: $BEFORE_RUNLOG, after: $AFTER_RUNLOG — must match"
```

**Invariant enforcement (code review):** INV-27 — confirm the no-op branch contains only a log statement and return — no function calls to Bronze, Silver, Gold, or run log utilities.

---

### Task 9.3 — Incremental Pipeline: Processing and Watermark Advance

**Inputs:** pipeline.py (Task 9.2), all Bronze/Silver/Gold models from Sessions 2–7.
**Outputs:** When source file present: incremental pipeline runs Bronze → Silver → Gold for next_date. Watermark advanced to next_date after all layers complete. Run log populated. Error on any layer failure prevents watermark advance.

**CC Prompt:**
```
Implement the incremental processing path and watermark advance in pipeline.py run_incremental.

When source file exists (past the no-op check):
1. Call load_bronze_transactions(date=next_date_str, run_id=run_id).
2. Call load_bronze_accounts(date=next_date_str, run_id=run_id) — no-op if accounts file absent (by design).
3. Check INV-33: Silver transaction_codes row count > 0 — raise if not.
4. Run dbt silver_accounts, silver_accounts_quarantine, silver_transactions, silver_quarantine for next_date.
5. Run dbt gold_daily_summary, gold_weekly_account_summary.
6. Log run log entries for each model (same pattern as historical pipeline).
7. All steps above are wrapped in try/except — if any step fails: log FAILED run log entry, re-raise exception. Do NOT write watermark.
8. If all steps succeed: write_watermark(next_date, run_id).

INVARIANT — INV-28: Watermark advances only after all layers complete. Confirm write_watermark(next_date) is called only after all Bronze, Silver, and Gold dbt calls have completed without exception. It must be inside a block that is only reached if no exception was raised.

INVARIANT — INV-29: write_watermark enforces monotonicity (Task 3.2). No additional check needed here.

INVARIANT — INV-35 (GLOBAL): run_id passed to all Bronze loaders, all dbt --vars, all run log entries, and write_watermark is the same UUID generated at the start of run_incremental.

Commit:
  git add pipeline.py
  git commit -m "9.3 — Incremental Processing: Bronze/Silver/Gold sequence, watermark advance on success"
```

**Test cases:**
- File present (simulated by copying a source CSV to an extra date): all layers run, watermark advances.
- Mid-pipeline failure (inject error): watermark not written, exception surfaces.
- All run log entries share same run_id.

**Verification command:**
```bash
# Simulate a new date by copying a source file
cp source/transactions_2024-01-21.csv source/transactions_2024-01-22.csv
cp source/accounts_2024-01-21.csv source/accounts_2024-01-22.csv
docker compose run --rm pipeline python pipeline.py incremental
duckdb -c "SELECT last_processed_date FROM read_parquet('data/pipeline/control.parquet');"
duckdb -c "SELECT count(*) FROM read_parquet('data/bronze/transactions/date=2024-01-22/data.parquet');"
duckdb -c "SELECT model_name, status FROM read_parquet('data/pipeline/run_log.parquet') ORDER BY started_at DESC LIMIT 8;"
# Cleanup
rm source/transactions_2024-01-22.csv source/accounts_2024-01-22.csv
```

**Invariant enforcement (code review):** INV-28 — write_watermark called only in the success branch after all dbt calls; INV-35 — run_id not regenerated inside the processing block.

---

### Task 9.4 — Incremental Pipeline Idempotency and Second No-Op Test

**Inputs:** pipeline.py (Task 9.3), processed state from Task 9.3 (if simulated file test run), or clean state.
**Outputs:** Verification script confirming incremental pipeline is idempotent on re-run of the same date (Bronze partition exists → skip) and no-op on second attempt with no new file.

**CC Prompt:**
```
Create verification script: scripts/test_incremental_idempotency.py

Steps:
1. Confirm historical pipeline has been run (watermark exists). If not: error and exit.
2. Simulate a new date: copy source/transactions_2024-01-21.csv to source/transactions_2024-01-22.csv.
3. Run incremental pipeline. Assert: watermark = 2024-01-22, Bronze partition for 2024-01-22 exists.
4. Run incremental pipeline again. Assert: NOOP (no file for 2024-01-23).
   Watermark unchanged. Run log row count unchanged.
5. Remove source/transactions_2024-01-22.csv.
6. Print PASS / FAIL per assertion.

INVARIANT — INV-27: Second run produces no layer changes. Assert file modification timestamps unchanged.

Commit:
  git add scripts/test_incremental_idempotency.py
  git commit -m "9.4 — Incremental Idempotency Test: re-run and no-op verification"
```

**Verification command:**
```bash
docker compose run --rm pipeline python scripts/test_incremental_idempotency.py
```

**Invariant enforcement:** INV-27 — no-op branch leaves all layers unchanged; INV-01 — Bronze partition not duplicated on re-run.

---

**Session 9 Integration Check:** (see top of session)

---

## Session 10 — System Sign-Off Verification

**Session goal:** Phase 8 sign-off evidence collected. All invariants verified end-to-end. Gold correctness assertions from brief Section 10.3 pass. Full idempotency sweep. Audit trail verified. VERIFICATION_CHECKLIST.md completed.

**Branch slug:** `session/10-system-signoff`

**Note:** This session produces NO new code. All tasks are verification-only. Any failure returns to the relevant session, not forward.

**Integration check:**
```bash
# Full clean run
rm -rf data/
docker compose run --rm pipeline python pipeline.py historical --start-date 2024-01-15 --end-date 2024-01-21
# All verification commands below must produce expected results
```

---

### Task 10.1 — Bronze and Silver Completeness Verification (Brief Section 10.1 and 10.2)

**Inputs:** Fully populated data/ directory from a clean historical run.
**Outputs:** All brief Section 10.1 and 10.2 checks pass and are recorded in VERIFICATION_CHECKLIST.md.

**CC Prompt:**
```
Run all Phase 8 Section 10.1 and 10.2 verification checks from the requirements brief.

Execute each command below, record the result, and confirm PASS or FAIL.

10.1 Bronze Completeness:
  a. duckdb -c "SELECT count(*) FROM read_parquet('data/bronze/transactions/**/*.parquet');" 
     vs. python -c "import csv, glob, pathlib; print(sum(sum(1 for _ in open(f)) - 1 for f in glob.glob('source/transactions_*.csv')))"
     Must be equal.

  b. duckdb -c "SELECT count(*) FROM read_parquet('data/bronze/accounts/**/*.parquet');"
     vs. python -c "import glob; print(sum(sum(1 for _ in open(f)) - 1 for f in glob.glob('source/accounts_*.csv')))"
     Must be equal.

  c. duckdb -c "SELECT count(*) FROM read_parquet('data/bronze/transaction_codes/data.parquet');"
     vs. python -c "print(sum(1 for _ in open('source/transaction_codes.csv')) - 1)"
     Must be equal.

10.2 Silver Quality:
  a. duckdb -c "SELECT count(*) FROM read_parquet('data/silver/transactions/**/*.parquet') + count(*) FROM read_parquet('data/silver/quarantine/**/*.parquet') WHERE _source_file LIKE '%transactions%'"
     vs. Bronze transactions row count. Must be equal (INV-07).

  b. duckdb -c "SELECT count(*) - count(DISTINCT transaction_id) AS duplicates FROM read_parquet('data/silver/transactions/**/*.parquet');"
     Must be 0 (INV-10).

  c. duckdb -c "SELECT count(*) FROM read_parquet('data/silver/transactions/**/*.parquet') t LEFT JOIN read_parquet('data/silver/transaction_codes/data.parquet') tc ON t.transaction_code = tc.transaction_code WHERE tc.transaction_code IS NULL;"
     Must be 0 (INV-11).

  d. duckdb -c "SELECT count(*) FILTER (WHERE _signed_amount IS NULL) FROM read_parquet('data/silver/transactions/**/*.parquet');"
     Must be 0 (INV-12).

  e. duckdb -c "SELECT DISTINCT _rejection_reason FROM read_parquet('data/silver/quarantine/**/*.parquet');"
     Must contain only values from the valid set (INV-09).

Record each result in VERIFICATION_CHECKLIST.md.

Commit:
  git add verification/VERIFICATION_CHECKLIST.md
  git commit -m "10.1 — Bronze/Silver Verification: INV-07, INV-09, INV-10, INV-11, INV-12 recorded"
```

**Verification commands:** Embedded above.

**Invariant enforcement:** INV-07, INV-09, INV-10, INV-11, INV-12. This task IS the Phase 8 enforcement for these invariants.

---

### Task 10.2 — Gold Correctness Verification (Brief Section 10.3)

**Inputs:** Gold output from clean historical run, Silver data.
**Outputs:** All brief Section 10.3 Gold correctness checks pass and recorded in VERIFICATION_CHECKLIST.md.

**CC Prompt:**
```
Run all Phase 8 Section 10.3 Gold correctness checks from the requirements brief.

10.3a — Gold daily_summary granularity (INV-21):
  duckdb -c "
    SELECT count(*) FROM read_parquet('data/gold/daily_summary/data.parquet')
    EXCEPT
    SELECT count(DISTINCT transaction_date) FROM read_parquet('data/silver/transactions/**/*.parquet') WHERE _is_resolvable=true;
  "
  Must return 0 (same count in both).

10.3b — Gold total_signed_amount per day matches Silver (INV-22):
  duckdb -c "
    SELECT g.transaction_date,
           g.total_signed_amount AS gold_amount,
           sum(s._signed_amount) AS silver_amount,
           abs(g.total_signed_amount - sum(s._signed_amount)) AS diff
    FROM read_parquet('data/gold/daily_summary/data.parquet') g
    JOIN read_parquet('data/silver/transactions/**/*.parquet') s
      ON g.transaction_date = s.transaction_date AND s._is_resolvable = true
    GROUP BY g.transaction_date, g.total_signed_amount
    HAVING abs(g.total_signed_amount - sum(s._signed_amount)) > 0.001;
  "
  Must return 0 rows (no discrepancy).

10.3c — Gold weekly purchases count matches Silver (INV-22):
  duckdb -c "
    SELECT w.account_id, w.week_start_date, w.total_purchases AS gold_purchases,
           count(s.transaction_id) AS silver_purchases
    FROM read_parquet('data/gold/weekly_account_summary/data.parquet') w
    JOIN read_parquet('data/silver/transactions/**/*.parquet') s
      ON w.account_id = s.account_id
      AND s._is_resolvable = true
      AND DATE_TRUNC('week', s.transaction_date) = w.week_start_date
      AND s.transaction_type = 'PURCHASE'
    GROUP BY w.account_id, w.week_start_date, w.total_purchases
    HAVING w.total_purchases != count(s.transaction_id);
  "
  Must return 0 rows.

10.3d — Gold excludes unresolvable transactions (INV-34):
  duckdb -c "
    SELECT count(*) FROM read_parquet('data/gold/daily_summary/data.parquet') g
    WHERE g.total_transactions > (
      SELECT count(*) FROM read_parquet('data/silver/transactions/**/*.parquet') s
      WHERE s.transaction_date = g.transaction_date AND s._is_resolvable = true);
  "
  Must be 0.

Record each result in VERIFICATION_CHECKLIST.md.

Commit:
  git add verification/VERIFICATION_CHECKLIST.md
  git commit -m "10.2 — Gold Correctness Verification: INV-21, INV-22, INV-34 recorded"
```

**Invariant enforcement:** INV-21, INV-22, INV-34. Phase 8 enforcement for these invariants.

---

### Task 10.3 — Idempotency and Audit Trail Verification (Brief Section 10.4 and 10.5)

**Inputs:** First run data/, a second clean run, run log.
**Outputs:** Idempotency verified across all layers. Audit trail verified — every Bronze and Silver record has non-null _pipeline_run_id, every run_id in Silver has a corresponding run log SUCCESS entry.

**CC Prompt:**
```
Run Phase 8 Section 10.4 (idempotency) and 10.5 (audit trail) checks.

10.4 Idempotency:
  a. Record Bronze transaction count for each partition after first run.
  b. Run pipeline.py historical --start-date 2024-01-15 --end-date 2024-01-21 again.
  c. Confirm Bronze counts identical:
     duckdb -c "SELECT date, count(*) n FROM read_parquet('data/bronze/transactions/**/*.parquet') GROUP BY date ORDER BY date;"
     Must match first run counts.
  d. Confirm Silver transaction count identical (cross-partition dedup prevents re-promotion).
  e. Confirm Gold row counts identical.
  f. Run incremental pipeline with no new file — confirm NOOP:
     Record Gold row count before. Run incremental. Confirm Gold row count unchanged.

10.5 Audit Trail:
  a. duckdb -c "SELECT count(*) FILTER (WHERE _pipeline_run_id IS NULL) FROM read_parquet('data/bronze/transactions/**/*.parquet');"
     Must be 0 (INV-03).

  b. duckdb -c "SELECT count(*) FILTER (WHERE _pipeline_run_id IS NULL) FROM read_parquet('data/silver/transactions/**/*.parquet');"
     Must be 0 (INV-03 applies to Silver audit column continuity).

  c. duckdb -c "
     SELECT count(*) FROM (
       SELECT DISTINCT _pipeline_run_id FROM read_parquet('data/silver/transactions/**/*.parquet')
     ) s
     LEFT JOIN read_parquet('data/pipeline/run_log.parquet') rl ON s._pipeline_run_id = rl.run_id AND rl.status = 'SUCCESS'
     WHERE rl.run_id IS NULL;
     "
     Must be 0 (every Silver run_id has a SUCCESS run log row).

Record all results in VERIFICATION_CHECKLIST.md.

Commit:
  git add verification/VERIFICATION_CHECKLIST.md
  git commit -m "10.3 — Idempotency and Audit Trail: INV-26, INV-03, INV-35 recorded"
```

**Invariant enforcement:** INV-03, INV-26, INV-35, INV-27.

---

### Task 10.4 — System Sign-Off Sign-Off Record

**Inputs:** VERIFICATION_CHECKLIST.md (Tasks 10.1–10.3).
**Outputs:** VERIFICATION_CHECKLIST.md completed with engineer sign-off. PROJECT_MANIFEST.md status columns updated to PRESENT for all completed documents.

**CC Prompt:**
```
Complete VERIFICATION_CHECKLIST.md with the final summary section.

Add to VERIFICATION_CHECKLIST.md:
- Total invariants verified: 37
- Total PASS: [engineer to fill from prior tasks]
- Total FAIL: [engineer to fill]
- Sign-off statement: [engineer to sign — do not pre-populate]
- Date: [engineer to fill]

Update PROJECT_MANIFEST.md status fields:
  docs/ARCHITECTURE.md → PRESENT
  docs/INVARIANTS.md → PRESENT
  docs/EXECUTION_PLAN.md → PRESENT
  verification/VERIFICATION_CHECKLIST.md → PRESENT

HUMAN GATE: Engineer signs VERIFICATION_CHECKLIST.md before committing this task.
The sign-off field must contain the engineer's name and date — it cannot be left blank.
Claude does not fill this field.

Commit:
  git add verification/VERIFICATION_CHECKLIST.md PROJECT_MANIFEST.md
  git commit -m "10.4 — System Sign-Off: VERIFICATION_CHECKLIST.md complete, engineer signed"
```

**Verification command:**
```bash
grep "Sign-off\|Date:" verification/VERIFICATION_CHECKLIST.md
cat PROJECT_MANIFEST.md | grep -E "PRESENT|PENDING"
```

**Invariant enforcement:** This is the Phase 8 Part 1 close-out. No new code — sign-off only.

---

**Session 10 Integration Check:**
```bash
# All checks in tasks 10.1–10.3 must pass. No single command — engineer reviews VERIFICATION_CHECKLIST.md.
cat verification/VERIFICATION_CHECKLIST.md | grep -E "PASS|FAIL"
```

---

## Invariant Coverage Map

| INV | Session(s) | Task(s) | Enforcement Type |
|---|---|---|---|
| INV-01 | S2, S3 | 2.2, 2.3, 3.1 | Code — existence + row count check before write |
| INV-02 | S2, S3 | 2.2, 2.3, 3.1 | Code — explicit dtype schema in read_csv |
| INV-03 | S2 (GLOBAL) | All Bronze writes | Claude.md — audit columns added before write |
| INV-04 | S2, S3 | 2.2, 2.3, 3.1 | Code — no overwrite path exists |
| INV-05 | S2 | 2.2, 2.3 | Code — path derived from date arg, not record content |
| INV-06 | S4, S5 (GLOBAL) | 4.3, 5.4, 6.3 | Claude.md — every Bronze record routes to Silver or quarantine |
| INV-07 | S5, S6 | 5.4, 6.4 | Code — conservation assertion post-hook (hard raise) |
| INV-08 | S4, S5 | 4.3, 5.4 | Code — _source_file carried from Bronze in quarantine write |
| INV-09 | S4, S5 | 4.3, 5.1–5.4 | Code — string literals from valid set; no free-text |
| INV-10 | S6 | 6.1, 6.2 | Code — within-batch ROW_NUMBER + cross-partition anti-join |
| INV-11 | S5 | 5.2 | Code — LEFT JOIN + NULL filter on Silver transaction_codes |
| INV-12 | S5 | 5.3 | Code — null check post-derivation routes to quarantine |
| INV-13 | S5 | 5.3 | Code — CASE on debit_credit_indicator; assertion all 5 conditions |
| INV-14 | S6 | 6.3 | Code — LEFT JOIN on Silver Accounts; NULL = _is_resolvable=false |
| INV-15 | S6 | 6.3 | Code — UNRESOLVABLE_ACCOUNT_ID absent from all quarantine CTEs |
| INV-16 | S6 | 6.2, 6.4 | Code — partition path from transaction_date field; guard CTE |
| INV-17 | S4 | 4.2 | Code — upsert on account_id; post-hook uniqueness assertion |
| INV-18 | S4 | 4.2 | Code — deterministic upsert; no timestamp tie-break |
| INV-19 | S7 | 7.1, 7.2 | Code review — no Bronze path in Gold model SQL |
| INV-20 | S7, S10 | 7.4, 10.2 | Script — determinism verification (verify_gold_determinism.py) |
| INV-21 | S7, S10 | 7.1, 10.2 | Code — GROUP BY transaction_date only; Phase 8 count check |
| INV-22 | S10 | 10.2 | Phase 8 — SUM comparison query |
| INV-23 | S7 | 7.1, 7.2 | Code — INNER JOIN on Silver ensures Silver-backed existence |
| INV-24 | S7 | 7.1, 7.2, 7.4 | Code — dbt materialized: table (full replace) |
| INV-25 | S7 | 7.2 | Code — INNER JOIN on Silver Accounts in weekly model |
| INV-26 | S10 | 10.3 | Phase 8 — second run row count comparison |
| INV-27 | S9, S10 | 9.2, 9.4, 10.3 | Code — no-op return before any writes; verification script |
| INV-28 | S8, S9 | 8.3, 9.3 | Code — write_watermark after all layers, exception blocks |
| INV-29 | S3, S8 | 3.2, 8.3 | Code — monotonicity guard in write_watermark (raises on violation) |
| INV-30 | S3 | 3.3 | Code — read-union-write; no delete/update paths |
| INV-31 | S3, S4, S5, S7, S8, S9 | 3.4, 4.4, 5.5, 7.3, 8.x, 9.3 | Code — try/except wrap per model execution |
| INV-32 | S3, S4, S5, S7, S8, S9 | 3.3, 3.4 | Code — status set from enumerated set; validation in append_run_log_entry |
| INV-33 | S4, S5, S8 | 4.4, 5.1, 8.1 | Code — pre-condition check before silver_transactions; dual check in historical |
| INV-34 | S7, S10 | 7.1, 7.2, 10.2 | Code — WHERE _is_resolvable=true in all Gold source queries |
| INV-35 | S1, S2, S8, S9 (GLOBAL) | 1.4, 2.4, 8.2, 9.3 | Claude.md — UUID generated once per invocation, passed through |
| INV-36 | S8 | 8.3 | Code — write_watermark(end_date) after while loop completes |
| INV-37 | S2, S3 | 2.2, 2.3, 3.1 | Code — explicit SELECT naming all columns; DESCRIBE post-write check |

---

*EXECUTION_PLAN.md — v1.0 · Phase 3 Complete · Credit Card Financial Transactions Lake · PBVI Data Engineering Training Vehicle*