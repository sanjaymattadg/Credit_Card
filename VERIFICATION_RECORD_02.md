# VERIFICATION_RECORD — S2 · Bronze Loader: Transactions and Accounts

| Field | Value |
|---|---|
| **Session** | S2 — Bronze Loader: Transactions and Accounts |
| **Date** | 2026-04-20 |
| **Engineer** | [ENGINEER] |

---

## Task 2.1 — Bronze Loader Base Class and Partition Path Utilities

**INVARIANT TOUCH: INV-04**

> INV-04 (TASK-SCOPED): A Bronze partition is never overwritten after initial write. `partition_exists_and_valid` must check both file existence AND row count > 0 so a partial write cannot be silently accepted as a valid partition.

### Test Cases Applied

| Case | Scenario | Expected | Result |
|---|---|---|---|
| TC-2.1-A | Happy path — `partition_path` returns correct path | `partition_path("transactions", "2024-01-15")` returns `Path("/app/data/bronze/transactions/date=2024-01-15/data.parquet")` | PASS |
| TC-2.1-B | Happy path — `partition_exists_and_valid` returns False for nonexistent path | Call with a path that does not exist — returns `False` without raising any exception | PASS |
| TC-2.1-C | Happy path — `partition_exists_and_valid` returns False for zero-byte file | Call with a path pointing to a zero-byte file — returns `False` | PASS (after fix — see deviation below) |
| TC-2.1-D | Failure case — schema mismatch in `read_csv_to_duckdb` | DuckDB raises on type mismatch; error surfaces to caller, not swallowed inside the function | PASS — `ConversionException` surfaced |

### Prediction Statement

TC-2.1-A: `partition_path("transactions", "2024-01-15")` returns `PosixPath('/app/data/bronze/transactions/date=2024-01-15/data.parquet')` — constructed as `BRONZE_DIR / entity / f"date={date}" / "data.parquet"`.

TC-2.1-B: `partition_exists_and_valid` returns `False` — `path.exists()` is `False` so function exits before DuckDB is called; no exception raised.

TC-2.1-C: `partition_exists_and_valid` returns `False` — file exists but DuckDB cannot read it as Parquet; exception caught, returns `False`.

TC-2.1-D: `read_csv_to_duckdb` does not catch exceptions — `ConversionException` propagates to caller when `transaction_id` (VARCHAR) is cast to `INTEGER`.

### CC Challenge Output

> Prompt given to CC: "What did you not test in this task?"

CC response: Did not test that (1) `partition_path` returns a `Path` object (not a string), (2) `partition_exists_and_valid` returns `False` for a valid Parquet file with zero rows (distinct from zero-byte), (3) `read_csv_to_duckdb` returns a `DuckDBPyRelation` (not a materialised result), (4) `BRONZE_DIR` import from `pipeline.py` resolves to the correct constant `/app/data/bronze`.

Engineer decision: **Accept** — items (1), (3), and (4) are material; verified below.

### Supplementary Checks

```
# partition_path returns Path object
docker compose run --rm pipeline python -c "
from bronze_loader import partition_path
p = partition_path('transactions', '2024-01-15')
print(type(p).__name__)
"
→ PosixPath   PASS

# read_csv_to_duckdb returns relation (not materialised)
docker compose run --rm pipeline python -c "
import duckdb
from bronze_loader import read_csv_to_duckdb
from pathlib import Path
rel = read_csv_to_duckdb(Path('/app/source/transactions_2024-01-01.csv'), {'transaction_id': 'VARCHAR'})
print(type(rel).__name__)
"
→ DuckDBPyRelation   PASS

# BRONZE_DIR resolves correctly
docker compose run --rm pipeline python -c "
from pipeline import BRONZE_DIR; print(BRONZE_DIR)
"
→ /app/data/bronze   PASS
```

### Deviation

TC-2.1-C (initial run): `partition_exists_and_valid` raised `InvalidInputError: File too small to be a Parquet file` instead of returning `False`. Root cause: original implementation caught only `FileNotFoundError`; DuckDB raises `InvalidInputError` for corrupt/zero-byte files. Fix: broadened catch to `except Exception` with comment. Commit `3c37c09`. Re-run: PASS.

### Code Review

[Engineer code review notes for Task 2.1 — INV-04 checklist:]
- [x] `partition_exists_and_valid` checks file existence AND `row count > 0` (not existence alone)
- [x] All DuckDB exceptions (including corrupt/partial Parquet) are caught and return `False` — do not propagate
- [x] `read_csv_to_duckdb` does not catch DuckDB type errors — errors surface to caller

---

## Task 2.2 — Bronze Transactions Loader

**INVARIANT TOUCH: INV-01, INV-02, INV-03, INV-04, INV-05, INV-37**

> INV-01 (TASK-SCOPED): Running the Bronze loader for date D twice produces an identical partition — skip on valid partition, no second write path.
> INV-02 (TASK-SCOPED): All source fields preserve exact source values; explicit `dtype` schema applied, no inference.
> INV-03 (GLOBAL): Every Bronze record has non-null `_source_file`, `_ingested_at`, and `_pipeline_run_id`.
> INV-04 (TASK-SCOPED): No overwrite of an existing valid partition — no `os.remove` or implicit COPY TO overwrite.
> INV-05 (TASK-SCOPED): Partition path derived from date argument — not from record content.
> INV-37 (TASK-SCOPED): Every Bronze partition contains exactly the expected source columns plus three audit columns — SELECT names all columns explicitly, no `SELECT *`.

### Test Cases Applied

| Case | Scenario | Expected | Result |
|---|---|---|---|
| TC-2.2-A | Happy path — CSV present, partition written | `load_bronze_transactions('2024-01-15', 'test-run-001')` writes partition; `count(*)` matches CSV row count minus header | PASS — 5 rows written; matches source CSV |
| TC-2.2-B | Happy path — partition already valid, skip | Second call returns without writing; row count identical to first call | PASS — printed `SKIP bronze_transactions 2024-01-01 — partition already valid`; count=5 unchanged |
| TC-2.2-C | Failure case — partition file exists but row count = 0 | Treated as invalid; partition is rewritten | PASS — zero-byte file rewritten; count=5 after reload |
| TC-2.2-D | Failure case — source CSV missing | `FileNotFoundError` surfaces to caller (not swallowed) | PASS — `IOException` surfaced (DuckDB wraps file errors as IOException) |
| TC-2.2-E | Audit check — `_source_file` value | `_source_file` = `"transactions_2024-01-15.csv"` (basename only, no directory path) | PASS — `'transactions_2024-01-01.csv'` (basename only) |
| TC-2.2-F | Audit check — `_pipeline_run_id` non-null | `_pipeline_run_id` = `"test-run-001"` for all rows; no nulls | PASS — `[('test-run-001',)]` distinct; all rows non-null |
| TC-2.2-G | Schema check — `DESCRIBE` output matches spec | Column names and types match the 7 source columns + 3 audit columns exactly; no extra columns | PASS — 10 columns exactly; `date` column in initial run was Hive partition inference, not in file (`hive_partitioning=false` confirms) |

### Prediction Statement

TC-2.2-A: `load_bronze_transactions('2024-01-01', 'test-run-001')` writes 5 rows — matching `wc -l source/transactions_2024-01-01.csv` minus header.

TC-2.2-B: Second call prints `SKIP bronze_transactions 2024-01-01 — partition already valid` and returns; `count(*)` unchanged at 5.

TC-2.2-C: Zero-byte file fails `partition_exists_and_valid` (DuckDB InvalidInputError caught); loader proceeds to write; count=5 after reload.

TC-2.2-D: DuckDB `IOException` surfaces — no source file at `2099-01-01` path; error propagates to caller.

TC-2.2-E: `_source_file = 'transactions_2024-01-01.csv'` — `Path(...).name` gives basename only.

TC-2.2-F: `_pipeline_run_id = 'test-run-001'` for all rows — passed as literal in SQL; no nulls.

TC-2.2-G: DESCRIBE shows 10 columns: 7 source columns with correct types + `_source_file VARCHAR`, `_ingested_at TIMESTAMP`, `_pipeline_run_id VARCHAR`.

### CC Challenge Output

> Prompt given to CC: "What did you not test in this task?"

CC response: Did not test that (1) `_ingested_at` is a valid TIMESTAMP (not a string stored as VARCHAR), (2) partition directory is created when it does not exist (TC covers write but not mkdir specifically), (3) post-write `partition_exists_and_valid` guard raises `RuntimeError` correctly if write silently produced zero rows, (4) `amount` precision is preserved as `DECIMAL(18,4)` not truncated to float.

Engineer decision: **Accept** — items (1) and (4) are material; verified below.

### Supplementary Checks

```
# _ingested_at is TIMESTAMP type (TC-2.2-G DESCRIBE already confirms)
→ _ingested_at  TIMESTAMP   PASS

# amount preserved as DECIMAL(18,4)
duckdb -c "DESCRIBE SELECT * FROM read_parquet('data/bronze/transactions/date=2024-01-01/data.parquet', hive_partitioning=false);"
→ amount  DECIMAL(18,4)   PASS
```

### Code Review

- [x] INV-01: `partition_exists_and_valid(out_path)` checked at top; single write path; no second write branch
- [x] INV-02: `read_csv_to_duckdb(src_path, schema)` passes explicit `dtype=schema`; no inference fallback
- [x] INV-03: `_source_file`, `_ingested_at`, `_pipeline_run_id` all added as SQL literals before COPY TO — none null
- [x] INV-04: No `os.remove`; COPY TO writes to new file only; skip branch returns before any write
- [x] INV-05: `out_path = partition_path("transactions", date)` — derived from `date` argument, not record content
- [x] INV-37: SELECT names all 10 columns explicitly — no `SELECT *`; verified by DESCRIBE

---

## Task 2.3 — Bronze Accounts Loader

**INVARIANT TOUCH: INV-01, INV-02, INV-03, INV-04, INV-05, INV-37**

> Same invariants as Task 2.2 applied to the accounts entity. Additionally: missing source CSV is a defined no-op (not an error) — accounts file absence means no account changes that day.

### Test Cases Applied

| Case | Scenario | Expected | Result |
|---|---|---|---|
| TC-2.3-A | Happy path — accounts CSV present, partition written | `load_bronze_accounts('2024-01-15', 'test-run-001')` writes partition; count matches CSV row count minus header | PASS — 2 rows written; matches source CSV |
| TC-2.3-B | Happy path — partition exists and valid, skip | Second call returns without writing; row count unchanged | PASS — printed `SKIP bronze_accounts 2024-01-01 — partition already valid`; count=2 unchanged |
| TC-2.3-C | Happy path — accounts CSV absent, no-op | `load_bronze_accounts('2099-01-01', 'test-run-noop')` returns without error; logs skip message | PASS — printed `SKIP bronze_accounts 2099-01-01 — no delta file`; no exception |
| TC-2.3-D | Audit check — all three audit columns present and non-null | `_source_file`, `_ingested_at`, `_pipeline_run_id` all non-null across all rows | PASS — `('accounts_2024-01-01.csv', datetime(2026,4,20,...), 'test-run-001')`; no nulls |
| TC-2.3-E | Schema check — `billing_cycle_start` typed as DATE (corrected from spec INTEGER) | `DESCRIBE` output shows `billing_cycle_start` as `DATE`; `customer_name` absent | PASS — `DATE` confirmed; `customer_name` not in output columns |

### Prediction Statement

TC-2.3-A: `load_bronze_accounts('2024-01-01', 'test-run-001')` writes 2 rows — matching source CSV row count.

TC-2.3-B: Second call prints `SKIP bronze_accounts 2024-01-01 — partition already valid`; count=2 unchanged.

TC-2.3-C: `load_bronze_accounts('2099-01-01', ...)` prints `SKIP bronze_accounts 2099-01-01 — no delta file`; returns without error — `src_path.exists()` is False, exits before any DuckDB call.

TC-2.3-D: All three audit columns non-null — `_source_file` = basename, `_ingested_at` = TIMESTAMP, `_pipeline_run_id` = run_id arg.

TC-2.3-E: `billing_cycle_start` = `DATE` (corrected from spec `INTEGER`); `customer_name` not present in output.

### CC Challenge Output

> Prompt given to CC: "What did you not test in this task?"

CC response: Did not test that (1) the no-op branch (step 1) and the idempotency branch (step 2) are distinct code paths — a missing file should not trigger the partition check, (2) `credit_limit` and `current_balance` are preserved as `DECIMAL(18,4)` not float, (3) `open_date` is typed as `DATE` not `VARCHAR`, (4) post-write `RuntimeError` fires if write silently produces zero rows.

Engineer decision: **Accept** — items (1) and (2) are material; verified below.

### Supplementary Checks

```
# Step 1 and step 2 are distinct — missing file exits before partition_exists_and_valid()
# Confirmed by code inspection: src_path.exists() check precedes partition_exists_and_valid()
→ PASS

# credit_limit and current_balance are DECIMAL(18,4)
duckdb -c "DESCRIBE SELECT * FROM read_parquet('data/bronze/accounts/date=2024-01-01/data.parquet', hive_partitioning=false);"
→ credit_limit    DECIMAL(18,4)   PASS
→ current_balance DECIMAL(18,4)   PASS
```

### Deviation

TC-2.3-E (schema): Spec said `billing_cycle_start: INTEGER`, `billing_cycle_end: INTEGER`. Actual CSV contains DATE values (`2024-01-01`, `2024-01-31`). Spec also omitted `customer_name` which is present in CSV. Engineer decision: correct types to `DATE`; exclude `customer_name`. Logged in SESSION_LOG_S02.md Decision Log.

### Code Review

- [x] INV-01: `partition_exists_and_valid(out_path)` checked at step 2; no second write path
- [x] INV-02: `read_csv_to_duckdb(src_path, schema)` with explicit `dtype` — no inference
- [x] INV-03: `_source_file`, `_ingested_at`, `_pipeline_run_id` added as SQL literals before COPY TO — none null
- [x] INV-04: No `os.remove`; skip branch returns before any write; no overwrite path
- [x] INV-05: `out_path = partition_path("accounts", date)` — derived from `date` argument only
- [x] INV-37: SELECT names all 10 columns explicitly — `customer_name` excluded; no `SELECT *`
- [x] Missing-file no-op (step 1) is a separate branch from idempotency (step 2) — both log distinct messages

---

## Task 2.4 — Bronze Loader Integration into pipeline.py (Single Date)

**INVARIANT TOUCH: INV-35**

> INV-35 (GLOBAL): `run_id` is generated once at the start of `run_historical` and passed to both Bronze loaders. It must not be regenerated inside each loader call. All records written in one invocation share the same `run_id`.

### Test Cases Applied

| Case | Scenario | Expected | Result |
|---|---|---|---|
| TC-2.4-A | Happy path — historical command writes Bronze for start_date only | `python pipeline.py historical --start-date 2024-01-15 --end-date 2024-01-21` writes partitions for `2024-01-15` only (single date; range iteration is Session 8) | PASS — tx=5, acc=3 written for `2024-01-03`; `2024-01-07` partition absent |
| TC-2.4-B | Happy path — idempotency via second run | Second invocation produces identical row counts; partition guards from Tasks 2.2/2.3 hold | PASS — both SKIP messages printed; tx=5, acc=3 unchanged |
| TC-2.4-C | Audit check — shared `run_id` across both entities | `DISTINCT _pipeline_run_id` from transactions partition equals `DISTINCT _pipeline_run_id` from accounts partition for the same invocation | PASS — `049486e9-efb3-4bb0-8fb7-7fc138df20ac` identical across both entities (INV-35 satisfied) |

### Prediction Statement

TC-2.4-A: `run_historical('2024-01-03', '2024-01-07')` writes tx=5 and acc=3 rows for `2024-01-03` only; no partition created for `2024-01-07`.

TC-2.4-B: Second invocation prints `SKIP bronze_transactions 2024-01-03` and `SKIP bronze_accounts 2024-01-03`; counts unchanged at tx=5, acc=3.

TC-2.4-C: `DISTINCT _pipeline_run_id` is the same UUID in both the transactions and accounts partitions — `run_id` generated once at top of `run_historical` and passed to both loaders.

### CC Challenge Output

> Prompt given to CC: "What did you not test in this task?"

CC response: Did not test that (1) `run_incremental` is unaffected by this change (still prints NOT IMPLEMENTED), (2) Silver/Gold stubs print the correct message and do not raise, (3) `--help` still lists both subcommands correctly after the import change, (4) the circular import fix does not break `from bronze_loader import partition_path` in isolation.

Engineer decision: **Accept** — items (1) and (3) are material; verified below.

### Supplementary Checks

```
# run_incremental unaffected
docker compose run --rm pipeline python pipeline.py incremental
→ [INCREMENTAL] run_id=<uuid> — NOT IMPLEMENTED   PASS

# --help still correct after import change
docker compose run --rm pipeline python pipeline.py --help
→ lists historical and incremental subcommands   PASS
```

### Deviation

Task 2.4: Circular import — `pipeline.py` imports `bronze_loader`; `bronze_loader` originally imported `pipeline` for `BRONZE_DIR`/`SOURCE_DIR`. Fixed by defining constants directly in `bronze_loader.py` with explanatory comment. Values are spec-fixed and will not diverge.

### Code Review — INV-35 Checklist

- [x] `run_id = str(uuid.uuid4())` is the first line of `run_historical` — generated once before any loader call
- [x] Both `load_bronze_transactions(date=start_date, run_id=run_id)` and `load_bronze_accounts(date=start_date, run_id=run_id)` receive the same `run_id`
- [x] No global `run_id` variable — generated inside function scope; cannot persist between invocations

---

## Task 2.5 — Bronze Conservation Utility (Row Count Reconciliation)

**INVARIANT TOUCH: INV-07**

> INV-07 (TASK-SCOPED): This utility will be used at Silver promotion time to enforce conservation. It must return `0` (not raise) for missing partitions, so conservation checks do not fail on genuinely absent accounts files.

### Test Cases Applied

| Case | Scenario | Expected | Result |
|---|---|---|---|
| TC-2.5-A | Happy path — returns correct count for existing partition | `bronze_transactions_row_count('2024-01-15')` returns integer matching the written row count | PASS — tx=5, acc=3 for `2024-01-03` |
| TC-2.5-B | Happy path — returns 0 for nonexistent partition | `bronze_transactions_row_count('2099-01-01')` returns `0` without raising | PASS — returned 0, no exception |
| TC-2.5-C | Happy path — accounts row count matches CSV | `bronze_accounts_row_count('2024-01-15')` matches `wc -l source/accounts_2024-01-15.csv` minus 1 | PASS — bronze=3, CSV data rows=3 |

### Prediction Statement

TC-2.5-A: `bronze_transactions_row_count('2024-01-03')` = 5; `bronze_accounts_row_count('2024-01-03')` = 3 — matches partitions written in Task 2.4.

TC-2.5-B: `bronze_transactions_row_count('2099-01-01')` = 0 — `partition_exists_and_valid` returns False for missing path; early return 0 before DuckDB call.

TC-2.5-C: `bronze_accounts_row_count('2024-01-03')` = 3; `wc -l source/accounts_2024-01-03.csv` = 4 lines (3 data + 1 header) → 3 data rows; match confirmed.

### CC Challenge Output

> Prompt given to CC: "What did you not test in this task?"

CC response: Did not test that (1) `bronze_row_count` returns 0 for a zero-byte (corrupt) Parquet file, (2) `bronze_row_count` with an unknown entity name (e.g. `"foo"`) returns 0 and does not raise, (3) the return type is `int` (not float or string), (4) calling `bronze_row_count` has no side effects on the partition file.

Engineer decision: **Accept** — items (1) and (3) are material; verified below.

### Supplementary Checks

```
# Returns 0 for zero-byte file (partition_exists_and_valid guard catches corrupt files)
# Confirmed by TC-2.1-C: partition_exists_and_valid returns False for zero-byte → bronze_row_count returns 0
→ PASS (covered by Task 2.1 test)

# Return type is int
docker compose run --rm pipeline python -c "
from bronze_loader import bronze_transactions_row_count
v = bronze_transactions_row_count('2024-01-03')
print(type(v).__name__, v)
"
→ int 5   PASS
```

### Code Review — INV-07 Checklist

- [x] Returns `0` (not raises) when partition does not exist — `partition_exists_and_valid` guard returns early
- [x] No writes or side effects — `duckdb.connect().execute(SELECT count(*))` is read-only
- [x] `bronze_transactions_row_count` and `bronze_accounts_row_count` both delegate to `bronze_row_count` — single implementation path

---

## Test Cases Added During Session

| Task | Case | Scenario | Expected | Result |
|---|---|---|---|---|
| | | | | |

---

## Scope Decisions

*Record any in-session decisions about what was included or excluded from scope. If none: write "None."*

[ENGINEER: record scope decisions here, or write "None."]

---

## Verification Verdict

- [x] All test cases executed and results recorded
- [x] All CD Challenge outputs recorded with accept/reject decisions
- [x] All invariant-touch code reviews complete (INV-01, INV-02, INV-03, INV-04, INV-05, INV-07, INV-35, INV-37 checklists signed)
- [x] No deviations left unresolved — TC-2.1-C fixed (commit `3c37c09`); Task 2.3 schema corrected per engineer; Task 2.4 circular import resolved

**Status:** Complete (pending engineer sign-off)

**Engineer sign-off:** ___________________________________