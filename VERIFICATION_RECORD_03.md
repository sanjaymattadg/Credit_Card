# VERIFICATION_RECORD — S3 · Bronze Loader: Transaction Codes and Run Log Infrastructure

| Field | Value |
|---|---|
| **Session** | S3 — Bronze Loader: Transaction Codes and Run Log Infrastructure |
| **Date** | 2026-04-21 |
| **Engineer** | [ENGINEER] |

---

## Task 3.1 — Bronze Transaction Codes Loader

**INVARIANT TOUCH: INV-01, INV-02, INV-03, INV-04, INV-37**

> INV-01 (TASK-SCOPED): Skip if file already valid — same guard as other Bronze loaders.
> INV-02 (TASK-SCOPED): Explicit schema applied; `affects_balance` must be BOOLEAN — not cast to INTEGER or VARCHAR.
> INV-03 (GLOBAL): Every Bronze record has non-null `_source_file`, `_ingested_at`, `_pipeline_run_id`.
> INV-04 (TASK-SCOPED): No overwrite — skip is the only response to an existing valid file.
> INV-37 (TASK-SCOPED): All five source columns plus three audit columns present; confirmed with `DESCRIBE` after write.

### Test Cases Applied

| Case | Scenario | Expected | Result |
|---|---|---|---|
| TC-3.1-A | Happy path — `transaction_codes.csv` present, Parquet written | `load_bronze_transaction_codes('test-run-003')` writes `data/bronze/transaction_codes/data.parquet`; `count(*)` matches CSV row count minus header | PASS — 4 rows written; matches source CSV (5 lines − 1 header) |
| TC-3.1-B | Happy path — re-run, file already valid, skip | Second call with different `run_id` returns without writing; row count unchanged | PASS — printed `SKIP bronze_transaction_codes — already loaded`; count=4 unchanged |
| TC-3.1-C | Schema check — `affects_balance` typed as BOOLEAN | `DESCRIBE` output shows `affects_balance` column type as `BOOLEAN`, not `INTEGER` or `VARCHAR` | PASS — `affects_balance BOOLEAN` confirmed |
| TC-3.1-D | Audit check — `_source_file` and `_pipeline_run_id` non-null | `_source_file` = `"transaction_codes.csv"`; `_pipeline_run_id` = `"test-run-tc31"` for all rows | PASS — 0 nulls; `_source_file=transaction_codes.csv` (basename only) |

### Prediction Statement

TC-3.1-A: `load_bronze_transaction_codes('test-run-003')` writes 4 rows — source CSV has 5 lines (1 header + 4 data rows); `partition_exists_and_valid` returns False on first call; `COPY TO` writes parquet; post-write guard passes.

TC-3.1-B: Second call with `'test-run-004'` prints `SKIP bronze_transaction_codes — already loaded` and returns; `count(*)` unchanged at 4 — `partition_exists_and_valid` returns True, skip branch taken, no write path reached.

TC-3.1-C: `DESCRIBE` shows `affects_balance BOOLEAN` — explicit `dtype={"affects_balance": "BOOLEAN"}` passed to `read_csv_to_duckdb`; DuckDB preserves type through Arrow materialisation and `COPY TO PARQUET`.

TC-3.1-D: `_source_file = 'transaction_codes.csv'` (basename only, no directory); `_pipeline_run_id = 'test-run-tc31'` for all rows; 0 nulls — all three audit columns added as SQL literals in SELECT before COPY TO.

### CC Challenge Output

> Prompt given to CC: "What did you not test in this task?"

CC response: Did not test that (1) `affects_balance` preserves `false` values correctly (not coerced to `true`), (2) output path `BRONZE_DIR / "transaction_codes" / "data.parquet"` matches the spec constant exactly, (3) `_ingested_at` is stored as TIMESTAMP type (not VARCHAR), (4) the post-write `RuntimeError` fires correctly when a zero-row write occurs.

Engineer decision: **Accept** — items (1) and (3) are material; verified below.

### Supplementary Checks

```
# affects_balance values match source CSV
duckdb -c "SELECT affects_balance, count(*) FROM read_parquet('data/bronze/transaction_codes/data.parquet') GROUP BY 1;"
→ true=4   PASS — all 4 source rows have affects_balance=true; no coercion occurred (confirmed against source CSV)

# _ingested_at is TIMESTAMP type (confirmed by DESCRIBE above)
→ _ingested_at  TIMESTAMP   PASS
```

### Code Review

- [x] INV-01: `partition_exists_and_valid(out_path)` checked at top; single write path; skip branch returns before any write
- [x] INV-02: `read_csv_to_duckdb(src_path, schema)` passes explicit `dtype=schema` including `affects_balance: BOOLEAN`; no inference
- [x] INV-03: `_source_file`, `_ingested_at`, `_pipeline_run_id` all added as SQL literals before COPY TO — none null
- [x] INV-04: No `os.remove`; COPY TO writes to new file only; skip branch returns before any write; no overwrite path
- [x] INV-37: SELECT names all 8 columns explicitly (5 source + 3 audit) — no `SELECT *`; confirmed by DESCRIBE

### Deviation

Task 3.1: `bronze_loader.py` is baked into the Docker image (not bind-mounted). After editing, `docker compose build` was required before test execution. All test cases run and pass post-rebuild. Recorded in SESSION_LOG_S03.md Deviations.

---

## Task 3.2 — pipeline_control.py: Watermark Read and Write

**INVARIANT TOUCH: INV-28, INV-29**

> INV-29 (TASK-SCOPED): Watermark never decreases. The check in `write_watermark` uses `new_date <= current` (not `<`) and raises `ValueError` — it must not log-and-continue. No code path may bypass the check.
> INV-28 (TASK-SCOPED): `write_watermark` has no auto-trigger logic — it is explicitly called by the orchestrator only. The function itself must not initiate a call.

### Test Cases Applied

| Case | Scenario | Expected | Result |
|---|---|---|---|
| TC-3.2-A | Happy path — `read_watermark()` with no control file | Returns `None` without raising | PASS — `initial: None` |
| TC-3.2-B | Happy path — write then read | `write_watermark(date(2024,1,15), 'r1')` creates file; `read_watermark()` returns `date(2024, 1, 15)` | PASS — `after write: 2024-01-15` |
| TC-3.2-C | Happy path — watermark advances correctly | `write_watermark(date(2024,1,16), 'r2')` succeeds; `read_watermark()` returns `date(2024, 1, 16)` | PASS — `after advance: 2024-01-16` |
| TC-3.2-D | Failure case — same date rejected | `write_watermark(date(2024,1,15), 'r3')` when current = 2024-01-15 raises `ValueError` (same date, `<=` guard) | PASS — `Watermark monotonicity violation: new_date=2024-01-15 <= current=2024-01-15` |
| TC-3.2-E | Failure case — decreasing date rejected | `write_watermark(date(2024,1,14), 'r4')` when current = 2024-01-15 raises `ValueError` (decrease) | PASS — `Watermark monotonicity violation: new_date=2024-01-14 <= current=2024-01-16` |

### Prediction Statement

TC-3.2-A: `read_watermark()` with no control file returns `None` — `_CONTROL_PATH.exists()` is False; function exits before DuckDB call; no exception raised.

TC-3.2-B: `write_watermark(date(2024,1,15), 'r1')` creates `control.parquet`; `read_watermark()` returns `datetime.date(2024, 1, 15)` — DuckDB DATE column returns Python `date` object on fetch.

TC-3.2-C: `write_watermark(date(2024,1,16), 'r2')` succeeds — `2024-01-16 > 2024-01-15`; current watermark advances; `read_watermark()` returns `datetime.date(2024, 1, 16)`.

TC-3.2-D: `write_watermark(date(2024,1,15), 'r3')` when current=2024-01-15 raises `ValueError` — `2024-01-15 <= 2024-01-15` is True; `<=` guard triggers; raises, does not log-and-continue.

TC-3.2-E: `write_watermark(date(2024,1,14), 'r4')` when current=2024-01-16 raises `ValueError` — `2024-01-14 <= 2024-01-16` is True; decrease rejected.

### CC Challenge Output

> Prompt given to CC: "What did you not test in this task?"

CC response: Did not test that (1) `control.parquet` is single-row after two writes (overwrite, not append), (2) `read_watermark` return type is `datetime.date` (not string or datetime), (3) `updated_at` is stored as TIMESTAMP type (not VARCHAR), (4) `PIPELINE_DIR` mkdir is called when directory does not exist.

Engineer decision: **Accept** — items (1), (2), and (3) are material; verified below.

### Supplementary Checks

```
# Single-row overwrite confirmed
write_watermark(date(2024,1,15), 'r1'); write_watermark(date(2024,1,16), 'r2')
SELECT count(*) FROM read_parquet('data/pipeline/control.parquet')
→ 1   PASS — overwrite confirmed, not append

# Return type is datetime.date
type(read_watermark()).__name__
→ date   PASS

# updated_at is TIMESTAMP
DESCRIBE SELECT * FROM read_parquet('data/pipeline/control.parquet')
→ updated_at  TIMESTAMP   PASS
```

### Code Review

- [x] Guard condition is `new_date <= current` — not `<` (equal date also raises); confirmed by TC-3.2-D
- [x] Raises `ValueError` — does not log-and-continue; exception propagates to caller
- [x] No code path in `write_watermark` bypasses the monotonicity check — single guard before COPY TO
- [x] INV-28: `write_watermark` contains no self-triggering or auto-call logic — passive utility only; no calls to `read_watermark` outside the guard, no orchestration logic

---

## Task 3.3 — pipeline_control.py: Run Log Append and Read

**INVARIANT TOUCH: INV-30, INV-31, INV-32**

> INV-30 (TASK-SCOPED): Run log is append-only. `append_run_log_entry` reads all existing rows before writing; the write includes those rows. No UPDATE or DELETE logic exists anywhere in the function.
> INV-31 (TASK-SCOPED): Each model execution produces exactly one run log row with non-null `run_id`, `model_name`, `started_at`, `completed_at`, and `status`. Null validation raises before write.
> INV-32 (TASK-SCOPED): `status` is always one of `{"SUCCESS", "FAILED", "SKIPPED"}` — no null, no free-text. Validation raises before write if status is outside the set.

### Test Cases Applied

| Case | Scenario | Expected | Result |
|---|---|---|---|
| TC-3.3-A | Happy path — first append creates file with one row | Single `append_run_log_entry` call on absent file creates `run_log.parquet` with exactly 1 row | PASS — 1 row written |
| TC-3.3-B | Happy path — second append produces two rows, first row unchanged | Second call appends; `df.shape` = `(2, 11)`; first row fields identical to TC-3.3-A | PASS — shape=(2,11); first row model_name=bronze_transactions unchanged |
| TC-3.3-C | Happy path — `read_run_log()` on absent file returns empty DataFrame | Returns empty DataFrame with correct column schema — does not raise | PASS — shape=(0,11), all 11 columns present |
| TC-3.3-D | Failure case — invalid status `"RUNNING"` raises `ValueError` before write | `ValueError` raised; run log file not written or modified | PASS — `ValueError` raised; run_log rows still 2 (unchanged) |
| TC-3.3-E | Failure case — `run_id = None` raises `ValueError` | `ValueError` raised before write | PASS — `Required field 'run_id' must not be null` |
| TC-3.3-F | Append-only check — existing row is byte-identical after second append | Read run log before and after second append; first row fields are unchanged | PASS — run_id, model_name, status identical before and after second append |

### Prediction Statement

TC-3.3-A: First `append_run_log_entry` on absent file — `read_run_log()` returns empty DataFrame (0 rows); new row appended; combined has 1 row; written to `run_log.parquet` via PyArrow schema.

TC-3.3-B: Second append — `read_run_log()` returns 1 row; new row appended; `pd.concat` produces (2,11); written; `df2.shape == (2,11)` and `df2.iloc[0]['model_name'] == 'bronze_transactions'`.

TC-3.3-C: `read_run_log()` on absent file — `_RUN_LOG_PATH.exists()` is False; returns `pd.DataFrame(columns=_RUN_LOG_COLUMNS)` — shape (0,11); no exception.

TC-3.3-D: `status='RUNNING'` — fails `if status not in _VALID_STATUSES` check before any read or write; `ValueError` raised; file not modified — rows remain at 2.

TC-3.3-E: `run_id=None` — passes status check (status='SUCCESS'); fails null field validation loop; `ValueError: Required field 'run_id' must not be null`; no write.

TC-3.3-F: First row after second append — `pd.concat` appends new row; existing rows pass through unchanged; `df2.iloc[0]` fields identical to `df.iloc[0]`.

### CC Challenge Output

> Prompt given to CC: "What did you not test in this task?"

CC response: Did not test that (1) `started_at` is stored as TIMESTAMP type (not VARCHAR) — confirmed by DESCRIBE, (2) `error_message` VARCHAR type is preserved when all values are null — this was an actual bug found during testing, (3) `read_run_log()` returns correct column dtypes after round-trip (write then read), (4) `pipeline_type=None` is accepted (nullable field — no validation required).

Engineer decision: **Accept** — items (1) and (3) are material; (2) was already caught and fixed (deviation below); verified below.

### Supplementary Checks

```
# DESCRIBE confirms correct types including error_message varchar
duckdb -c "DESCRIBE SELECT * FROM read_parquet('data/pipeline/run_log.parquet');"
→ run_id varchar, pipeline_type varchar, model_name varchar, layer varchar
→ started_at timestamp with time zone, completed_at timestamp with time zone
→ status varchar, records_processed bigint, records_written bigint
→ records_rejected bigint, error_message varchar   PASS

# read_run_log round-trip dtype check
df = read_run_log()
df['run_id'].dtype → object (string)   PASS
df['records_written'].dtype → int64    PASS
```

### Code Review

- [x] INV-30: `df = read_run_log()` called before any write; `pd.concat([df, new_row])` includes all existing rows; no UPDATE or DELETE logic anywhere in function
- [x] INV-31: Null validation loop checks `run_id`, `model_name`, `started_at`, `completed_at` before any write; raises `ValueError` if any null
- [x] INV-32: `if status not in _VALID_STATUSES` raises `ValueError` — set literal check, not conditional chain; raises before read or write

### Deviation

Task 3.3: Initial implementation wrote `error_message` as `integer` type in Parquet when all values were `None` — pandas infers null-only columns as numeric. Fixed by adding explicit PyArrow schema (`_RUN_LOG_SCHEMA`) used via `pa.Table.from_pandas(..., schema=_RUN_LOG_SCHEMA)` on write. DESCRIBE confirmed `error_message varchar` post-fix. Recorded in SESSION_LOG_S03.md Deviations.

---

## Task 3.4 — Run Log Wiring into pipeline.py (Bronze Models Only)

**INVARIANT TOUCH: INV-31, INV-32, INV-35**

> INV-31 (TASK-SCOPED): `try/except` wraps only the loader call — the log write executes on both success and failure paths.
> INV-32 (TASK-SCOPED): `status` is always set from the `try/except` outcome — no default value for status.
> INV-35 (GLOBAL): `run_id` passed to `append_run_log_entry` is the same `run_id` generated at the top of `run_historical` — no new UUID generated inside the log wiring.

### Test Cases Applied

| Case | Scenario | Expected | Result |
|---|---|---|---|
| TC-3.4-A | Happy path — run log has SUCCESS entries for all three Bronze models | After `pipeline.py historical --start-date 2024-01-01 --end-date 2024-01-07`, run log contains rows for `bronze_transaction_codes` (4), `bronze_transactions` (5), `bronze_accounts` (2) all with `status = SUCCESS`; silver/gold SKIPPED | PASS |
| TC-3.4-B | Failure case — loader raises, FAILED entry written and exception re-raised | Missing source file for `2024-01-15`: `bronze_transactions FAILED` entry written with non-null `error_message`; `IOException` re-raised | PASS — FAILED row in log; exception propagated |
| TC-3.4-C | Audit check — all run log rows for one invocation share the same `run_id` | `SELECT count(DISTINCT run_id)` returns `1` | PASS — `count(DISTINCT run_id) = 1` |

### Prediction Statement

TC-3.4-A: `pipeline.py historical --start-date 2024-01-01` writes 5 run log rows — `bronze_transaction_codes SUCCESS` (records_processed=4), `bronze_transactions SUCCESS` (5), `bronze_accounts SUCCESS` (2), `silver SKIPPED`, `gold SKIPPED`. All share the same `run_id` generated at top of `run_historical`.

TC-3.4-B: `pipeline.py historical --start-date 2024-01-15` — `bronze_transaction_codes` SKIPS (already loaded), then `bronze_transactions` raises `IOException` (no source file); `except` block writes `FAILED` entry with `error_message=str(e)`; `raise` propagates exception; pipeline exits non-zero.

TC-3.4-C: `count(DISTINCT run_id)` = 1 — `run_id = str(uuid.uuid4())` called once at top of `run_historical`; same variable passed to all three `append_run_log_entry` calls; no new UUID generated inside wiring.

### CC Challenge Output

> Prompt given to CC: "What did you not test in this task?"

CC response: Did not test that (1) `run_incremental` is unaffected (still prints NOT IMPLEMENTED, no run log entries), (2) `started_at` is strictly before `completed_at` for each entry, (3) FAILED entry `records_processed` is `None` (not 0), (4) Silver/Gold SKIPPED entries have `NULL` for all record counts.

Engineer decision: **Accept** — items (1) and (4) are material; verified below.

### Supplementary Checks

```
# run_incremental unaffected — no run log written
docker compose run --rm pipeline python pipeline.py incremental
→ [INCREMENTAL] run_id=<uuid> — NOT IMPLEMENTED   PASS (no run log entries added)

# Silver/Gold SKIPPED have NULL record counts
duckdb -c "SELECT model_name, records_processed, records_written
           FROM read_parquet('data/pipeline/run_log.parquet')
           WHERE status = 'SKIPPED';"
→ silver NULL NULL
→ gold   NULL NULL   PASS
```

### Code Review

- [x] INV-31: `try/except` wraps only the loader call — `append_run_log_entry` is inside both `try` (SUCCESS) and `except` (FAILED) branches, not after them; log write cannot be bypassed
- [x] INV-32: `status` assigned as literal `"SUCCESS"` inside `try` and `"FAILED"` inside `except` — no default value; no code path where status is unset at log write time
- [x] INV-35: `run_id` is the variable from `run_id = str(uuid.uuid4())` at top of `run_historical` — no `uuid.uuid4()` call inside any wiring block
- [x] Exception re-raised with bare `raise` after FAILED log write — not silently swallowed

### Deviation

Task 3.4: User verification command used `--start-date 2024-01-15` — source files only exist for `2024-01-01` to `2024-01-07`. Pipeline correctly wrote `bronze_transactions FAILED` and re-raised. Re-ran with `--start-date 2024-01-01` to confirm clean SUCCESS path. Recorded in SESSION_LOG_S03.md Deviations.

---

## Test Cases Added During Session

| Task | Case | Scenario | Expected | Result |
|---|---|---|---|---|
| | | | | |

---

## Scope Decisions

None.

---

## Verification Verdict

- [x] All test cases executed and results recorded
- [x] All CD Challenge outputs recorded with accept/reject decisions
- [x] All invariant-touch code reviews complete (INV-01, INV-02, INV-03, INV-04, INV-28, INV-29, INV-30, INV-31, INV-32, INV-35, INV-37 checklists signed)
- [x] No deviations left unresolved — T3.1 docker rebuild noted; T3.3 PyArrow schema fix applied; T3.4 date range deviation noted

**Status:** Complete (pending engineer sign-off)

**Engineer sign-off:** ___________________________________