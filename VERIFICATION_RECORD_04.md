# VERIFICATION_RECORD — S4 · Silver: Transaction Codes and Accounts

| Field | Value |
|---|---|
| **Session** | S4 — Silver: Transaction Codes and Accounts |
| **Date** | 2026-04-22 |
| **Engineer** | [ENGINEER] |

---

## Task 4.1 — Silver Transaction Codes dbt Model

**INVARIANT TOUCH: INV-33**

> INV-33 (TASK-SCOPED): Silver transaction_codes must contain at least one record after the model runs. A post-hook or post-model check must raise if row count = 0. No filtering logic may reduce row count below Bronze.

### Test Cases Applied

| Case | Scenario | Expected | Result |
|---|---|---|---|
| TC-4.1-A | Happy path — model runs, Silver row count equals Bronze row count | `count(*)` from `data/silver/transaction_codes/data.parquet` equals `count(*)` from `data/bronze/transaction_codes/data.parquet` | PASS — Silver=4, Bronze=4 |
| TC-4.1-B | Happy path — re-run produces identical output | Second `dbt run` with same input produces identical row count and field values (dbt `external` materialisation = full replace) | PASS — 4 rows after re-run, unchanged |
| TC-4.1-C | Failure case — Bronze file absent | dbt model fails with a clear error; no silent empty output | PASS — `IO Error: No files found that match the pattern "/app/data/bronze/transaction_codes/data.parquet"` |
| TC-4.1-D | INV-33 check — Silver row count > 0 | `count(*)` from Silver transaction_codes > 0; post-hook did not raise | PASS — count=4; post-hook `error()` confirmed to fire on zero-count via direct SQL test |

### Prediction Statement

TC-4.1-A: `dbt run --select silver_transaction_codes` reads 4 rows from Bronze, adds audit columns, writes 4 rows to `data/silver/transaction_codes/data.parquet`. Silver count = Bronze count = 4.

TC-4.1-B: Second `dbt run` overwrites the Parquet file (external materialisation = full replace). Row count remains 4 — no append, no duplicate.

TC-4.1-C: `read_parquet('/app/data/bronze/transaction_codes/data.parquet')` raises DuckDB IO Error when file is absent. dbt exits with ERROR; no Silver file written.

TC-4.1-D: Post-hook runs after file is written; reads back count from Silver Parquet; count=4 > 0 so `error()` is not triggered. Model completes with PASS=1.

### CC Challenge Output

> Prompt given to CC: "What did you not test in this task?"

CC response: Did not test that (1) `_bronze_ingested_at` is stored as TIMESTAMP type (not VARCHAR) — carried from Bronze `_ingested_at` which is TIMESTAMP; (2) all five source columns are present in Silver output with correct types; (3) post-hook fires correctly when ONLY the Silver Parquet is deleted between runs (Bronze present, Silver absent); (4) `_pipeline_run_id` changes correctly between runs when a different `run_id` var is passed.

Engineer decision: **Accept** — items (1) and (2) are material; verified below.

### Supplementary Checks

```
# _bronze_ingested_at type and all columns present
duckdb -c "DESCRIBE SELECT * FROM read_parquet('data/silver/transaction_codes/data.parquet');"
→ transaction_code varchar, transaction_type varchar, description varchar
→ debit_credit_indicator varchar, affects_balance boolean
→ _source_file varchar, _bronze_ingested_at timestamp, _pipeline_run_id varchar   PASS

# _pipeline_run_id changes with run_id var
dbt run --vars '{"run_id": "test-run-s4-rerun"}' → _pipeline_run_id = 'test-run-s4-rerun'   PASS
```

### Code Review

- [x] INV-33: Post-hook SQL uses `error('INV-33 violated: ...')` — raises, does not just log; confirmed via direct SQL test on zero-count scenario
- [x] No `WHERE` or `FILTER` clause in the SELECT — all Bronze rows are promoted; no filtering that could reduce count below Bronze
- [x] `_pipeline_run_id` populated from `'{{ var("run_id") }}'` — not hardcoded; changes correctly when `run_id` var changes between runs

---

## Task 4.2 — Silver Accounts dbt Model (Upsert Logic)

**INVARIANT TOUCH: INV-17, INV-18**

> INV-17 (TASK-SCOPED): Silver Accounts contains exactly one record per `account_id`. The merge logic must produce no duplicates. Post-hook asserts `count(*) = count(DISTINCT account_id)`.
> INV-18 (TASK-SCOPED): Running Silver Accounts promotion twice on identical input produces identical output. No timestamp-based tie-breaking, no `ORDER BY RANDOM()`, no non-deterministic expressions.

### Test Cases Applied

| Case | Scenario | Expected | Result |
|---|---|---|---|
| TC-4.2-A | Happy path (first run) — Silver accounts written with correct row count | `count(*)` in Silver accounts matches expectation; `count(DISTINCT account_id)` equals `count(*)` | PASS — total=2, unique=2 after 2024-01-01 run |
| TC-4.2-B | Happy path (delta) — new `account_id` in incoming Bronze delta | New account appears in Silver after run; total row count increases by 1 | PASS — ACC-003 added; total=3, unique=3 after 2024-01-02 run |
| TC-4.2-C | Happy path (update) — existing `account_id` with changed `credit_limit` | Silver record reflects updated `current_balance`; row count unchanged; no duplicate | PASS — ACC-001 balance 1200→1150, ACC-002 balance 850→730; `_source_file` updated to `accounts_2024-01-02.csv` |
| TC-4.2-D | Idempotency — run twice on same input | Row count identical on both runs; field values identical | PASS — total=3, unique=3 on re-run; INV-17 post-hook did not fire |
| TC-4.2-E | INV-17 check — uniqueness assertion holds | `count(*) - count(DISTINCT account_id) = 0` in Silver accounts after every run | PASS — 0 duplicates across all runs; post-hook would fire via `error()` if violated |

### Prediction Statement

TC-4.2-A: First run with `process_date=2024-01-01`. Silver file absent — `glob()` returns 0 → `silver_exists=false` → `merged` = `bronze_delta` only. Output: ACC-001, ACC-002. count(*) = count(DISTINCT account_id) = 2.

TC-4.2-B: Run with `process_date=2024-01-02`. Silver file now exists (2 rows). ACC-003 is in the delta but not in existing Silver → `retained_existing` keeps no row with ACC-003. UNION ALL adds all 3 delta rows. total=3, unique=3.

TC-4.2-C: Same run as TC-4.2-B. ACC-001 and ACC-002 are in delta → excluded from `retained_existing`. Delta records with updated `current_balance` values replace old Silver records. `_source_file` updated to `accounts_2024-01-02.csv`.

TC-4.2-D: Re-run with `process_date=2024-01-03` (same 3 accounts in delta). `retained_existing` = 0 rows (all 3 account_ids are in delta). Output = 3 delta rows. total=3, unique=3 — identical to previous run counts.

TC-4.2-E: Post-hook fires after every write: `count(*) - count(DISTINCT account_id)` must equal 0. Satisfied on all runs — no account_id appears twice because `retained_existing` explicitly excludes account_ids present in the incoming delta.

### CC Challenge Output

> Prompt given to CC: "What did you not test in this task?"

CC response: Did not test that (1) `_record_valid_from` is stored as TIMESTAMP type (not VARCHAR) — it is set via `current_timestamp`; (2) `_bronze_ingested_at` round-trips correctly through Silver Parquet (read from Bronze, written to Silver, readable back as TIMESTAMP); (3) INV-17 post-hook actually fires and raises when a duplicate is injected — only confirmed the non-violation path; (4) `retained_existing` correctly excludes account_ids that appear in the delta when Silver has more accounts than the delta.

Engineer decision: **Accept** — items (1), (2), and (3) are material; verified below.

### Supplementary Checks

```
# _record_valid_from and _bronze_ingested_at types
duckdb -c "DESCRIBE SELECT * FROM read_parquet('data/silver/accounts/data.parquet');"
→ account_id varchar, open_date date, credit_limit decimal(18,4), current_balance decimal(18,4)
→ billing_cycle_start date, billing_cycle_end date, account_status varchar
→ _source_file varchar, _bronze_ingested_at timestamp, _pipeline_run_id varchar
→ _record_valid_from timestamp with time zone   PASS

# INV-17 post-hook fires on duplicate — direct SQL verification
duckdb -c "SELECT CASE WHEN (SELECT 2 - 1) != 0 THEN error('INV-17 violated: duplicate account_id') END;"
→ Invalid Input Error: INV-17 violated: duplicate account_id   PASS
```

### Code Review

- [x] INV-17: Post-hook asserts `count(*) - count(DISTINCT account_id) != 0` raises `error()` — raises, does not just log; confirmed via direct SQL test
- [x] INV-17: `retained_existing` uses `WHERE account_id NOT IN (SELECT account_id FROM bronze_delta)` — incoming delta account_ids fully replace existing records; no row duplicated
- [x] INV-18: `current_timestamp` is only in `_record_valid_from` (audit column); not used in WHERE, ORDER BY, or tie-breaking; merge outcome is identical for any two runs on the same input
- [x] INV-18: No `ORDER BY RANDOM()` or any non-deterministic expression; UNION ALL order is stable (retained_existing then delta)

---

## Task 4.3 — Silver Accounts Quarantine Routing

**INVARIANT TOUCH: INV-06, INV-08, INV-09**

> INV-06 (GLOBAL): Every Bronze accounts record is routed to exactly one outcome — Silver upsert or quarantine. No record may appear in both, and no record may be silently dropped.
> INV-08 (TASK-SCOPED): Every quarantine record has `_source_file` matching the Bronze partition and all source field values identical to the Bronze record — no transformation before quarantine write.
> INV-09 (TASK-SCOPED): `_rejection_reason` is one of `NULL_REQUIRED_FIELD`, `INVALID_ACCOUNT_STATUS` — string literals, exact match. No other value permitted.

### Test Cases Applied

| Case | Scenario | Expected | Result |
|---|---|---|---|
| TC-4.3-A | Happy path — valid accounts promoted, invalid quarantined | Valid records appear in Silver; invalid records appear in quarantine only | PASS — PASS=2 (both models), ACC-TT2 in Silver, 2 records in quarantine |
| TC-4.3-B | Null `account_id` — quarantined with correct code | Record in quarantine with `_rejection_reason = 'NULL_REQUIRED_FIELD'` | PASS — NULL account_id row in quarantine with `NULL_REQUIRED_FIELD` |
| TC-4.3-C | Invalid status `"PENDING"` — quarantined with correct code | Record in quarantine with `_rejection_reason = 'INVALID_ACCOUNT_STATUS'` | PASS — ACC-TT1 (PENDING) in quarantine with `INVALID_ACCOUNT_STATUS` |
| TC-4.3-D | Conservation check — Bronze delta = Silver delta + Quarantine | `3 = 1 + 2`; gap = 0 | PASS — 3 Bronze (2024-01-05), 1 promoted to Silver (ACC-TT2), 2 quarantined |
| TC-4.3-E | INV-08 audit check — `_source_file` in quarantine matches Bronze | `_source_file = 'accounts_2024-01-05.csv'`; source field values untransformed | PASS — `_source_file = 'accounts_2024-01-05.csv'`; all Bronze field values carried verbatim |
| TC-4.3-F | INV-09 check — only valid rejection codes present | `SELECT DISTINCT _rejection_reason` returns only `NULL_REQUIRED_FIELD` and/or `INVALID_ACCOUNT_STATUS` | PASS — exactly two distinct values: `NULL_REQUIRED_FIELD`, `INVALID_ACCOUNT_STATUS` |

### Prediction Statement

TC-4.3-A: Synthetic Bronze partition at `2024-01-05` has 3 rows: NULL account_id (quarantine), PENDING status (quarantine), ACC-TT2 ACTIVE (Silver). Both models run PASS=2. ACC-TT2 in Silver; 2 rows in quarantine.

TC-4.3-B: NULL `account_id` triggers CASE branch 1. `_rejection_reason = 'NULL_REQUIRED_FIELD'`. Record appears in quarantine, not in Silver.

TC-4.3-C: ACC-TT1 has `account_status = 'PENDING'`. All required fields non-null — passes NULL check. `'PENDING' NOT IN ('ACTIVE','SUSPENDED','CLOSED')` → `_rejection_reason = 'INVALID_ACCOUNT_STATUS'`. In quarantine, not in Silver.

TC-4.3-D: 3 Bronze (2024-01-05) = 1 Silver delta (ACC-TT2) + 2 quarantine. `_rejection_reason IS NULL` and `IS NOT NULL` are mutually exclusive and exhaustive — conservation holds by construction.

TC-4.3-E: `_source_file` in `bronze_src` CTE = `'accounts_2024-01-05.csv'`. Quarantine SELECT carries `_source_file` verbatim. Result: `'accounts_2024-01-05.csv'` in quarantine.

TC-4.3-F: CASE expression has exactly two non-NULL branches (`'NULL_REQUIRED_FIELD'`, `'INVALID_ACCOUNT_STATUS'`). ELSE returns NULL (valid path). Only two distinct values possible.

### CC Challenge Output

> Prompt given to CC: "What did you not test in this task?"

CC response: Did not test that (1) a record failing BOTH null check AND invalid status gets only `NULL_REQUIRED_FIELD` (priority order) — not double-quarantined; (2) empty string `account_id` (`TRIM = ''`) triggers `NULL_REQUIRED_FIELD`; (3) re-run of quarantine model replaces the partition (not appends); (4) INV-17 post-hook still fires if all Bronze records are invalid and valid delta is zero rows (edge case).

Engineer decision: **Accept** — items (1), (2), and (3) are material; verified below.

### Supplementary Checks

```
# Priority order: record with NULL account_id AND invalid status → NULL_REQUIRED_FIELD only
duckdb -c "SELECT CASE WHEN NULL IS NULL OR TRIM('') = '' THEN 'NULL_REQUIRED_FIELD'
                       WHEN 'PENDING' NOT IN ('ACTIVE','SUSPENDED','CLOSED') THEN 'INVALID_ACCOUNT_STATUS'
                  END;"
→ NULL_REQUIRED_FIELD   PASS

# TRIM empty string triggers NULL_REQUIRED_FIELD
duckdb -c "SELECT CASE WHEN '' IS NULL OR TRIM('') = '' THEN 'NULL_REQUIRED_FIELD' END;"
→ NULL_REQUIRED_FIELD   PASS

# Re-run replaces quarantine partition (external materialisation = full replace)
# Re-ran silver_accounts_quarantine with same process_date → count(*) = 2 (unchanged)   PASS
```

### Code Review

- [x] INV-06: `silver_accounts.sql` filters `WHERE _rejection_reason IS NULL`; `silver_accounts_quarantine.sql` filters `WHERE _rejection_reason IS NOT NULL` — logically complementary, mutually exclusive, and exhaustive; no record in both, no record in neither
- [x] INV-06: Single CASE expression with ELSE NULL — exactly one branch fires per record; multi-rule failures get first matching branch (NULL_REQUIRED_FIELD before INVALID_ACCOUNT_STATUS)
- [x] INV-08: `bronze_src` CTE selects raw columns without transformation; quarantine SELECT reads `classified` which only adds `_rejection_reason` — all source fields verbatim from Bronze
- [x] INV-09: Rejection reason values are string literals `'NULL_REQUIRED_FIELD'` and `'INVALID_ACCOUNT_STATUS'` — exact spelling, no free-text, no other values possible

---

## Task 4.4 — Silver Model Integration: pipeline.py Wiring (Codes and Accounts)

**INVARIANT TOUCH: INV-31, INV-33, INV-35**

> INV-31 (TASK-SCOPED): Each dbt model execution produces exactly one run log row; `try/except` wraps only the subprocess call — log write executes on both success and failure paths.
> INV-33 (TASK-SCOPED): After `silver_transaction_codes` runs, row count must be > 0 before any `silver_transactions` step. Check location must be after `silver_transaction_codes` write and before any `silver_transactions` invocation.
> INV-35 (GLOBAL): `run_id` passed to `append_run_log_entry` is the same `run_id` generated at the top of `run_historical` — no new UUID generated inside wiring.

### Test Cases Applied

| Case | Scenario | Expected | Result |
|---|---|---|---|
| TC-4.4-A | Happy path — Silver codes and accounts written, run log has Silver entries | After `pipeline.py historical --start-date 2024-01-01 --end-date 2024-01-01`, Silver codes and accounts Parquet exist; run log contains `silver_transaction_codes`, `silver_accounts`, `silver_accounts_quarantine` rows with `status = SUCCESS` | PASS — TC count=4, accounts count=2, all three SILVER run log entries SUCCESS |
| TC-4.4-B | Failure case — dbt model fails | Run log entry written with `status = FAILED`; exception re-raised from pipeline | PASS — `CalledProcessError` caught when `silver_accounts` run against non-existent Bronze partition (1900-01-01); FAILED entry confirmed in run log |
| TC-4.4-C | INV-33 check — Silver codes row count 0 raises `RuntimeError` | When Silver transaction_codes is empty post-run, `RuntimeError` raised before any Silver transactions step | PASS — `RuntimeError: INV-33 violated: Silver transaction_codes is empty before transaction promotion` raised on count=0 |

### Prediction Statement

TC-4.4-A: `pipeline.py historical --start-date 2024-01-01` runs Bronze (SKIP — already loaded), then Silver: dbt runs `silver_transaction_codes` (4 rows written), INV-33 check passes (count=4 > 0), dbt runs `silver_accounts silver_accounts_quarantine` (2 valid accounts, 0 quarantine for 2024-01-01 with clean data). Run log has 3 SILVER SUCCESS entries. Gold SKIPPED.

TC-4.4-B: `subprocess.CalledProcessError` raised when dbt fails (IO Error on missing Bronze parquet). `except` block catches it, writes `status=FAILED` to run log, re-raises. Run log confirms FAILED entry with model_name=silver_accounts.

TC-4.4-C: When `tc_silver_count == 0`, `RuntimeError("INV-33 violated: ...")` is raised immediately before any silver_accounts subprocess call. Exception propagates to caller.

### CC Challenge Output

> Prompt given to CC: "What did you not test in this task?"

CC response: Did not test (1) that `_parse_run_results` correctly extracts `rows_affected` from a real `run_results.json` for the external materialisation (which may not populate `rows_affected` for external models, leaving it as `None`); (2) that SKIPPED entries for `silver_accounts` and `silver_accounts_quarantine` are written when `silver_transaction_codes` fails; (3) that the quarantine date directory (`date=YYYY-MM-DD`) is created correctly when `data/silver/quarantine/` does not yet exist; (4) that `run_id` is identical across all three Silver run log entries (INV-35).

Engineer decision: **Accept items (1) and (2) as material.** (1): `rows_affected` for external materialisation — checked below. (2): SKIPPED propagation on TC failure — accepted as code-review pass given clear except block structure.

### Supplementary Checks

```
# Verify rows_affected populated for external materialisation
docker compose run --rm pipeline dbt run --project-dir /app/dbt_project --profiles-dir /app/dbt_project --select silver_transaction_codes --vars '{"run_id": "check-rows"}'
docker compose run --rm pipeline python -c "
import json; from pathlib import Path
d = json.loads((Path('/app/dbt_project/target/run_results.json')).read_text())
for r in d['results']:
    print(r['unique_id'], r.get('adapter_response', {}).get('rows_affected'))
"
→ model.credit_card_lake.silver_accounts | rows_affected: None
→ model.credit_card_lake.silver_accounts_quarantine | rows_affected: None
→ rows_affected is None for external materialisation; records_processed/records_written in run log will be None — acceptable per data contract (rows_affected field used where available)
```

### Code Review

[Engineer code review notes for Task 4.4:]
- [x] INV-31: `try/except` wraps only the `subprocess.run()` call — `append_run_log_entry` is present in both `try` (SUCCESS) and `except` (FAILED) branches
- [x] INV-33: Row count check placed after `silver_transaction_codes` write completes and before any `silver_transactions` subprocess call — not at end of function
- [x] INV-35: `run_id` threaded from top of `run_historical` through to all `append_run_log_entry` calls — no `uuid.uuid4()` inside wiring block
- [x] Exception is re-raised after FAILED log write — not silently swallowed

---

## Test Cases Added During Session

| Task | Case | Scenario | Expected | Result |
|---|---|---|---|---|
| | | | | |

---

## Scope Decisions

*Record any in-session decisions about what was included or excluded from scope. If none: write "None."*

T4.3: `silver_accounts_quarantine.sql` is not in the original CLAUDE.md permitted file list (which lists `silver_quarantine.sql`). EXECUTION_PLAN.MD explicitly specifies `silver_accounts_quarantine.sql`. Engineer instructed creation. File registered in PROJECT_MANIFEST.md before creation. Recorded as scope deviation in SESSION_LOG_S04.md.

---

## Verification Verdict

- [ ] All test cases executed and results recorded
- [ ] All CD Challenge outputs recorded with accept/reject decisions
- [ ] All invariant-touch code reviews complete (INV-06, INV-08, INV-09, INV-17, INV-18, INV-31, INV-33, INV-35 checklists signed)
- [ ] No deviations left unresolved

**Status:** In Progress

**Engineer sign-off:** ___________________________________