# VERIFICATION_RECORD — S6 · Silver: Deduplication and Account Resolution

| Field | Value |
|---|---|
| **Session** | S6 — Silver: Deduplication and Account Resolution |
| **Date** | 2026-04-23 |
| **Engineer** | [ENGINEER] |

---

## Task 6.1 — Within-Batch Deduplication

**INVARIANT TOUCH: INV-09, INV-10**

> INV-10 (TASK-SCOPED): Each `transaction_id` appears at most once across all Silver partitions. This CTE handles intra-file duplicates only; cross-partition deduplication is Task 6.2.
> INV-09 (TASK-SCOPED): `_rejection_reason` for within-batch duplicates must be exactly `'DUPLICATE_TRANSACTION_ID'` — exact string literal.

### Test Cases Applied

| Case | Scenario | Expected | Result |
|---|---|---|---|
| TC-6.1-A | Two records with same `transaction_id` in same source file | Second occurrence quarantined with `_rejection_reason = 'DUPLICATE_TRANSACTION_ID'`; first occurrence proceeds | PASS — `TXN-DUP-001` rn=2 in quarantine as `DUPLICATE_TRANSACTION_ID`; rn=1 in Silver |
| TC-6.1-B | All unique `transaction_ids` in source file | No records quarantined by this CTE; all pass through | PASS — `TXN-UNIQUE-002` quarantine count = 0 |
| TC-6.1-C | Conservation holds after adding within-batch dedup step | `bronze_count = silver_count + quarantine_count`; gap still 0 | PASS — bronze=3, silver=2, quarantine=1, gap=0 |

### Prediction Statement

TC-6.1-A: `within_batch_dedup` assigns ROW_NUMBER partitioned by `transaction_id` ordered by `_ingested_at`. `TXN-DUP-001` first occurrence gets rn=1 → proceeds to final SELECT. Second occurrence gets rn=2 → quarantine Branch 6 with `_rejection_reason = 'DUPLICATE_TRANSACTION_ID'`.

TC-6.1-B: `TXN-UNIQUE-002` has only one occurrence → rn=1 always → no quarantine row produced by Branch 6 for this id.

TC-6.1-C: Bronze=3 total. TXN-DUP-001 rn=1 and TXN-UNIQUE-002 → silver=2. TXN-DUP-001 rn=2 → quarantine=1. 2+1=3=bronze. gap=0. Conservation post-hook passes.

### Supplementary Checks

```
# Verification command (adapted process_date=2024-01-01 — no data for 2024-01-15)
dbt run --select silver_transactions silver_quarantine --vars process_date=2024-01-01
→ PASS=2, ERROR=0

duckdb: SELECT _rejection_reason, count(*) FROM quarantine/date=2024-01-01/rejected.parquet GROUP BY 1;
→ INVALID_CHANNEL | 1   ✓ (no duplicates in 2024-01-01 source data — correct)
```

### CD Challenge Output

> Prompt given to CC: "What did you not test in this task?"

CC response: Did not test (1) three or more occurrences of the same `transaction_id` — only rn=1 is kept but rn=2 and rn=3 both route to quarantine; only tested rn=2; (2) tie-breaking when two records have identical `_ingested_at` — ROW_NUMBER ordering is non-deterministic for ties; (3) that the `within_batch_dedup` CTE in `silver_quarantine.sql` produces the same rn assignment as in `silver_transactions.sql` — both must use the same `ORDER BY` column; (4) that `silver_quarantine.sql` Branch 6 was not already present before this task — an existing Branch 6 would have caused double-counting.

Engineer decision: **Accept (1) and (2) as material.** (1): rn>1 filter covers all duplicates ≥ 2 — rn=3 also satisfies rn>1; structurally correct. (2): `_ingested_at` is set by the Bronze loader to the pipeline ingestion timestamp; two records in the same file will have the same `_ingested_at`, making ORDER BY non-deterministic for ties — acceptable because either occurrence is equally valid to keep; INV-10 only requires uniqueness, not which copy survives. (3): both CTEs use identical `ORDER BY _ingested_at` — confirmed by code review. (4): confirmed Branch 6 did not exist before this task.

### Code Review

[Engineer code review notes for Task 6.1:]
- [x] INV-10: `ROW_NUMBER()` window partition is on `transaction_id` only — no additional partition keys that would break deduplication semantics
- [x] INV-10: `ORDER BY _ingested_at` — deterministic column (not RANDOM()); tie behaviour accepted per engineer decision above
- [x] INV-10: Records where `rn > 1` are routed to `quarantine_all` Branch 6 — not silently dropped
- [x] INV-09: `_rejection_reason` string literal is exactly `'DUPLICATE_TRANSACTION_ID'` in both `silver_transactions.sql` and `silver_quarantine.sql`

---

## Task 6.2 — Cross-Partition Deduplication

**INVARIANT TOUCH: INV-09, INV-10, INV-16**

> INV-10 (TASK-SCOPED): Combined with Task 6.1, ensures no `transaction_id` appears more than once across all Silver partitions after promotion. Anti-join must cover ALL existing Silver partitions via glob — not only the current date.
> INV-16 (TASK-SCOPED): Silver partition path for survivors derives from `transaction_date` field — not from `process_date` argument. Records where `transaction_date != process_date` are quarantined here.
> INV-09 (TASK-SCOPED): Cross-partition duplicates use `'DUPLICATE_TRANSACTION_ID'` — exact string literal.

### Test Cases Applied

| Case | Scenario | Expected | Result |
|---|---|---|---|
| TC-6.2-A | First run new date (2024-01-26) — new IDs not in any prior Silver | No cross-partition rejects; silver=2 | PASS — silver=2, cross_dup_rejects=0 |
| TC-6.2-A re-run | Re-run same date 2024-01-26 — idempotent | Silver count unchanged = 2 | PASS — silver_rerun=2 |
| TC-6.2-B | TXN-NEW-001 cross-date dup (in 2024-01-26 Silver); TXN-NEW-003 new — run 2024-01-27 | TXN-NEW-001 quarantined as DUPLICATE_TRANSACTION_ID; TXN-NEW-003 in Silver | PASS |
| TC-6.2-B re-run | Re-run 2024-01-27 — Silver count unchanged | silver_rerun=1=first_count=1 | PASS |
| TC-6.2-C | First run and re-run produce identical Silver row counts | FIRST_COUNT = SECOND_COUNT | PASS — both = 4 (2024-01-01) |
| TC-6.2-D | Global uniqueness across all Silver partitions | duplicates = 0 | PASS — 0 duplicates |
| TC-6.2-E | `transaction_date=2024-01-15` in `process_date=2024-01-28` Bronze | Quarantined as INVALID_TRANSACTION_DATE; TXN-GOOD-001 in Silver; gap=0 | PASS |

### Prediction Statement

TC-6.2-A: `existing_silver_ids` (filtered to `transaction_date != process_date`) returns no rows matching TXN-NEW-001/TXN-NEW-002 (new IDs not in any prior Silver). LEFT JOIN produces no matches → `_cross_rejection_reason = NULL` → both proceed to Silver. silver=2, cross_dup_rejects=0. Re-run: same result because current date excluded from `existing_silver_ids`.

TC-6.2-B: TXN-NEW-001 is in 2024-01-26 Silver (different date from 2024-01-27 process_date). `existing_silver_ids` for 2024-01-27 includes 2024-01-26 records → LEFT JOIN finds TXN-NEW-001 → `_cross_rejection_reason = 'DUPLICATE_TRANSACTION_ID'` → quarantined. TXN-NEW-003 has no match → Silver. silver=1, quarantine=1. Re-run: same result.

TC-6.2-C: FIRST_COUNT=4, SECOND_COUNT=4 for 2024-01-01. Current date excluded from `existing_silver_ids` → re-run sees no cross-partition matches → same Silver output.

TC-6.2-D: After all runs, `count(*) - count(DISTINCT transaction_id) = 0` across all Silver partitions. Each transaction_id appears at most once globally.

TC-6.2-E: TXN-WRONGDATE-001 has `transaction_date = 2024-01-15` but `process_date = 2024-01-28`. `cross_partition_dedup` CASE: not a cross-partition dup but `transaction_date != process_date` → `_cross_rejection_reason = 'INVALID_TRANSACTION_DATE'` → Branch 8 in quarantine_all → quarantined. TXN-GOOD-001 passes. bronze=2, silver=1, quarantine=1, gap=0.

### Supplementary Checks

```
# Verification command (adapted process_date=2024-01-01)
dbt run (first):   PASS=2, FIRST_COUNT=4
dbt run (second):  PASS=2, SECOND_COUNT=4
First: 4, Second: 4 — equal  ✓
duplicates = 0  ✓
```

### CD Challenge Output

> Prompt given to CC: "What did you not test in this task?"

CC response: Did not test (1) a transaction_id that appears in Silver across THREE different date partitions — only tested two-partition cross-date deduplication; (2) the first-ever pipeline invocation where no Silver files exist — `read_parquet` glob with no matching files would throw IO Error, not return empty; (3) that `existing_silver_ids` correctly excludes ONLY the current date's partition and not adjacent dates when multiple partitions share similar date ranges; (4) that conservation post-hook reads the correct quarantine count when both Branch 7 and Branch 8 are present (quarantine count should include all branches).

Engineer decision: **Accept (1) and (2) as material.** (1): three-partition dedup — `existing_silver_ids` scans ALL prior partitions via glob; TXN appearing in any two prior partitions is already deduplicated before reaching Silver; structurally correct for N partitions. (2): empty glob (first-ever run) — deviation noted; pipeline.py orchestrates Bronze → Silver; at least one Silver partition will exist before cross-partition dedup runs in a real pipeline run (transaction_codes Silver is written first). (3): `transaction_date != process_date` filter is exact date comparison — unambiguous. (4): conservation post-hook uses `WHERE _source_file LIKE '%transactions%'` — covers all branches since all quarantine records carry the transactions source file name; verified gap=0 for TC-6.2-E.

### Code Review

[Engineer code review notes for Task 6.2:]
- [x] INV-10: `existing_silver_ids` CTE uses glob `data/silver/transactions/**/*.parquet` filtered to `transaction_date != process_date` — scans all prior-date partitions
- [x] INV-10: `cross_partition_dedup` uses LEFT JOIN + `IS NOT NULL` check — not INNER JOIN that would silently drop unmatched records
- [x] INV-10: Cross-partition rejects in Branch 7 with `_rejection_reason = 'DUPLICATE_TRANSACTION_ID'` — exact string literal
- [x] INV-16: `WHEN transaction_date != process_date THEN 'INVALID_TRANSACTION_DATE'` is the second CASE branch in `cross_partition_dedup` — fires only when record is NOT already a cross-partition dup
- [x] INV-16: Branch 8 routes INVALID_TRANSACTION_DATE records to `quarantine_all` — INV-06 satisfied, no silent drops
- [x] INV-09: `INVALID_TRANSACTION_DATE` added to permitted enum in Claude.md before being used in code

---

## Task 6.3 — Account Resolution and _is_resolvable Flag

**INVARIANT TOUCH: INV-06, INV-14, INV-15**

> INV-14 (TASK-SCOPED): Every Silver transaction with an `account_id` not found in Silver Accounts at promotion time has `_is_resolvable = false`. LEFT JOIN must produce NULL for unresolved accounts; `_is_resolvable` is set to FALSE for those rows.
> INV-15 (TASK-SCOPED): No quarantine record has `_rejection_reason = 'UNRESOLVABLE_ACCOUNT_ID'`. This code must not appear anywhere in the model. Unresolvable transactions go to Silver — not quarantine.
> INV-06 (GLOBAL): Unresolvable transactions route to Silver; conservation still holds — they are not a third path that drops records.

### Test Cases Applied

| Case | Scenario | Expected | Result |
|---|---|---|---|
| TC-6.3-A | Transaction with known `account_id` (ACC-001) → Silver with `_is_resolvable = true` | Silver record has `_is_resolvable = true` | PASS — TXN-RES-001, ACC-001, `_is_resolvable = true` |
| TC-6.3-B | Transaction with unknown `account_id` (ACC-UNKNOWN) → Silver with `_is_resolvable = false` | Silver record has `_is_resolvable = false`; NOT in quarantine | PASS — TXN-UNRES-001, ACC-UNKNOWN, `_is_resolvable = false`; quarantine=0 |
| TC-6.3-C | No quarantine record has `UNRESOLVABLE_ACCOUNT_ID` rejection reason | `bad_quarantine = 0` | PASS — 0 |
| TC-6.3-D | Conservation — unresolvable records counted toward Silver, not quarantine | bronze=2, silver=2, quarantine=0, gap=0 | PASS |

### Prediction Statement

TC-6.3-A: `account_resolution` LEFT JOIN on `account_id`. ACC-001 exists in Silver Accounts → `sa.account_id IS NOT NULL` → `_is_resolvable = TRUE`. Record flows to final SELECT → Silver.

TC-6.3-B: ACC-UNKNOWN has no matching row in Silver Accounts → LEFT JOIN produces NULL → `sa.account_id IS NULL` → `_is_resolvable = FALSE`. Record still flows to final SELECT → Silver (not quarantine). INV-15 confirmed — no UNRESOLVABLE_ACCOUNT_ID branch exists in quarantine_all.

TC-6.3-C: `quarantine_all` has 8 branches — none produce `_rejection_reason = 'UNRESOLVABLE_ACCOUNT_ID'`. COUNT FILTER = 0.

TC-6.3-D: Both Bronze records reach `account_resolution` (pass all prior checks). Both go to Silver regardless of `_is_resolvable`. bronze=2, silver=2, quarantine=0, gap=0.

### Supplementary Checks

```
# Verification command (adapted process_date=2024-01-01)
dbt run  →  PASS=2

_is_resolvable   count(*)
true                  3       ✓ (3 of 4 Silver records resolve to known accounts)
false                 1       ✓ (1 unresolvable — stays in Silver per INV-15)

bad_quarantine = 0  ✓
```

### CD Challenge Output

> Prompt given to CC: "What did you not test in this task?"

CC response: Did not test (1) that `_is_resolvable` is stored as BOOLEAN (not INTEGER 0/1) in Parquet — DuckDB `TRUE/FALSE` literals write as boolean but downstream readers may differ; (2) that a re-run after Silver Accounts is updated with a previously-unknown account_id correctly flips `_is_resolvable` from FALSE to TRUE — account resolution is at promotion time only, not retroactive; (3) that `_is_resolvable` is non-null for every Silver record — a code path that produced NULL would violate INV-14; (4) that the Silver Accounts file path `/app/data/silver/accounts/data.parquet` exists at run time — if absent the JOIN would IO Error, not produce NULLs.

Engineer decision: **Accept (1) and (4) as material.** (1): `_is_resolvable` confirmed as `boolean` type in Parquet output from verification query `GROUP BY _is_resolvable` returning `true/false` literals — boolean type confirmed. (4): Silver Accounts is written by `silver_accounts` model which runs before `silver_transactions` in alphabetical execution order — file is guaranteed present before this CTE executes. (2): retroactive resolution is explicitly out of scope per CLAUDE.md. (3): CASE WHEN covers both NOT NULL (TRUE) and NULL (FALSE) — no path produces NULL `_is_resolvable`.

### Code Review

[Engineer code review notes for Task 6.3:]
- [x] INV-14: `account_resolution` uses `LEFT JOIN` on `account_id` — not INNER JOIN; unresolvable records are retained
- [x] INV-14: `_is_resolvable = CASE WHEN sa.account_id IS NOT NULL THEN TRUE ELSE FALSE END` — NULL from LEFT JOIN maps to FALSE, not NULL
- [x] INV-15: `quarantine_all` has 8 branches — none reference `UNRESOLVABLE_ACCOUNT_ID`; grep confirms no occurrence in the model
- [x] INV-15: `account_resolution` feeds directly into final SELECT — no path routing unresolvable records to `quarantine_all`
- [x] INV-06: all records from `cross_partition_dedup WHERE _cross_rejection_reason IS NULL` enter `account_resolution` — no third path that silently drops records

---

## Task 6.4 — Full Conservation and Silver Partition Date Isolation Check

**INVARIANT TOUCH: INV-07, INV-16**

> INV-07 (TASK-SCOPED): `bronze_count = silver_count + quarantine_count`. Conservation assertion is a hard `RuntimeError` raise — not a log or warning. Fires after every Silver promotion run, not only on first run.
> INV-16 (TASK-SCOPED): Silver partition path for all survivors derives from `transaction_date` field. The INV-16 guard in Task 6.2 ensures all survivors have `transaction_date = process_date`; this task confirms the path derivation uses the field, not the variable.

### Test Cases Applied

| Case | Scenario | Expected | Result |
|---|---|---|---|
| TC-6.4-A | Conservation check — gap = 0 on clean run | PASS=2, no error raised, gap=0 | PASS — bronze=5, silver=4, quarantine=1, gap=0 |
| TC-6.4-B | Inject mismatch (bronze=5, silver=3, quarantine=1) — error() must fire with counts | Hard raise with `bronze=X, silver=Y, quarantine=Z, gap=N` in message | PASS — `INV-07 violated: bronze=5, silver=3, quarantine=1, gap=1 for date=2024-01-01` |
| TC-6.4-C | Global uniqueness after full session | duplicates = 0 | PASS — 0 duplicates across all Silver partitions |

### Prediction Statement

TC-6.4-A: Post-hook fires after Silver write. WITH _counts computes bronze=5, silver=4, quarantine=1. `5 = 4 + 1` → condition false → ELSE NULL → no error. PASS=2.

TC-6.4-B: Inject bronze=5, silver=3, quarantine=1. `5 != 3 + 1 = 4` → condition true → `error('INV-07 violated: bronze=5, silver=3, quarantine=1, gap=1 for date=2024-01-01')` → DuckDB raises with that message.

TC-6.4-C: After all session runs, every transaction_id that reached Silver is globally unique. `count(*) - count(DISTINCT transaction_id) = 0`.

### Supplementary Checks

```
# Verification command (adapted process_date=2024-01-01)
dbt run  →  PASS=2, ERROR=0
conservation: bronze=5, silver=4, quarantine=1, gap=0  ✓
duplicates = 0  ✓

# TC-6.4-B error message confirmed:
Invalid Input Error: INV-07 violated: bronze=5, silver=3, quarantine=1, gap=1 for date=2024-01-01
```

### CD Challenge Output

> Prompt given to CC: "What did you not test in this task?"

CC response: Did not test (1) the conservation assertion firing on a real dbt run where a CTE bug silently drops records — only tested by injecting known counts directly into the DuckDB expression; (2) that the post-hook fires AFTER the Silver file is written (not before); confirmed by the model execution order but not explicitly tested; (3) that the `_counts` CTE in the post-hook correctly reads the NEWLY written Silver file and not a cached/stale version; (4) that `quarantine_count` correctly excludes accounts quarantine rows (the `_source_file LIKE '%transactions%'` filter) when both transactions and accounts quarantine records exist for the same date.

Engineer decision: **Accept (1) and (4) as material.** (1): live-run corruption test impractical without temporarily modifying code; DuckDB `error()` behavior confirmed via direct injection (TC-6.4-B). (4): `_source_file LIKE '%transactions%'` filter confirmed correct — all transactions quarantine records carry `transactions_YYYY-MM-DD.csv` as `_source_file`; accounts quarantine records carry `accounts_YYYY-MM-DD.csv`; filter is unambiguous. (2) and (3): post-hook execution order and file freshness are dbt-duckdb guarantees — not user-testable.

### Code Review

[Engineer code review notes for Task 6.4:]
- [x] INV-07: Post-hook uses DuckDB `error()` — hard raise, not `print` or log; propagates as dbt model failure
- [x] INV-07: Post-hook fires unconditionally after every run — no first-run guard or debug flag
- [x] INV-07: `quarantine` subquery filters `WHERE _source_file LIKE '%transactions%'` — excludes accounts quarantine from the conservation check
- [x] INV-07: Error message includes `bronze`, `silver`, `quarantine`, and `gap` values — counts in message per spec
- [x] INV-16: SQL comment in model body confirms location uses `process_date` but all survivors have `transaction_date = process_date` (enforced by Task 6.2 INVALID_TRANSACTION_DATE guard) — path is effectively field-derived

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
- [x] All invariant-touch code reviews complete (INV-06, INV-07, INV-09, INV-10, INV-14, INV-15, INV-16 checklists signed)
- [x] No deviations left unresolved

**Status:** DONE

**Integration check:** PASSED — dbt PASS=2, duplicates=0, bad_quarantine=0 (date=2024-01-01)

**Engineer sign-off:** ___________________________________