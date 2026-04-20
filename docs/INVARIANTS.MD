# INVARIANTS.md
## Credit Card Financial Transactions Lake
**Version:** v1.0 · **Status:** Draft — Phase 2 Complete · **Classification:** Training Demo System

---

## Changelog

| Version | Date | Author | Change |
|---|---|---|---|
| v1.0 | 2026-04-17 | Engineer | Greenfield — Initial invariant set produced at Phase 2 completion |

---

## Preamble

This document defines the system invariants for the Credit Card Financial Transactions Lake. Each invariant states a condition that must always be true. A violation of any invariant means the system is broken, regardless of what else works correctly.

**No security invariants are present in this set.** This system has no authentication, authorisation, or encryption boundary. That is a deliberate architectural constraint — the system processes pre-existing data locally with no external service calls, no user-facing access layer, and no credential handling. The absence of security invariants is intentional, not an oversight.

**Scope classification:**

- **GLOBAL** — applies to every task in the system, regardless of layer or feature area. Goes into Claude.md Section 2.
- **TASK-SCOPED** — applies only when a specific component, layer, or feature is touched. Embedded inline in the relevant task prompts in EXECUTION_PLAN.md.

---

## Bronze Layer Invariants

---

**INV-01: Bronze idempotency**
Running the Bronze loader for date D twice on identical source input produces an identical Bronze partition — same row count, no new records added on the second run.

- **Category:** operational
- **Scope:** TASK-SCOPED — Bronze loader tasks
- **Why this matters:** If re-running the Bronze loader duplicates records, every downstream count is wrong. A re-run after a partial failure — a routine operational event — would inflate Silver, Gold, and all analyst-facing aggregations with no visible signal that duplication occurred. An analyst querying total daily spend would see inflated figures with no indication the numbers are wrong.
- **Enforcement points:** Bronze loader — existence check before write; write is skipped if the partition already exists and has a row count greater than zero.

---

**INV-02: Bronze field fidelity**
All source fields in a Bronze record preserve exact source values. Data types are explicitly defined per the source schema specification in the requirements brief and consistently applied during ingestion. No implicit or lossy type conversions are permitted. Type mappings are defined in the source schema specification and applied consistently by the Bronze loader — no field in any Bronze partition has a type that diverges from the specification.

- **Category:** data correctness
- **Scope:** TASK-SCOPED — Bronze loader tasks
- **Why this matters:** If Bronze silently truncates a decimal amount or misrepresents a date, every downstream transformation operates on wrong data. The error would propagate to Silver _signed_amount values and Gold aggregates. Because Bronze is the source of record, the error is undetectable without going back to the original CSV — which defeats the purpose of the pipeline.
- **Enforcement points:** Bronze loader — record construction; explicit type cast mapping applied per entity schema; no implicit inference.

---

**INV-03: Bronze audit column completeness**
Every Bronze record has non-null values for _source_file, _ingested_at, and _pipeline_run_id.

- **Category:** operational
- **Scope:** GLOBAL
- **Why this matters:** The _pipeline_run_id is the connective tissue of the entire audit trail. A Bronze record with a null _pipeline_run_id cannot be traced to a pipeline run, cannot be linked to a run log entry, and breaks the audit chain that allows an analyst to verify where a Gold number came from. An analyst asked to explain a suspicious aggregate has no path back to the source record.
- **Enforcement points:** Bronze loader — record construction; audit columns are added by the loader before write, not derived from source data.

---

**INV-04: Bronze partition immutability**
Once written, a Bronze partition is never overwritten or deleted. No Bronze partition file is modified after initial write, regardless of pipeline re-runs.

- **Category:** data correctness
- **Scope:** TASK-SCOPED — Bronze loader tasks
- **Why this matters:** Bronze is the raw landing zone and the only layer where data can be compared against the original CSV source. If a partition is overwritten, there is no way to verify that what Bronze contains matches what the source system delivered. An overwrite could silently drop records, change values, or introduce records from a different run — all without any detection mechanism.
- **Enforcement points:** Bronze loader — write logic; loader checks for partition existence before write and aborts rather than overwrites.

---

**INV-05: Bronze partition source isolation**
A Bronze partition for date D contains only records from the corresponding source file for date D.

- **Category:** data correctness
- **Scope:** TASK-SCOPED — Bronze loader tasks
- **Why this matters:** If records from date D+1 land in the date D partition, Silver deduplication and partition-level conservation checks will produce incorrect results. A transaction dated 2024-01-16 appearing in the 2024-01-15 partition would be counted in the wrong day's aggregations and could be quarantined as a duplicate when the correct date is processed.
- **Enforcement points:** Bronze loader — file-to-partition assignment; partition path is derived from filename date, not from record content.

---

**INV-06: Bronze record routing completeness**
Every Bronze record, across all entities (transactions and accounts), is routed to exactly one outcome: Silver or Quarantine. This invariant applies to transactions and accounts entities. Transaction codes have no quarantine path — they are validated at load time as a prerequisite condition, not subject to per-record routing.

- **Category:** data correctness
- **Scope:** GLOBAL
- **Why this matters:** A record that is neither promoted nor quarantined is silently dropped. Silent drops are undetectable through normal use — an analyst would not know a transaction was missing from Gold unless they independently cross-checked Bronze row counts. A dropped payment record would understate a customer's payments and potentially misrepresent their balance position.
- **Enforcement points:** Silver promotion — routing logic; every record exiting the Bronze read must enter either the Silver write path or the quarantine write path with no third path possible.

---

**INV-07: Bronze conservation**
For each daily Bronze transactions partition for date D, the Bronze row count equals the count of Silver transaction records promoted from date D plus the count of quarantine records from date D. The same conservation rule applies to accounts: the Bronze accounts row count for date D equals the count of Silver Accounts upsert operations from date D plus the count of quarantine account records from date D.

- **Category:** data correctness
- **Scope:** TASK-SCOPED — Silver promotion tasks
- **Why this matters:** This is the primary runtime verification of INV-06. If Bronze count does not equal Silver plus Quarantine, at least one record was silently dropped. A single dropped transaction means Gold aggregates are understated — potentially by a large amount if the dropped record was a high-value transaction. The conservation check is the only mechanism that makes silent drops detectable.
- **Enforcement points:** Silver promotion — post-write row count reconciliation; verified by DuckDB count queries after each Silver promotion run.

---

## Quarantine Invariants

---

**INV-08: Quarantine traceability**
Every quarantine record has a non-null _source_file matching an existing Bronze partition. Its source field values correspond to a record in that Bronze partition, verifiable by deterministic matching of all source field values.

- **Category:** data correctness
- **Scope:** TASK-SCOPED — Silver promotion tasks
- **Why this matters:** Quarantine is the audit trail for data quality failures. If a quarantine record cannot be traced back to a Bronze record, there is no way to verify whether the rejection was correct or whether the record was manufactured during Silver promotion. A quarantine record with no traceable Bronze origin could indicate a bug in the promotion logic itself — which would be undetectable without this traceability requirement.
- **Enforcement points:** Silver promotion — quarantine write path; _source_file is carried forward from Bronze; source field values are written to quarantine without modification.

---

**INV-09: Quarantine rejection reason validity**
Every quarantined record has a non-null _rejection_reason from the predefined set: NULL_REQUIRED_FIELD, INVALID_AMOUNT, DUPLICATE_TRANSACTION_ID, INVALID_TRANSACTION_CODE, INVALID_CHANNEL, INVALID_ACCOUNT_STATUS. No rejection reason outside this set is used.

- **Category:** data correctness
- **Scope:** TASK-SCOPED — Silver promotion tasks
- **Why this matters:** Rejection reason codes are the mechanism by which data quality failures are categorised and acted upon. An unrecognised code cannot be aggregated into quality metrics, cannot be routed to a remediation workflow, and cannot be used to diagnose systematic data issues. A null rejection reason means the record is in quarantine with no explanation — it is effectively lost from an operational perspective.
- **Enforcement points:** Silver promotion — quarantine write path; rejection_reason is set from an enumerated constant, not a free-text field.

---

## Silver — Transactions Invariants

---

**INV-10: Transaction ID global uniqueness**
Each transaction_id appears at most once across all Silver transactions partitions.

- **Category:** data correctness
- **Scope:** TASK-SCOPED — Silver transactions promotion task
- **Why this matters:** A duplicate transaction_id in Silver means the same transaction is counted twice. A duplicated debit inflates total spend and outstanding balance. A duplicated payment understates the net effect of the payment. Gold aggregates computed from Silver with duplicates are materially wrong — and the error is invisible to analysts querying Gold, who have no way to know a transaction_id has been double-counted.
- **Enforcement points:** Silver promotion — two-step deduplication: (1) within-batch deduplication before write, (2) cross-partition deduplication against all existing Silver partitions before write. Both steps required on every promotion run.

---

**INV-11: Transaction code referential integrity**
Every Silver transaction references a valid transaction_code present in the Silver transaction_codes reference file.

- **Category:** data correctness
- **Scope:** TASK-SCOPED — Silver transactions promotion task
- **Why this matters:** The transaction_code is the basis for _signed_amount derivation via debit_credit_indicator. A Silver transaction with an invalid or missing transaction_code would have no valid sign assignment, making its _signed_amount incorrect or null. Gold aggregates that include such records would aggregate incorrect amounts. The transaction_code is also used to categorise transactions by type for Gold weekly summaries — an invalid code means the record cannot be classified.
- **Enforcement points:** Silver promotion — transaction_code join against Silver transaction_codes before write; records with no matching code are quarantined with rejection reason INVALID_TRANSACTION_CODE.

---

**INV-12: _signed_amount non-null**
Every Silver transaction has a non-null _signed_amount.

- **Category:** data correctness
- **Scope:** TASK-SCOPED — Silver transactions promotion task
- **Why this matters:** A null _signed_amount cannot be summed, averaged, or compared. Any Gold aggregate that includes a record with null _signed_amount will itself be null or incorrect depending on the aggregation function. A Gold daily_summary with a null total_signed_amount would be unqueryable for that date — the analyst would see a null where a number should be, with no explanation.
- **Enforcement points:** Silver promotion — _signed_amount derivation step; null check enforced after derivation before write.

---

**INV-13: _signed_amount sign correctness and completeness**
Every Silver transaction satisfies all of the following conditions:
- debit_credit_indicator is either 'DR' or 'CR' — no other value is present in Silver
- _signed_amount is non-null
- If debit_credit_indicator = 'DR' then _signed_amount > 0
- If debit_credit_indicator = 'CR' then _signed_amount < 0
- _signed_amount is never equal to 0

- **Category:** data correctness
- **Scope:** TASK-SCOPED — Silver transactions promotion task
- **Why this matters:** Sign assignment is the mechanism by which the pipeline reflects whether a transaction increases or decreases a balance. A reversed sign on a payment (CR treated as positive) would make the payment appear to increase the outstanding balance rather than decrease it. Gold weekly_account_summary closing balance and total_payments figures would be wrong in a way that is indistinguishable from correct data to an analyst who does not independently re-derive the sign logic. A _signed_amount of zero would indicate a zero-value transaction — which is invalid per the source quality rules and would distort averages.
- **Enforcement points:** Silver promotion — _signed_amount derivation step; sign applied from debit_credit_indicator after transaction_code join; post-derivation assertion checks all five conditions before write.

---

**INV-14: Unresolvable account flagging**
Every Silver transaction with an account_id not found in Silver Accounts at promotion time has _is_resolvable = false.

- **Category:** data correctness
- **Scope:** TASK-SCOPED — Silver transactions promotion task
- **Why this matters:** Transactions with unknown account references are excluded from Gold aggregations. If such a transaction is incorrectly marked _is_resolvable = true, it enters Gold with no valid account backing. The account_id in Gold weekly_account_summary would reference an account that does not exist in Silver Accounts — violating INV-25 and producing a Gold record that cannot be joined to account attributes.
- **Enforcement points:** Silver promotion — account_id resolution step; anti-join against Silver Accounts; _is_resolvable flag set before write.

---

**INV-15: Unresolvable account not quarantined**
No quarantine record has _rejection_reason = 'UNRESOLVABLE_ACCOUNT_ID'.

- **Category:** data correctness
- **Scope:** TASK-SCOPED — Silver transactions promotion task
- **Why this matters:** An unresolvable account reference is a timing condition — the account delta file may not have arrived yet. Quarantining such records would permanently exclude them from Silver without any possibility of correction, even after the account reference arrives. The correct handling is Silver entry with _is_resolvable = false, leaving the record available for a future backfill. Quarantining by mistake means the transaction is permanently lost from Silver and all downstream Gold aggregations.
- **Enforcement points:** Silver promotion — routing logic; UNRESOLVABLE_ACCOUNT_ID is not a valid quarantine reason code; records failing only on account_id resolution must enter Silver, not quarantine.

---

**INV-16: Silver partition date isolation**
A Silver transactions partition for date D contains only records with transaction_date = D.

- **Category:** data correctness
- **Scope:** TASK-SCOPED — Silver transactions promotion task
- **Why this matters:** If a record with transaction_date = 2024-01-16 lands in the 2024-01-15 Silver partition, the Bronze conservation check for 2024-01-15 will fail, and Gold daily_summary for 2024-01-15 will contain a transaction from a different date. The cross-partition deduplication check will also fail to detect duplicates correctly, because it uses partition date as part of its scan logic.
- **Enforcement points:** Silver promotion — partition write logic; partition path is derived from transaction_date field value, not from the Bronze source file date.

---

## Silver — Accounts Invariants

---

**INV-17: Silver Accounts uniqueness**
Silver Accounts contains exactly one record per account_id.

- **Category:** data correctness
- **Scope:** TASK-SCOPED — Silver Accounts promotion task
- **Why this matters:** Silver Accounts is the reference used to resolve _is_resolvable for all Silver transactions and to populate closing_balance in Gold weekly_account_summary. If an account_id appears more than once, the join used for _is_resolvable resolution is non-deterministic — the same account_id could resolve to different credit_limit or account_status values depending on which duplicate is used. Gold closing_balance figures would be unreliable.
- **Enforcement points:** Silver Accounts promotion — upsert logic; upsert key is account_id; existing records are replaced, not appended, when a newer delta arrives.

---

**INV-18: Silver Accounts promotion idempotency**
Running Silver Accounts promotion twice on identical input state — same Bronze accounts delta for date D and unchanged pre-existing Silver Accounts state — produces identical Silver Accounts output: same row count, same records, same field values.

- **Category:** operational
- **Scope:** TASK-SCOPED — Silver Accounts promotion task
- **Why this matters:** If re-running Silver Accounts promotion produces different output for the same input, re-running after a pipeline failure changes the Silver state. A changed Silver Accounts state invalidates all existing _is_resolvable flags in Silver transactions that were computed against the prior state. Gold aggregates computed from those transactions would be based on a different account resolution state than the one that was used when _is_resolvable was set.
- **Enforcement points:** Silver Accounts promotion — upsert logic; deterministic upsert on account_id with no timestamp-based tie-breaking that could differ across runs.

---

## Silver — Transaction Codes Invariant

---

**INV-33: Transaction codes prerequisite**
Silver transaction_codes must contain at least one record before any Silver transactions promotion run begins. A Silver transactions promotion run against an empty or absent transaction_codes reference is a pipeline error, not a data quality failure.

- **Category:** operational
- **Scope:** TASK-SCOPED — Silver transactions promotion task
- **Why this matters:** Every Silver transaction promotion depends on the transaction_codes reference for INVALID_TRANSACTION_CODE rejection and _signed_amount sign derivation via debit_credit_indicator. If transaction_codes is absent or empty, every transaction would fail the INVALID_TRANSACTION_CODE check and be quarantined. The entire day's transaction data would land in quarantine — not because of data quality failures, but because of a pipeline sequencing error. The quarantine records would all carry the same rejection code with no indication that the root cause was a missing reference file.
- **Enforcement points:** Silver transactions promotion — pre-condition check before any record processing begins; promotion aborts with a pipeline error if Silver transaction_codes is absent or has row count of zero.

---

## Gold Layer Invariants

---

**INV-19: Gold source constraint**
Gold is computed only from Silver data. No Gold dbt model reads from Bronze directly.

- **Category:** data correctness
- **Scope:** TASK-SCOPED — Gold dbt model tasks
- **Why this matters:** Bronze contains unvalidated data including malformed records, nulls, and duplicates that Silver promotion deliberately excludes. If Gold reads from Bronze, it bypasses all quality controls and could aggregate invalid amounts, duplicate transactions, or records with invalid transaction codes. The entire purpose of the Silver layer is to be the clean, validated source for Gold — bypassing it defeats the architecture.
- **Enforcement points:** Gold dbt models — source references; enforced by code review of all Gold model source() and ref() calls; no Bronze path is permitted.

---

**INV-20: Gold determinism**
Gold outputs are deterministic given the same Silver input. Running Gold computation twice against an identical Silver state produces identical Gold output.

- **Category:** data correctness
- **Scope:** TASK-SCOPED — Phase 8 sign-off
- **Why this matters:** Non-deterministic Gold output means two pipeline runs on the same data produce different analyst-facing numbers. An analyst who reruns a query would get different results without any change to underlying data. This would destroy trust in the pipeline as an authoritative source and make Gold numbers unreproducible for regulatory or audit purposes.
- **Enforcement points:** Gold dbt models — aggregation logic; verified at Phase 8 by running Gold computation twice against a fixed Silver state and confirming identical output via file diff.

---

**INV-21: Gold daily_summary granularity**
Gold daily_summary contains exactly one row per transaction_date present in Silver transactions where _is_resolvable = true.

- **Category:** data correctness
- **Scope:** TASK-SCOPED — Gold daily_summary dbt model task
- **Why this matters:** Multiple rows for the same transaction_date would cause double-counting when analysts query Gold daily_summary totals. A missing row for a date means that date's transaction activity is invisible to analysts. Both failures are indistinguishable from correct data without independently querying Silver — which is exactly the inconsistency the pipeline is designed to prevent.
- **Enforcement points:** Gold daily_summary dbt model — grouping logic; transaction_date is the group-by key with no secondary grouping that could produce multiple rows per date.

---

**INV-22: Gold aggregate correctness**
Gold aggregates match corresponding aggregates computed directly from Silver, verified through explicitly defined DuckDB query-based checks as specified in the requirements brief Section 10.3. Specific checks include: total_signed_amount per day, total_purchases count per account per week, and total_payments sum per account per week.

- **Category:** data correctness
- **Scope:** TASK-SCOPED — Phase 8 sign-off
- **Why this matters:** A Gold aggregate that does not match its Silver source is a fabricated number. Analysts making decisions based on incorrect totals — for example, a risk team reviewing total weekly fees per account — would be acting on wrong data. Because Gold is the sole analyst-facing layer, there is no other source to catch the discrepancy. The error would propagate silently into all downstream analysis.
- **Enforcement points:** Gold dbt models — aggregation logic; Phase 8 verification commands per Section 10.3 of the requirements brief.

---

**INV-23: Gold Silver-backed existence**
Every Gold record is backed by corresponding Silver data at the same grain. For Gold daily_summary: each transaction_date present in Gold corresponds to at least one Silver transaction with that transaction_date and _is_resolvable = true. For Gold weekly_account_summary: each (account_id, week) combination present in Gold corresponds to at least one Silver transaction with _is_resolvable = true for that account and week, and the account_id exists in Silver Accounts.

- **Category:** data correctness
- **Scope:** TASK-SCOPED — Gold dbt model tasks
- **Why this matters:** A Gold record with no backing Silver data is a fabricated aggregate. A daily_summary row for a date with no Silver transactions would show counts and amounts derived from nothing. A weekly_account_summary row for an account with no Silver transactions that week would include a closing_balance from Silver Accounts but zero transaction activity — which may appear valid but is not correctly computed.
- **Enforcement points:** Gold dbt models — source filter logic; Gold is computed only from Silver transactions where _is_resolvable = true, joined to Silver Accounts for account validation.

---

**INV-24: Gold freshness**
Gold output files are fully overwritten on each pipeline run. No Gold record has a _computed_at timestamp from a previous pipeline run after the current run completes.

- **Category:** operational
- **Scope:** TASK-SCOPED — Gold dbt model tasks
- **Why this matters:** If Gold is not fully overwritten, stale records from prior runs persist alongside new records. An analyst querying Gold weekly_account_summary mid-week would see current-week partial data mixed with records from a prior run that were not overwritten. The _computed_at timestamp would reveal the inconsistency — but only to an analyst who checks it. The numbers themselves would be wrong with no visible warning.
- **Enforcement points:** Gold dbt models — write mode; Gold dbt models configured with full table replacement, not incremental append.

---

**INV-25: Gold account referential integrity**
Every account_id present in Gold weekly_account_summary exists in Silver Accounts.

- **Category:** data correctness
- **Scope:** TASK-SCOPED — Gold weekly_account_summary dbt model task
- **Why this matters:** An account_id in Gold with no corresponding Silver Accounts record means the closing_balance field cannot be populated from a valid source. A null or fabricated closing_balance in Gold weekly_account_summary would misrepresent the account's balance position for the week. An analyst using Gold to monitor account balances would have no indication that the closing_balance figure is unreliable.
- **Enforcement points:** Gold weekly_account_summary dbt model — join to Silver Accounts; only account_ids present in Silver Accounts are included in output.

---

**INV-34: Gold excludes unresolvable transactions**
No Gold aggregate includes any Silver transaction where _is_resolvable = false. Gold aggregate values are computed exclusively from Silver transactions where _is_resolvable = true.

- **Category:** data correctness
- **Scope:** TASK-SCOPED — Gold dbt model tasks
- **Why this matters:** A Silver transaction with _is_resolvable = false has an unknown account_id. Including it in Gold would either attribute its amount to an unknown account or produce a Gold record with a null account_id — neither of which is a valid aggregate. Including unresolvable transactions in daily_summary totals would also overstate total_transactions and total_signed_amount relative to what is actually attributable to known accounts.
- **Enforcement points:** Gold dbt models — source filter; WHERE _is_resolvable = true applied in all Gold model source queries against Silver transactions.

---

## Pipeline Control and Operational Invariants

---

**INV-26: Full pipeline idempotency**
Re-running the pipeline with identical input produces identical outputs across all layers — identical Bronze partitions, identical Silver records, identical quarantine records, and identical Gold output.

- **Category:** operational
- **Scope:** TASK-SCOPED — Phase 8 sign-off
- **Why this matters:** A pipeline that produces different results on re-run cannot be trusted after a failure. If a partial failure occurs and the pipeline is re-run as the recovery action, different outputs mean the recovery made things worse. An analyst who relies on Gold numbers from a re-run would have no way to know whether the re-run produced the same result as the original run or a different one.
- **Enforcement points:** Verified at Phase 8 by running the full pipeline twice on identical input and comparing all layer file contents, row counts, and Gold aggregate values.

---

**INV-27: No-op incremental behaviour**
An incremental pipeline run with no new data produces no changes in any layer. All layer file modification timestamps, row counts, and content are identical before and after a no-op run. No new run log entry is produced for a no-op run.

- **Category:** operational
- **Scope:** TASK-SCOPED — Incremental pipeline task
- **Why this matters:** If a no-op run modifies any layer, scheduled runs on days with no new files would corrupt existing data. A Gold overwrite triggered by a no-op run would produce a Gold file with updated _computed_at timestamps but unchanged data — creating confusion about when Gold was last meaningfully updated. Worse, if the no-op run produces slightly different output due to non-determinism, it would silently corrupt valid Gold records.
- **Enforcement points:** pipeline.py — no-op detection branch; watermark + 1 date check against available files; pipeline exits without executing any layer if no file is present for the next date.

---

**INV-28: Watermark advance condition**
The watermark in pipeline/control.parquet advances only after successful completion of Bronze, Silver, and Gold for a given date. A pipeline failure at any point before all three layers complete must not advance the watermark. The watermark for a given run_id advances only if every run log row for that run_id has status = SUCCESS.

- **Category:** operational
- **Scope:** TASK-SCOPED — pipeline.py watermark write task
- **Why this matters:** If the watermark advances after a partial run — for example, after Bronze and Silver complete but before Gold completes — the incremental pipeline will move to the next date on the next run. The date that failed to complete Gold is permanently skipped. Gold is never computed for that date. Analysts querying Gold for that date get no data, with no indication that a pipeline failure occurred.
- **Enforcement points:** pipeline.py — watermark write logic; watermark update is the final step of a pipeline run, executed only after explicit success confirmation from all three layer executions.

---

**INV-29: Watermark monotonicity**
The watermark (last_processed_date) in pipeline/control.parquet is monotonically non-decreasing across all pipeline runs. The watermark never decreases.

- **Category:** operational
- **Scope:** TASK-SCOPED — pipeline.py watermark write task
- **Why this matters:** A watermark decrease would cause the incremental pipeline to reprocess already-completed dates. Reprocessing a completed date would attempt to re-promote records already in Silver — triggering duplicate detection and quarantining valid records as DUPLICATE_TRANSACTION_ID. Gold would be recomputed from a Silver state that now has more quarantine records than on the original run, producing different aggregates for dates the analyst considers settled.
- **Enforcement points:** pipeline.py — watermark write logic; new watermark value is compared to current watermark before write; write is rejected if the new value is less than the current value.

---

**INV-30: Run log append-only**
The run log file (pipeline/run_log.parquet) is append-only. Existing run log rows are never modified or deleted. The run log row count is non-decreasing across pipeline runs.

- **Category:** operational
- **Scope:** TASK-SCOPED — pipeline.py run log write task
- **Why this matters:** The run log is the audit trail that allows an analyst to trace a Gold aggregate back through Silver to a specific Bronze load and pipeline run. If run log rows are modified after the fact, the audit trail is unreliable. A modified status field — for example, changing FAILED to SUCCESS — would make a failed run appear successful and conceal the failure from anyone reviewing the audit trail.
- **Enforcement points:** pipeline.py — run log write logic; run log is written by appending new rows only; no update or delete operations are performed on existing rows.

---

**INV-31: Run log completeness**
Each Bronze loader execution and each dbt model execution in a pipeline run produces exactly one run log row with non-null values for run_id, model_name, started_at, completed_at, and status. Models covered: bronze_transactions, bronze_accounts, bronze_transaction_codes, silver_transactions, silver_accounts, silver_transaction_codes, gold_daily_summary, gold_weekly_account_summary.

- **Category:** operational
- **Scope:** TASK-SCOPED — pipeline.py run log write task
- **Why this matters:** A missing run log entry for a model means that model's execution is invisible to audit. An analyst investigating an incorrect Gold aggregate cannot determine whether a specific Silver promotion succeeded, how many records were processed, or whether any were rejected. A partially populated run log is worse than no run log — it gives the appearance of an audit trail while concealing gaps.
- **Enforcement points:** pipeline.py — run log write logic; each model execution is wrapped in a logging block that writes a run log entry on both success and failure; no model execution path exits without producing a run log row.

---

**INV-32: Run log status accuracy**
Each run log entry accurately reflects the execution outcome of the model it records. A model execution is marked status = SUCCESS only if it completes without failure. A model execution that encounters any failure is marked status = FAILED. No run log entry has a null status field.

- **Category:** operational
- **Scope:** TASK-SCOPED — pipeline.py run log write task
- **Why this matters:** A run log entry marked SUCCESS for a model that actually failed gives the incremental pipeline incorrect state information. For the historical pipeline, a false SUCCESS entry in the run log would cause the date to be skipped on re-run — the pipeline would see SUCCESS and skip processing, leaving Silver and Gold incomplete for that date with no further opportunity to correct it within the historical pipeline's skip logic.
- **Enforcement points:** pipeline.py — run log write logic; status field is set from an enumerated constant (SUCCESS, FAILED, SKIPPED) derived from the execution result, not from a default value.

---

**INV-35: Pipeline run ID uniqueness**
Each pipeline invocation has a unique run_id shared across all model-level run log rows for that invocation. No two pipeline invocations share a run_id. A run_id appearing in any layer audit column must correspond to run log rows from exactly one pipeline run.

- **Category:** operational
- **Scope:** GLOBAL
- **Why this matters:** The run_id is the connective tissue that links every Bronze record, every Silver record, and every Gold record to the pipeline run that produced it. If two pipeline runs share a run_id, it is impossible to determine which run produced a given record. An analyst tracing a suspicious Gold aggregate to its pipeline run would find run log rows from two different executions under the same run_id — the audit trail would be ambiguous and unresolvable.
- **Enforcement points:** pipeline.py — run_id generation; a UUID or equivalent globally unique identifier is generated at the start of each pipeline invocation, before any model executes.

---

**INV-36: Historical pipeline watermark initialisation**
After a successful historical pipeline run covering dates start_date through end_date, the watermark (last_processed_date) in pipeline/control.parquet equals end_date.

- **Category:** operational
- **Scope:** TASK-SCOPED — Historical pipeline task
- **Why this matters:** If the historical pipeline completes successfully but sets the watermark incorrectly — for example, to start_date or to a date within the range — the incremental pipeline will attempt to reprocess already-completed dates. This triggers the duplicate detection logic and quarantines valid Silver records as DUPLICATE_TRANSACTION_ID. The incremental pipeline would also skip the gap between the incorrect watermark and end_date, believing those dates are still in the future.
- **Enforcement points:** pipeline.py — historical pipeline watermark write; watermark is set to end_date as the final step of a successful historical run, after all layers for all dates in the range have completed.

---

**INV-37: Bronze partition schema compliance**
Every Bronze partition contains exactly the expected source columns for its entity plus the three audit columns (_source_file, _ingested_at, _pipeline_run_id). No source column is absent. No column beyond the expected set is present. Expected columns are defined by the source schema specification in the requirements brief Section 2.

- **Category:** data correctness
- **Scope:** TASK-SCOPED — Bronze loader tasks
- **Why this matters:** A missing source column in Bronze means the column is permanently absent from the pipeline — Silver cannot derive it from a Bronze record that does not contain it. A missing amount column, for example, would make _signed_amount derivation impossible for the entire partition. An extra column not in the schema specification means the Bronze loader applied an undocumented transformation, breaking the no-transformation guarantee in INV-02.
- **Enforcement points:** Bronze loader — schema validation step after write; Parquet column names compared against the entity schema specification before the partition is considered valid.

---

## Invariant Index

| INV | Statement Summary | Category | Scope |
|---|---|---|---|
| INV-01 | Bronze idempotency | operational | TASK-SCOPED |
| INV-02 | Bronze field fidelity | data correctness | TASK-SCOPED |
| INV-03 | Bronze audit column completeness | operational | GLOBAL |
| INV-04 | Bronze partition immutability | data correctness | TASK-SCOPED |
| INV-05 | Bronze partition source isolation | data correctness | TASK-SCOPED |
| INV-06 | Bronze record routing completeness | data correctness | GLOBAL |
| INV-07 | Bronze conservation | data correctness | TASK-SCOPED |
| INV-08 | Quarantine traceability | data correctness | TASK-SCOPED |
| INV-09 | Quarantine rejection reason validity | data correctness | TASK-SCOPED |
| INV-10 | Transaction ID global uniqueness | data correctness | TASK-SCOPED |
| INV-11 | Transaction code referential integrity | data correctness | TASK-SCOPED |
| INV-12 | _signed_amount non-null | data correctness | TASK-SCOPED |
| INV-13 | _signed_amount sign correctness and completeness | data correctness | TASK-SCOPED |
| INV-14 | Unresolvable account flagging | data correctness | TASK-SCOPED |
| INV-15 | Unresolvable account not quarantined | data correctness | TASK-SCOPED |
| INV-16 | Silver partition date isolation | data correctness | TASK-SCOPED |
| INV-17 | Silver Accounts uniqueness | data correctness | TASK-SCOPED |
| INV-18 | Silver Accounts promotion idempotency | operational | TASK-SCOPED |
| INV-19 | Gold source constraint | data correctness | TASK-SCOPED |
| INV-20 | Gold determinism | data correctness | TASK-SCOPED |
| INV-21 | Gold daily_summary granularity | data correctness | TASK-SCOPED |
| INV-22 | Gold aggregate correctness | data correctness | TASK-SCOPED |
| INV-23 | Gold Silver-backed existence | data correctness | TASK-SCOPED |
| INV-24 | Gold freshness | operational | TASK-SCOPED |
| INV-25 | Gold account referential integrity | data correctness | TASK-SCOPED |
| INV-26 | Full pipeline idempotency | operational | TASK-SCOPED |
| INV-27 | No-op incremental behaviour | operational | TASK-SCOPED |
| INV-28 | Watermark advance condition | operational | TASK-SCOPED |
| INV-29 | Watermark monotonicity | operational | TASK-SCOPED |
| INV-30 | Run log append-only | operational | TASK-SCOPED |
| INV-31 | Run log completeness | operational | TASK-SCOPED |
| INV-32 | Run log status accuracy | operational | TASK-SCOPED |
| INV-33 | Transaction codes prerequisite | operational | TASK-SCOPED |
| INV-34 | Gold excludes unresolvable transactions | data correctness | TASK-SCOPED |
| INV-35 | Pipeline run ID uniqueness | operational | GLOBAL |
| INV-36 | Historical pipeline watermark initialisation | operational | TASK-SCOPED |
| INV-37 | Bronze partition schema compliance | data correctness | TASK-SCOPED |

---

**GLOBAL invariants (3):** INV-03, INV-06, INV-35 — these go into Claude.md Section 2.

**TASK-SCOPED invariants (34):** All others — embedded inline in the relevant task prompts in EXECUTION_PLAN.md.

**No security invariants** — deliberate absence; system has no authentication, authorisation, or encryption boundary.

---

*INVARIANTS.md — v1.0 · Phase 2 Complete · Credit Card Financial Transactions Lake · PBVI Data Engineering Training Vehicle*