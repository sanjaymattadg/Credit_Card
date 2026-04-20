# ARCHITECTURE.md
## Credit Card Financial Transactions Lake
**Version:** v1.0 · **Status:** Draft — Phase 1 Decide · **Classification:** Training Demo System

---

## Changelog

| Version | Date | Author | Change |
|---|---|---|---|
| v1.0 | 2026-04-13 | Engineer | Greenfield — Initial architecture document produced at Phase 1 completion |

---

## 1. Problem Framing

### What the System Solves

A financial services client processes credit card transactions across multiple channels daily. Analysts and risk teams currently access raw CSV extract files directly, with no controlled processing layer between the source system and analysis. This creates three failure modes:

- **Inconsistent results** when analysts work from different versions of the same extract file, breaking trust in reported numbers.
- **No quality control layer**, meaning malformed, null, or duplicate records flow directly into analysis without detection.
- **No audit trail**, making it impossible to trace how a raw extract became a final aggregated figure.

The primary pain is inconsistent outputs. Analysts cannot agree on numbers derived from the same underlying data. The solution is a structured data lake implementing the Medallion architecture (Bronze → Silver → Gold) that gives all analysts a single, consistent, auditable source of truth.

Success means: every analyst queries the same Gold layer, aggregations are reproducible, every output is traceable back to its source file, and the pipeline can be re-run on the same input without producing different results.

### What the System Does Not Solve

This system is strictly an ingestion and transformation pipeline. It explicitly does not:

- Compute risk scores or creditworthiness assessments.
- Make or support credit decisions.
- Modify any source system records — source CSV files are read-only inputs.
- Provide a serving API layer — Gold outputs are queried directly via DuckDB CLI.
- Implement SCD Type 2 for account history — Silver accounts stores the latest record only.
- Support backfill of unresolvable account references — this requires a dedicated pipeline out of scope here.
- Handle schema evolution, streaming ingestion, or production monitoring infrastructure.

---

## 2. Key Design Decisions

### Decision 1 — Watermark as the Authoritative Pipeline State Indicator

**What was decided:** The pipeline control table watermark is the single authoritative indicator of completed processing state. The incremental pipeline reads the watermark, derives the next date as watermark + 1, and advances the watermark only after Bronze, Silver, and Gold all complete successfully for that date. The run log is audit infrastructure only — it is never consulted to decide what to process next.

**Rationale:** Using a single authoritative state indicator eliminates the risk of divergence between two competing sources of truth. If both the watermark and the run log were used as control mechanisms, a failure scenario could produce a SUCCESS entry in the run log without a corresponding watermark advance, leaving the two in conflict with no clear resolution rule.

**Alternative considered:** Run log as the primary gating mechanism — the incremental pipeline checks the run log for watermark+1 before processing.

**Why rejected:** This creates a divergence risk. The run log is append-only and records individual model executions. If a SUCCESS entry exists for a model but the watermark did not advance (due to a later failure in the same run), the run log would indicate the date is processed while the watermark says it is not. There is no safe resolution rule for this conflict without additional complexity.

---

### Decision 2 — Separate Entry Points for Historical and Incremental Pipelines

**What was decided:** The historical load pipeline and the incremental pipeline are implemented as two separate entry points in pipeline.py. Both call the same underlying dbt models, but their orchestration logic, state management, and invocation interfaces are distinct. The historical pipeline accepts start_date and end_date parameters and iterates over the date range. The incremental pipeline reads the watermark and processes a single date.

**Rationale:** The two pipelines have meaningfully different concerns. The historical pipeline must handle date range iteration, transaction codes initialisation, per-date idempotency skip checks via the run log, and watermark initialisation at end_date. The incremental pipeline must handle watermark-controlled single-date processing and no-op behaviour when no file is available. Combining these into a single entry point would require conditional logic that obscures the intent of each pipeline and makes both harder to test and reason about independently.

**Alternative considered:** A single entry point in pipeline.py with parameters controlling behaviour — a mode flag or date range vs. single date argument.

**Why rejected:** A single parameterised entry point requires conditional branching inside the orchestration logic to handle the different state management concerns. This makes each pipeline's behaviour less explicit and harder to verify independently. It also couples the two pipelines such that a change to incremental behaviour requires reasoning about its effect on historical behaviour and vice versa.

---

### Decision 3 — Run Log-Driven Idempotency for Historical Pipeline with Per-Date Sequential Checks

**What was decided:** The historical pipeline checks the run log per date during iteration, not upfront in batch. For each date in the range, it queries the run log for an existing SUCCESS entry before processing. If a SUCCESS entry exists and the Silver transaction_codes file is valid (for the transaction codes entity), the date or entity is skipped. This is the idempotency mechanism for the historical pipeline only — the incremental pipeline does not use the run log as a gate.

**Rationale:** Per-date sequential checks allow the historical pipeline to resume correctly from the point of failure without reprocessing already-completed dates. An upfront batch check would require reading the entire run log before starting, and any state change between the check and execution could produce a stale decision. Sequential per-date checks keep the decision point as close as possible to the execution point.

**Alternative considered:** Upfront batch check — read the run log for all dates in the range before starting, determine which dates to skip, then process the remainder.

**Why rejected:** An upfront check produces a static list of dates to process. If the pipeline fails during processing, the list is stale on re-run and would need to be regenerated. It also makes partial failure recovery more complex — the pipeline cannot simply resume from where it stopped without regenerating the skip list.

**Special case — transaction codes:** Transaction codes idempotency uses run log SUCCESS check plus Silver data state validation. If the run log shows SUCCESS but the Silver transaction_codes file is missing or has a row count of zero, the pipeline treats this as an inconsistency and reloads. The run log alone is not sufficient for this entity because its Silver output is a prerequisite for all transaction processing.

---

### Decision 4 — Two-Step Silver Deduplication (Within-Batch then Cross-Partition)

**What was decided:** Silver transaction deduplication is performed in two sequential steps. First, within-batch deduplication identifies duplicate transaction_ids within the incoming source file, keeps one record, and sends the rest to quarantine. Second, cross-partition deduplication checks the surviving records against all existing Silver transaction partitions and quarantines any that match. Both steps are required on every Silver promotion run.

**Rationale:** The brief requires global uniqueness of transaction_id across all Silver partitions. A cross-partition check alone would miss duplicates within the same source file when no prior partition exists for that date. The within-batch step must precede the cross-partition step to ensure that both intra-file and inter-partition duplicates are caught before any records are written to Silver.

**Alternative considered:** Cross-partition check only, relying on the source system to guarantee no intra-file duplicates.

**Why rejected:** The brief states that malformed records, nulls, and duplicates are preserved in Bronze exactly as received. This means the pipeline cannot assume source data is free of intra-file duplicates. Relying on source system guarantees would make Silver correctness dependent on an external assumption not stated in the brief.

**Performance trade-off:** The cross-partition scan is a full scan of all existing Silver transaction partitions on every promotion run. This is a consciously accepted trade-off given the brief's explicit global uniqueness requirement and DuckDB's capability to handle this efficiently on Parquet. This could be optimised in a future enhancement if dataset scale requires it.

---

### Decision 5 — Gold as a Fully Recomputed Overwrite Layer

**What was decided:** Gold aggregations are fully recomputed from Silver on every pipeline run and the output is overwritten. Gold is not append-only. The weekly_account_summary includes partial weeks, updated daily until the week is complete. Gold correctness is guaranteed relative to the Silver data available at the time of computation — not in an absolute sense, since transactions with _is_resolvable = false are excluded from all Gold aggregations.

**Rationale:** The weekly_account_summary aggregation depends on transaction data across multiple days within a week. An incremental Gold update that only appends new day's data would leave weekly aggregates stale or incorrect mid-week. Full recomputation guarantees that all aggregations are consistent with the current state of Silver on every run, without requiring complex incremental merge logic.

**Alternative considered:** Incremental Gold append — each pipeline run appends a new Gold record for the processed date only, without recomputing existing records.

**Why rejected:** Incremental append cannot correctly maintain the weekly_account_summary without tracking and updating existing week records. The complexity of determining which existing weekly records to update, and ensuring consistency when doing so, exceeds the cost of full recomputation given the expected dataset size and DuckDB's query performance.

**UNRESOLVABLE_ACCOUNT_ID implication:** Records with _is_resolvable = false enter Silver but are excluded from all Gold aggregations. Gold correctness is therefore conditional — it reflects only transactions with known account references at promotion time. This is a deliberate design decision, not a data quality failure. Resolution requires a backfill pipeline that is explicitly out of scope.

---

## 3. Challenge My Decisions

| Decision | Strongest Argument Against | Valid or Rejected | Reasoning |
|---|---|---|---|
| Watermark as authoritative state | The watermark is a single point of failure. If the control table file is corrupted or deleted, the pipeline has no state and cannot determine what has been processed. The run log, being append-only, is more resilient. | Rejected | A corrupted control table is a recoverable operational incident — the watermark can be reconstructed from the run log by finding the latest date with a full set of SUCCESS entries across all three layers. The run log remains available for this purpose. Adding the run log as a co-equal control mechanism introduces divergence risk that is harder to recover from. |
| Separate pipeline entry points | Two entry points mean two code paths to maintain. A bug fix applied to one must be consciously applied to the other. Over time the two paths risk drifting apart. | Valid — acknowledged risk | This is a real maintenance risk carried into Key Risks. The mitigation is that both entry points call the same dbt models — transformation logic is shared. Divergence risk is confined to orchestration logic in pipeline.py. Explicit testing of both entry points at Phase 8 is the enforcement mechanism. |
| Per-date sequential run log checks | Sequential checks are slower than an upfront batch check for large date ranges. For a historical load spanning months, the per-date overhead adds up. | Rejected | DuckDB querying a Parquet run log is fast. For the date ranges described in the brief, sequential overhead is negligible. Correctness and simplicity outweigh the performance concern at current scope. |
| Two-step deduplication | Within-batch deduplication adds complexity. If the source system guarantees no intra-file duplicates, this step is unnecessary overhead that adds a failure point without value. | Rejected | The brief explicitly states Bronze stores data exactly as received, including duplicates. The pipeline cannot make assumptions about source system guarantees the brief does not state. The within-batch step is lightweight and protects against a class of failure the brief acknowledges is possible. |
| Gold full recompute | Full recomputation grows more expensive as Silver accumulates. In a production system recomputing all weekly summaries on every incremental run becomes increasingly costly. | Valid — acknowledged trade-off | Accepted consciously for correctness and simplicity at current scope. DuckDB on Parquet handles this well for the exercise dataset. A production system with significant data volume would implement partition-aware incremental Gold updates. Documented in Future Enhancements. |

---

## 4. Key Risks

| Risk | Description | Mitigation |
|---|---|---|
| Pipeline entry point divergence | Historical and incremental entry points share dbt models but have separate orchestration logic. Behaviour may drift between the two paths if changes are not applied consistently. | Both entry points must be exercised in Phase 8 system sign-off. Idempotency verification covers both pipelines explicitly. |
| Control table file loss | If pipeline/control.parquet is deleted or corrupted, the incremental pipeline cannot determine the watermark and cannot run safely. | The watermark can be reconstructed from the run log by identifying the latest date with SUCCESS entries across all three layers. This procedure should be documented as an operational runbook. |
| Cross-partition deduplication performance | As Silver transaction partitions accumulate, the full cross-partition scan on every promotion run grows in cost. At significant data volumes this could become a bottleneck. | Accepted at current scope. DuckDB handles Parquet scans efficiently. Optimisation deferred to future enhancement if scale requires it. |
| Gold recomputation cost growth | Full Gold recomputation on every run becomes more expensive as Silver accumulates. Weekly summaries require scanning all Silver transactions to recompute all weeks. | Accepted at current scope. Partition-aware incremental Gold updates are a documented future enhancement. |
| Partial Bronze write on failure | If Bronze writing fails mid-write, a partial file may be left on disk. The existence check would incorrectly treat this as a valid partition on the next run. | Validity check requires row count greater than zero in addition to file existence. A zero-row partition is treated as invalid and triggers a re-write. |
| Unresolvable account references accumulate | Transactions with _is_resolvable = false are excluded from Gold permanently within this system. If a significant proportion reference unknown accounts, Gold aggregations become materially incomplete without any alert. | The _is_resolvable flag and quarantine counts are visible in Silver and the run log. Monitoring of unresolvable record counts is an operational concern out of scope for this exercise but should be addressed in production. |

---

## 5. Key Assumptions

| Assumption | Implication if False |
|---|---|
| Source CSV files are read-only and stable between runs. A file written for a given date does not change content after initial delivery. | Silver accounts idempotency, which is defined as input-relative, would not hold. Re-running the same date with different file content is a different input, not a re-run. |
| A missing transactions file for a given date is a genuine failure condition, not an expected no-data scenario. | The pipeline would need a mechanism to distinguish between expected no-data dates and genuinely missing files, adding complexity not currently designed for. |
| A missing accounts delta file for a given date means no account changes occurred that day — it is a no-change scenario, not an error. | If a missing accounts file actually indicates a delivery failure, the Silver accounts layer would silently proceed with stale data without any alert. |
| DuckDB reading Parquet is fast enough for cross-partition deduplication scans and full Gold recomputation at the dataset sizes described in the brief. | Performance bottlenecks would require redesign of the deduplication and Gold computation approaches before the system could be operated reliably. |
| Transaction codes are static for the duration of this exercise and do not change once loaded. | Any new or modified transaction codes arriving after historical initialisation would not be reflected in Silver, causing INVALID_TRANSACTION_CODE rejections for valid transactions. |
| The run log Parquet file supports concurrent reads during pipeline execution without locking issues. | Parallel execution of pipeline steps that read the run log could produce incorrect skip decisions or read errors. |
| pipeline.py has sufficient access to dbt execution results to populate run log fields accurately, including records_processed, records_written, records_rejected, and error_message. | The run log would contain incomplete or inaccurate execution metadata, undermining its value as an audit trail. |

---

## 6. Open Questions

The following questions are not blocking Phase 2 invariant definition but must be resolved before Phase 3 execution planning begins.

| # | Question | Phase Needed By | Notes |
|---|---|---|---|
| OQ-01 | How does pipeline.py extract records_processed, records_written, and records_rejected from dbt model execution results to populate the run log accurately? | Phase 3 | dbt-duckdb adapter may expose execution metadata via dbt artifacts (run_results.json). This needs verification against the fixed stack before task-level execution planning. |
| OQ-02 | What is the exact mechanism for writing run log entries atomically as Parquet append operations? DuckDB does not natively support appending to an existing Parquet file without rewriting it. | Phase 3 | The run log must be append-only per the brief. If DuckDB requires a full rewrite to add rows, the append-only guarantee must be implemented at the application layer in pipeline.py. |
| OQ-03 | If the historical pipeline is re-run and the control table already has a watermark, what is the exact implementation guard in pipeline.py? | Phase 3 | Agreed decision: watermark moves forward only and is never overwritten by a re-run. The exact guard logic needs to be specified before implementation. |
| OQ-04 | Does a no-op incremental run produce any observable output beyond leaving all layers unchanged and the watermark unadvanced? | Phase 3 | Agreed: no run log entries for a no-op. This needs to be explicit in the pipeline.py implementation spec so the behaviour is testable at Phase 8. |

---

## 7. Future Enhancements (Conscious Deferrals)

| Enhancement | Rationale for Deferral |
|---|---|
| SCD Type 2 for Silver Accounts | Full history of account attribute changes requires Type 2 slowly changing dimension logic. Deferred to keep scope manageable. Current design stores latest record only — historical reconstruction of account state is not possible. |
| Backfill pipeline for unresolvable account references | Correcting _is_resolvable = false records requires a dedicated pipeline with watermark guard logic to prevent incorrect future-date processing. Explicitly out of scope for this exercise. |
| Partition-aware incremental Gold computation | Full Gold recomputation on every run is accepted for current scale. A production system with significant data volume would benefit from computing only affected date and week partitions rather than all Gold on each run. |
| Cross-partition deduplication optimisation | Full cross-partition scan on every Silver promotion run is accepted for current scale. An optimised approach (e.g. a maintained bloom filter or transaction_id index) could reduce scan cost at larger data volumes. |
| Operational monitoring and alerting | Production deployment would require monitoring of unresolvable record accumulation, pipeline failure alerting, and watermark staleness detection. All out of scope for this exercise. |
| Schema evolution support | The CSV schema is fixed for this exercise. A production system would need a schema registry and evolution handling strategy. Deferred as explicitly out of scope. |

---

## 8. Data Model

### Source Entities (Bronze)

| Entity | Type | Frequency | Key | Description |
|---|---|---|---|---|
| transactions | Fact | Daily file per calendar day | transaction_id | All transactions processed on a given date. Append-only — never updated or deleted in source. Amount always positive — sign assigned in Silver. |
| accounts | Dimension (delta) | Daily delta — new or changed records only | account_id | Account attributes. Only changed records arrive daily. Absence of a file means no changes that day. |
| transaction_codes | Reference | Single static file loaded once | transaction_code | Lookup table mapping codes to type, description, debit/credit indicator, and balance impact flag. |

### Entity Relationships

- `transactions.account_id` → `accounts.account_id` (foreign key — soft enforcement in Silver via _is_resolvable flag)
- `transactions.transaction_code` → `transaction_codes.transaction_code` (foreign key — hard enforcement in Silver, records quarantined if code not found)

### Silver Layer Entities

| Entity | Storage | Key Behaviour |
|---|---|---|
| silver/transactions | Partitioned by date | Validated and signed records. Global transaction_id uniqueness enforced via two-step deduplication. _is_resolvable flag for unknown accounts. |
| silver/accounts | Single non-partitioned file | Latest record per account_id. Deterministic upsert on account_id. Input-relative idempotency. |
| silver/transaction_codes | Single non-partitioned file | Loaded once. Validated on re-run via run log + Silver file existence and row count check. |
| silver/quarantine | Partitioned by Bronze source date | Records rejected during Silver promotion. Preserved with rejection reason code. Never promoted without backfill. |

### Gold Layer Entities

| Entity | Storage | Granularity | Key Behaviour |
|---|---|---|---|
| gold/daily_summary | Single file, overwritten on each run | One record per calendar day | Counts and sums of resolvable transactions by type and channel. Excludes _is_resolvable = false records. |
| gold/weekly_account_summary | Single file, overwritten on each run | One record per account per ISO week (Mon–Sun) | Purchase counts, payment and fee sums, closing balance. Partial weeks included and updated daily. Only accounts with at least one resolvable transaction in the week included. |

### Pipeline Infrastructure Entities

| Entity | Storage | Purpose |
|---|---|---|
| pipeline/control.parquet | Single file | Watermark — last successfully processed date across all three layers. Owned and updated exclusively by pipeline.py. |
| pipeline/run_log.parquet | Append-only single file | One row per dbt model or Bronze loader per pipeline invocation. Written by pipeline.py. Used for audit and historical pipeline idempotency skip checks only. |

---

## 9. Out of Scope

The following are explicitly outside the boundary of this system:

- Risk computation, credit scoring, or any business decision logic.
- Modification of source system records or source CSV files.
- Backfill pipeline for resolving _is_resolvable = false records.
- SCD Type 2 account history — full history of attribute changes is not retained.
- Transaction code dimension changes during pipeline operation.
- Streaming or near-realtime ingestion — this is a batch pipeline only.
- A serving API layer — Gold is queried directly via DuckDB CLI.
- Schema evolution — the CSV schema is fixed for this exercise.
- Data encryption at rest or in transit.
- Production deployment, monitoring, or alerting infrastructure.
- Parallel or concurrent pipeline execution — the pipeline is designed for sequential single-run operation.
- Resolution of _is_resolvable = false records — requires backfill pipeline.

---

*ARCHITECTURE.md — v1.0 · Phase 1 Decide · Credit Card Financial Transactions Lake · PBVI Data Engineering Training Vehicle*