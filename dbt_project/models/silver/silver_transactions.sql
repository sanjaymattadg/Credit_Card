-- silver_transactions.sql
-- OQ-1/OQ-2 strategy: external materialization with per-date partition files.
-- dbt-duckdb 1.7.x: incremental+location does not write external parquet.
-- Cross-partition dedup: DUPLICATE_TRANSACTION_ID handled in silver_quarantine,
-- which checks silver partitions for OTHER dates (filename-filtered).
-- Cross-partition dedup verified: Yes — per-date external partitions + quarantine detection.
--
-- Promotes Bronze transactions to Silver after quality checks.
-- Sign assignment exclusively via transaction_codes.debit_credit_indicator (I-10).
-- _is_resolvable flag set by LEFT JOIN to Silver Accounts at promotion time (I-13).
-- Records failing quality checks are written to silver_quarantine (separate model).
-- UNRESOLVABLE_ACCOUNT_ID is a flag only — record enters Silver with _is_resolvable=false.

{{
  config(
    materialized='external',
    location='/app/data/silver/transactions/date={{ var("processing_date") }}/data.parquet',
    format='parquet'
  )
}}

WITH bronze_source AS (
    SELECT *
    FROM read_parquet(
        '/app/data/bronze/transactions/date={{ var("processing_date") }}/*.parquet'
    )
),

quality_check AS (
    SELECT
        *,
        CASE
            WHEN transaction_id IS NULL OR transaction_id = ''
              OR account_id IS NULL OR account_id = ''
              OR transaction_date IS NULL
              OR amount IS NULL
              OR transaction_code IS NULL OR transaction_code = ''
              OR channel IS NULL OR channel = ''
            THEN 'NULL_REQUIRED_FIELD'
            WHEN amount <= 0
            THEN 'INVALID_AMOUNT'
            WHEN channel NOT IN ('ONLINE', 'IN_STORE')
            THEN 'INVALID_CHANNEL'
            WHEN NOT EXISTS (
                SELECT 1 FROM read_parquet('/app/data/silver/transaction_codes/data.parquet') tc
                WHERE tc.transaction_code = bronze_source.transaction_code
            )
            THEN 'INVALID_TRANSACTION_CODE'
            ELSE NULL
        END AS _initial_rejection
    FROM bronze_source
),

passed_initial AS (
    SELECT * FROM quality_check WHERE _initial_rejection IS NULL
),

with_sign AS (
    SELECT
        p.*,
        CASE
            WHEN tc.debit_credit_indicator = 'DR' THEN p.amount
            ELSE -1 * p.amount
        END AS _signed_amount,
        tc.transaction_type,
        tc.debit_credit_indicator
    FROM passed_initial p
    JOIN read_parquet('/app/data/silver/transaction_codes/data.parquet') tc
      ON p.transaction_code = tc.transaction_code
),

with_resolvable AS (
    SELECT
        w.*,
        CASE WHEN a.account_id IS NOT NULL THEN TRUE ELSE FALSE END AS _is_resolvable
    FROM with_sign w
    LEFT JOIN read_parquet('/app/data/silver/accounts/data.parquet') a
      ON w.account_id = a.account_id
)

SELECT
    transaction_id,
    account_id,
    transaction_date,
    amount,
    transaction_code,
    merchant_name,
    channel,
    _source_file,
    _ingested_at                              AS _bronze_ingested_at,
    '{{ var("run_id", "no_run_id") }}'        AS _pipeline_run_id,
    CURRENT_TIMESTAMP                         AS _promoted_at,
    _is_resolvable,
    _signed_amount
FROM with_resolvable
