-- silver_quarantine.sql
-- Captures Bronze transaction records that fail Silver quality checks.
-- Enforces I-01 conservation law (silver/transactions + silver/quarantine = bronze/transactions per date).
-- Enforces I-15: rejection reason values are exactly one of the permitted codes.
-- DUPLICATE_TRANSACTION_ID: cross-partition check via filename-filtered read of silver/transactions
--   (excludes current processing_date to avoid mis-flagging same-date records).
-- UNRESOLVABLE_ACCOUNT_ID is NOT a quarantine code — records enter Silver with _is_resolvable=false.

{{
  config(
    materialized='external',
    location='/app/data/silver/quarantine/date={{ var("processing_date") }}/rejected.parquet',
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

-- Records failing initial checks go straight to quarantine
failed_initial AS (
    SELECT * FROM quality_check WHERE _initial_rejection IS NOT NULL
),

-- Records passing initial checks: check for cross-partition duplicates
passed_initial AS (
    SELECT * FROM quality_check WHERE _initial_rejection IS NULL
),

duplicate_check AS (
    SELECT
        *,
        CASE
            WHEN transaction_id IN (
                SELECT transaction_id
                FROM read_parquet('/app/data/silver/transactions/**/*.parquet', filename=true)
                WHERE regexp_extract(filename, 'date=([0-9-]+)', 1) != '{{ var("processing_date") }}'
            )
            THEN 'DUPLICATE_TRANSACTION_ID'
            ELSE NULL
        END AS _dup_rejection
    FROM passed_initial
),

failed_dup AS (
    SELECT * FROM duplicate_check WHERE _dup_rejection IS NOT NULL
),

-- Combine all rejected records with their rejection reasons
all_rejected AS (
    SELECT
        transaction_id,
        account_id,
        transaction_date,
        amount,
        transaction_code,
        merchant_name,
        channel,
        _source_file,
        _ingested_at,
        _pipeline_run_id,
        _initial_rejection AS _rejection_reason
    FROM failed_initial

    UNION ALL

    SELECT
        transaction_id,
        account_id,
        transaction_date,
        amount,
        transaction_code,
        merchant_name,
        channel,
        _source_file,
        _ingested_at,
        _pipeline_run_id,
        _dup_rejection AS _rejection_reason
    FROM failed_dup
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
    CURRENT_TIMESTAMP                         AS _quarantined_at,
    _rejection_reason
FROM all_rejected
