{{
    config(
        materialized='external',
        location='/app/data/silver/transactions/date=' ~ var('process_date') ~ '/data.parquet',
        format='parquet'
    )
}}

WITH bronze_src AS (
    SELECT
        transaction_id,
        account_id,
        transaction_date,
        amount,
        transaction_code,
        channel,
        _source_file,
        _ingested_at
    FROM read_parquet('/app/data/bronze/transactions/date={{ var("process_date") }}/data.parquet')
),

null_check AS (
    SELECT
        *,
        CASE
            WHEN transaction_id IS NULL OR TRIM(transaction_id) = ''
              OR account_id IS NULL OR TRIM(account_id) = ''
              OR transaction_date IS NULL
              OR amount IS NULL
              OR transaction_code IS NULL OR TRIM(transaction_code) = ''
              OR channel IS NULL OR TRIM(channel) = ''
            THEN 'NULL_REQUIRED_FIELD'
            ELSE NULL
        END AS _rejection_reason
    FROM bronze_src
),

amount_check AS (
    -- Applied only to records that passed null_check (_rejection_reason IS NULL)
    SELECT
        *,
        CASE
            WHEN _rejection_reason IS NULL AND (amount <= 0 OR amount IS NULL)
            THEN 'INVALID_AMOUNT'
            ELSE _rejection_reason
        END AS _rejection_reason_final
    FROM null_check
),

quarantine_candidates AS (
    -- INV-06: all records with any rejection reason — disjoint with valid_so_far
    -- INV-09: _rejection_reason is 'NULL_REQUIRED_FIELD' or 'INVALID_AMOUNT' only
    SELECT
        transaction_id,
        account_id,
        transaction_date,
        amount,
        transaction_code,
        channel,
        _source_file,
        _ingested_at,
        _rejection_reason_final AS _rejection_reason
    FROM amount_check
    WHERE _rejection_reason_final IS NOT NULL
),

valid_so_far AS (
    -- INV-06: all records passing both checks — disjoint with quarantine_candidates
    SELECT
        transaction_id,
        account_id,
        transaction_date,
        amount,
        transaction_code,
        channel,
        _source_file,
        _ingested_at
    FROM amount_check
    WHERE _rejection_reason_final IS NULL
)

SELECT * FROM valid_so_far
