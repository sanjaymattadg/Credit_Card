{{
    config(
        materialized='external',
        location='/app/data/silver/quarantine/date=' ~ var('process_date') ~ '/rejected.parquet',
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
        merchant_name,
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
    SELECT
        *,
        CASE
            WHEN _rejection_reason IS NULL AND (amount <= 0 OR amount IS NULL)
            THEN 'INVALID_AMOUNT'
            ELSE _rejection_reason
        END AS _rejection_reason_final
    FROM null_check
),

valid_so_far AS (
    SELECT
        transaction_id,
        account_id,
        transaction_date,
        amount,
        transaction_code,
        merchant_name,
        channel,
        _source_file,
        _ingested_at
    FROM amount_check
    WHERE _rejection_reason_final IS NULL
),

code_check AS (
    SELECT
        v.transaction_id,
        v.account_id,
        v.transaction_date,
        v.amount,
        v.transaction_code,
        v.merchant_name,
        v.channel,
        v._source_file,
        v._ingested_at,
        tc.debit_credit_indicator,
        CASE
            WHEN tc.transaction_code IS NULL THEN 'INVALID_TRANSACTION_CODE'
            ELSE NULL
        END AS _rejection_reason
    FROM valid_so_far v
    LEFT JOIN read_parquet('/app/data/silver/transaction_codes/data.parquet') tc
        ON v.transaction_code = tc.transaction_code
),

channel_check AS (
    SELECT
        *,
        CASE
            WHEN channel NOT IN ('ONLINE', 'IN_STORE') THEN 'INVALID_CHANNEL'
            ELSE NULL
        END AS _channel_rejection_reason
    FROM code_check
    WHERE _rejection_reason IS NULL
),

sign_assignment AS (
    SELECT
        transaction_id,
        account_id,
        transaction_date,
        amount,
        transaction_code,
        merchant_name,
        channel,
        debit_credit_indicator,
        CASE
            WHEN debit_credit_indicator = 'DR' THEN amount
            WHEN debit_credit_indicator = 'CR' THEN -amount
            ELSE NULL
        END AS _signed_amount,
        _source_file,
        _ingested_at
    FROM channel_check
    WHERE _channel_rejection_reason IS NULL
),

silver_ready AS (
    SELECT
        *,
        CASE
            WHEN _signed_amount IS NULL OR _signed_amount = 0 THEN 'INVALID_AMOUNT'
            ELSE NULL
        END AS _sign_rejection_reason
    FROM sign_assignment
),

within_batch_dedup AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY _ingested_at) AS rn
    FROM silver_ready
    WHERE _sign_rejection_reason IS NULL
),

existing_silver_ids AS (
    SELECT DISTINCT transaction_id
    FROM read_parquet('/app/data/silver/transactions/**/*.parquet')
    WHERE transaction_date::DATE != CAST('{{ var("process_date") }}' AS DATE)
),

cross_partition_dedup AS (
    SELECT
        w.*,
        CASE
            WHEN e.transaction_id IS NOT NULL THEN 'DUPLICATE_TRANSACTION_ID'
            WHEN w.transaction_date::DATE != CAST('{{ var("process_date") }}' AS DATE) THEN 'INVALID_TRANSACTION_DATE'
            ELSE NULL
        END AS _cross_rejection_reason
    FROM within_batch_dedup w
    LEFT JOIN existing_silver_ids e ON w.transaction_id = e.transaction_id
    WHERE w.rn = 1
),

quarantine_all AS (
    -- Branch 1: NULL_REQUIRED_FIELD
    SELECT transaction_id, account_id, transaction_date, amount,
           transaction_code, merchant_name, channel, _source_file,
           _rejection_reason_final AS _rejection_reason
    FROM amount_check WHERE _rejection_reason_final = 'NULL_REQUIRED_FIELD'
    UNION ALL
    -- Branch 2: INVALID_AMOUNT (amount <= 0)
    SELECT transaction_id, account_id, transaction_date, amount,
           transaction_code, merchant_name, channel, _source_file,
           _rejection_reason_final AS _rejection_reason
    FROM amount_check WHERE _rejection_reason_final = 'INVALID_AMOUNT'
    UNION ALL
    -- Branch 3: INVALID_TRANSACTION_CODE
    SELECT transaction_id, account_id, transaction_date, amount,
           transaction_code, merchant_name, channel, _source_file,
           _rejection_reason
    FROM code_check WHERE _rejection_reason = 'INVALID_TRANSACTION_CODE'
    UNION ALL
    -- Branch 4: INVALID_CHANNEL
    SELECT transaction_id, account_id, transaction_date, amount,
           transaction_code, merchant_name, channel, _source_file,
           _channel_rejection_reason AS _rejection_reason
    FROM channel_check WHERE _channel_rejection_reason = 'INVALID_CHANNEL'
    UNION ALL
    -- Branch 5: INVALID_AMOUNT (zero or null derived _signed_amount)
    SELECT transaction_id, account_id, transaction_date, amount,
           transaction_code, merchant_name, channel, _source_file,
           _sign_rejection_reason AS _rejection_reason
    FROM silver_ready WHERE _sign_rejection_reason = 'INVALID_AMOUNT'
    UNION ALL
    -- Branch 6: DUPLICATE_TRANSACTION_ID (within-batch duplicate — rn > 1)
    SELECT transaction_id, account_id, transaction_date, amount,
           transaction_code, merchant_name, channel, _source_file,
           'DUPLICATE_TRANSACTION_ID' AS _rejection_reason
    FROM within_batch_dedup WHERE rn > 1
    UNION ALL
    -- Branch 7: DUPLICATE_TRANSACTION_ID (cross-partition — transaction_id already in Silver)
    SELECT transaction_id, account_id, transaction_date, amount,
           transaction_code, merchant_name, channel, _source_file,
           _cross_rejection_reason AS _rejection_reason
    FROM cross_partition_dedup WHERE _cross_rejection_reason = 'DUPLICATE_TRANSACTION_ID'
    UNION ALL
    -- Branch 8: INVALID_TRANSACTION_DATE (INV-16 — transaction_date != process_date)
    SELECT transaction_id, account_id, transaction_date, amount,
           transaction_code, merchant_name, channel, _source_file,
           _cross_rejection_reason AS _rejection_reason
    FROM cross_partition_dedup WHERE _cross_rejection_reason = 'INVALID_TRANSACTION_DATE'
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
    '{{ var("run_id") }}'   AS _pipeline_run_id,
    current_timestamp       AS _rejected_at,
    _rejection_reason
FROM quarantine_all
