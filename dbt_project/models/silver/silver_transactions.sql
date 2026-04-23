{{
    config(
        materialized='external',
        location='/app/data/silver/transactions/date=' ~ var('process_date') ~ '/data.parquet',
        format='parquet',
        post_hook=[
            "SELECT CASE WHEN (SELECT count(*) FROM read_parquet('/app/data/bronze/transactions/date=" ~ var('process_date') ~ "/data.parquet')) != (SELECT count(*) FROM read_parquet('/app/data/silver/transactions/date=" ~ var('process_date') ~ "/data.parquet')) + (SELECT count(*) FROM read_parquet('/app/data/silver/quarantine/date=" ~ var('process_date') ~ "/rejected.parquet') WHERE _source_file LIKE '%transactions%') THEN error('INV-07 violated: conservation check failed — bronze_count != silver_count + quarantine_count for date=" ~ var('process_date') ~ "') ELSE NULL END"
        ]
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
        merchant_name,
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
        merchant_name,
        channel,
        _source_file,
        _ingested_at
    FROM amount_check
    WHERE _rejection_reason_final IS NULL
),

code_check AS (
    -- LEFT JOIN against Silver transaction_codes.
    -- INV-11: records with no matching tc row get NULL debit_credit_indicator and are quarantined.
    -- INV-33: if Silver TC file is absent the JOIN produces NULL for every record → all quarantined.
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
    -- Applied only to records passing code_check (_rejection_reason IS NULL).
    -- INV-09: 'INVALID_CHANNEL' is the only rejection reason produced here.
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
    -- Applied only to records passing channel_check.
    -- INV-13: DR → positive _signed_amount; CR → negative _signed_amount.
    -- ELSE produces NULL — caught by post-derivation assertion below.
    -- INV-12: _signed_amount IS NULL OR = 0 routes to quarantine as 'INVALID_AMOUNT'.
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
    -- Post-derivation assertion: route null or zero _signed_amount to quarantine.
    -- INV-12: no Silver record may have null _signed_amount.
    -- INV-09: rejection reason is 'INVALID_AMOUNT' for this edge case.
    SELECT
        *,
        CASE
            WHEN _signed_amount IS NULL OR _signed_amount = 0 THEN 'INVALID_AMOUNT'
            ELSE NULL
        END AS _sign_rejection_reason
    FROM sign_assignment
),

quarantine_all AS (
    -- INV-06: union of all five quarantine sets — every rejected record appears exactly once.
    -- INV-09: all _rejection_reason values are from the valid enum.
    -- INV-08: _source_file carried verbatim from Bronze in every branch.
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
)

SELECT
    transaction_id,
    account_id,
    transaction_date,
    amount,
    _signed_amount,
    transaction_code,
    merchant_name,
    channel,
    debit_credit_indicator,
    _source_file,
    _ingested_at                    AS _bronze_ingested_at,
    '{{ var("run_id") }}'           AS _pipeline_run_id,
    current_timestamp               AS _promoted_at,
    TRUE                            AS _is_resolvable
FROM silver_ready
WHERE _sign_rejection_reason IS NULL
