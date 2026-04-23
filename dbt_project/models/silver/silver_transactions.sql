{{
    config(
        materialized='external',
        location='/app/data/silver/transactions/date=' ~ var('process_date') ~ '/data.parquet',
        format='parquet',
        post_hook=[
            "WITH _counts AS (
                SELECT
                    (SELECT count(*) FROM read_parquet('/app/data/bronze/transactions/date=" ~ var('process_date') ~ "/data.parquet')) AS bronze,
                    (SELECT count(*) FROM read_parquet('/app/data/silver/transactions/date=" ~ var('process_date') ~ "/data.parquet')) AS silver,
                    (SELECT count(*) FROM read_parquet('/app/data/silver/quarantine/date=" ~ var('process_date') ~ "/rejected.parquet') WHERE _source_file LIKE '%transactions%') AS quarantine
            )
            SELECT CASE WHEN bronze != silver + quarantine
                THEN error('INV-07 violated: bronze=' || bronze || ', silver=' || silver || ', quarantine=' || quarantine || ', gap=' || (bronze - silver - quarantine) || ' for date=" ~ var('process_date') ~ "')
                ELSE NULL
            END FROM _counts"
        ]
    )
}}

-- INV-16: location uses var('process_date'). All Silver survivors have transaction_date = process_date
-- (enforced by cross_partition_dedup INVALID_TRANSACTION_DATE branch — Task 6.2), so the partition
-- path is effectively derived from the transaction_date field, not only the process_date variable.

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

within_batch_dedup AS (
    -- INV-10: ROW_NUMBER over transaction_id within this Bronze file. rn=1 proceeds to Silver.
    -- rn>1 records are within-batch duplicates and route to quarantine_all Branch 6.
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY _ingested_at) AS rn
    FROM silver_ready
    WHERE _sign_rejection_reason IS NULL
),

existing_silver_ids AS (
    -- INV-10: scan Silver partitions from OTHER dates only.
    -- Excludes the current process_date partition so re-runs of the same date are idempotent
    -- (same records promoted, not quarantined on second run). Only cross-DATE duplicates are caught.
    SELECT DISTINCT transaction_id
    FROM read_parquet('/app/data/silver/transactions/**/*.parquet')
    WHERE transaction_date::DATE != CAST('{{ var("process_date") }}' AS DATE)
),

cross_partition_dedup AS (
    -- INV-10: records already promoted in any prior partition → DUPLICATE_TRANSACTION_ID.
    -- INV-16: records where transaction_date != process_date → INVALID_TRANSACTION_DATE.
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

account_resolution AS (
    -- INV-14: LEFT JOIN on account_id. NULL from join → _is_resolvable = FALSE.
    -- INV-15: unresolvable records go to Silver, NOT quarantine. No UNRESOLVABLE_ACCOUNT_ID branch exists.
    SELECT
        c.*,
        CASE WHEN sa.account_id IS NOT NULL THEN TRUE ELSE FALSE END AS _is_resolvable
    FROM cross_partition_dedup c
    LEFT JOIN read_parquet('/app/data/silver/accounts/data.parquet') sa
        ON c.account_id = sa.account_id
    WHERE c._cross_rejection_reason IS NULL
),

quarantine_all AS (
    -- INV-06: union of all eight quarantine sets — every rejected record appears exactly once.
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
    UNION ALL
    -- Branch 6: DUPLICATE_TRANSACTION_ID (within-batch duplicate — rn > 1)
    -- INV-10: intra-file duplicates detected here; cross-partition dedup is Task 6.2.
    -- INV-09: 'DUPLICATE_TRANSACTION_ID' is the exact rejection reason string.
    SELECT transaction_id, account_id, transaction_date, amount,
           transaction_code, merchant_name, channel, _source_file,
           'DUPLICATE_TRANSACTION_ID' AS _rejection_reason
    FROM within_batch_dedup WHERE rn > 1
    UNION ALL
    -- Branch 7: DUPLICATE_TRANSACTION_ID (cross-partition — transaction_id already in Silver)
    -- INV-10: combined with Branch 6, ensures global uniqueness across all Silver partitions.
    SELECT transaction_id, account_id, transaction_date, amount,
           transaction_code, merchant_name, channel, _source_file,
           _cross_rejection_reason AS _rejection_reason
    FROM cross_partition_dedup WHERE _cross_rejection_reason = 'DUPLICATE_TRANSACTION_ID'
    UNION ALL
    -- Branch 8: INVALID_TRANSACTION_DATE (INV-16 — transaction_date != process_date)
    -- INV-06: ensures these records route to quarantine, not silently dropped.
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
    _signed_amount,
    transaction_code,
    merchant_name,
    channel,
    debit_credit_indicator,
    _source_file,
    _ingested_at                    AS _bronze_ingested_at,
    '{{ var("run_id") }}'           AS _pipeline_run_id,
    current_timestamp               AS _promoted_at,
    _is_resolvable
FROM account_resolution
