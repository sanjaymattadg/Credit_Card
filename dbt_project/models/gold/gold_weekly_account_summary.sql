{{
    config(
        materialized='external',
        location='/app/data/gold/weekly_account_summary/data.parquet',
        format='parquet'
    )
}}

WITH silver_resolvable AS (
    -- INV-34: _is_resolvable = true applied before all aggregations.
    -- INV-19: source is silver/ only — no bronze/ path.
    -- Join to silver_transaction_codes to obtain transaction_type for FILTER aggregations.
    SELECT
        t.account_id,
        t._signed_amount,
        tc.transaction_type,
        DATE_TRUNC('week', t.transaction_date)                       AS week_start_date,
        DATE_TRUNC('week', t.transaction_date) + INTERVAL 6 DAYS     AS week_end_date
    FROM read_parquet('/app/data/silver/transactions/**/*.parquet') t
    INNER JOIN read_parquet('/app/data/silver/transaction_codes/data.parquet') tc
        ON t.transaction_code = tc.transaction_code
    WHERE t._is_resolvable = true
),

weekly_agg AS (
    -- INV-23: one row per (account_id, week_start_date, week_end_date).
    SELECT
        account_id,
        week_start_date,
        week_end_date,
        COUNT(*) FILTER (WHERE transaction_type = 'PURCHASE')            AS total_purchases,
        AVG(_signed_amount) FILTER (WHERE transaction_type = 'PURCHASE') AS avg_purchase_amount,
        SUM(_signed_amount) FILTER (WHERE transaction_type = 'PAYMENT')  AS total_payments,
        SUM(_signed_amount) FILTER (WHERE transaction_type = 'FEE')      AS total_fees,
        SUM(_signed_amount) FILTER (WHERE transaction_type = 'INTEREST') AS total_interest
    FROM silver_resolvable
    GROUP BY account_id, week_start_date, week_end_date
)

-- INV-25: INNER JOIN on Silver Accounts — every output account_id exists in Silver Accounts.
-- _is_resolvable = true (INV-14) guarantees the account_id is present, so no rows are dropped.
SELECT
    w.account_id,
    w.week_start_date,
    w.week_end_date,
    w.total_purchases,
    w.avg_purchase_amount,
    w.total_payments,
    w.total_fees,
    w.total_interest,
    sa.current_balance                  AS closing_balance,
    current_timestamp                   AS _computed_at,
    '{{ var("run_id") }}'               AS _pipeline_run_id
FROM weekly_agg w
INNER JOIN read_parquet('/app/data/silver/accounts/data.parquet') sa
    ON w.account_id = sa.account_id
ORDER BY w.account_id, w.week_start_date
