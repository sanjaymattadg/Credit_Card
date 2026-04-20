-- gold_weekly_account_summary.sql
-- One record per (account_id, ISO week) from Silver transactions where _is_resolvable=true.
-- ISO week definition: Monday start. DATE_TRUNC('week', ...) is Monday-start in DuckDB.
-- Only accounts with at least one resolvable transaction in the week are included.
-- closing_balance sourced from Silver Accounts — NOT computed from transactions.
-- Full table replace on every run (ARCHITECTURE.md Decision 4).
-- _is_resolvable = true filter applied BEFORE all aggregations (I-17).
-- I-05: per (account_id, week) counts and amounts must match Silver source.

{{
  config(
    materialized='external',
    location='/app/data/gold/weekly_account_summary/data.parquet',
    format='parquet'
  )
}}

WITH resolvable_txns AS (
    -- I-17: filter applied once here; all downstream aggregations use only this CTE
    SELECT
        t.transaction_id,
        t.account_id,
        t.transaction_date,
        t._signed_amount,
        t._is_resolvable,
        tc.transaction_type,
        DATE_TRUNC('week', t.transaction_date)              AS week_start_date,
        DATE_TRUNC('week', t.transaction_date) + INTERVAL 6 DAYS AS week_end_date
    FROM read_parquet('/app/data/silver/transactions/**/*.parquet') t
    JOIN read_parquet('/app/data/silver/transaction_codes/data.parquet') tc
      ON t.transaction_code = tc.transaction_code
    WHERE t._is_resolvable = true
),

weekly_agg AS (
    SELECT
        week_start_date,
        week_end_date,
        account_id,
        COUNT(*) FILTER (WHERE transaction_type = 'PURCHASE')           AS total_purchases,
        AVG(_signed_amount) FILTER (WHERE transaction_type = 'PURCHASE') AS avg_purchase_amount,
        SUM(_signed_amount) FILTER (WHERE transaction_type = 'PAYMENT')  AS total_payments,
        SUM(_signed_amount) FILTER (WHERE transaction_type = 'FEE')      AS total_fees,
        SUM(_signed_amount) FILTER (WHERE transaction_type = 'INTEREST') AS total_interest
    FROM resolvable_txns
    GROUP BY week_start_date, week_end_date, account_id
)

SELECT
    w.week_start_date,
    w.week_end_date,
    w.account_id,
    w.total_purchases,
    w.avg_purchase_amount,
    w.total_payments,
    w.total_fees,
    w.total_interest,
    a.current_balance                                       AS closing_balance,
    CURRENT_TIMESTAMP                                       AS _computed_at,
    '{{ var("run_id", "no_run_id") }}'                      AS _pipeline_run_id
FROM weekly_agg w
LEFT JOIN read_parquet('/app/data/silver/accounts/data.parquet') a
  ON w.account_id = a.account_id
ORDER BY w.week_start_date, w.account_id
