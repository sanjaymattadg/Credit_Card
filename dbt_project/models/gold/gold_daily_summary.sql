-- gold_daily_summary.sql
-- One record per calendar day from Silver transactions where _is_resolvable = true.
-- Full table replace on every run (ARCHITECTURE.md Decision 4).
-- _is_resolvable = true filter applied BEFORE all aggregations (I-17).
-- No unresolvable record may contribute to any column.
-- I-04: total_transactions per date must match COUNT(*) Silver WHERE _is_resolvable=true.
-- I-14: _pipeline_run_id and _computed_at non-null on every row.

{{
  config(
    materialized='external',
    location='/app/data/gold/daily_summary/data.parquet',
    format='parquet'
  )
}}

WITH resolvable_txns AS (
    -- I-17: filter applied once here; all downstream aggregations use this CTE only
    SELECT
        t.transaction_id,
        t.account_id,
        t.transaction_date,
        t.amount,
        t.transaction_code,
        t.merchant_name,
        t.channel,
        t._signed_amount,
        t._is_resolvable,
        tc.transaction_type
    FROM read_parquet('/app/data/silver/transactions/**/*.parquet') t
    JOIN read_parquet('/app/data/silver/transaction_codes/data.parquet') tc
      ON t.transaction_code = tc.transaction_code
    WHERE t._is_resolvable = true
),

daily_agg AS (
    SELECT
        transaction_date,
        COUNT(*)                                             AS total_transactions,
        SUM(_signed_amount)                                  AS total_signed_amount,
        COUNT(*) FILTER (WHERE channel = 'ONLINE')           AS online_transactions,
        COUNT(*) FILTER (WHERE channel = 'IN_STORE')         AS instore_transactions,
        -- transactions_by_type as a struct
        STRUCT_PACK(
            purchase_count  := COUNT(*) FILTER (WHERE transaction_type = 'PURCHASE'),
            purchase_amount := SUM(_signed_amount) FILTER (WHERE transaction_type = 'PURCHASE'),
            payment_count   := COUNT(*) FILTER (WHERE transaction_type = 'PAYMENT'),
            payment_amount  := SUM(_signed_amount) FILTER (WHERE transaction_type = 'PAYMENT'),
            fee_count       := COUNT(*) FILTER (WHERE transaction_type = 'FEE'),
            fee_amount      := SUM(_signed_amount) FILTER (WHERE transaction_type = 'FEE'),
            interest_count  := COUNT(*) FILTER (WHERE transaction_type = 'INTEREST'),
            interest_amount := SUM(_signed_amount) FILTER (WHERE transaction_type = 'INTEREST'),
            refund_count    := COUNT(*) FILTER (WHERE transaction_type = 'REFUND'),
            refund_amount   := SUM(_signed_amount) FILTER (WHERE transaction_type = 'REFUND')
        )                                                    AS transactions_by_type
    FROM resolvable_txns
    GROUP BY transaction_date
),

period_bounds AS (
    SELECT
        MIN(transaction_date) AS _source_period_start,
        MAX(transaction_date) AS _source_period_end
    FROM resolvable_txns
)

SELECT
    d.transaction_date,
    d.total_transactions,
    d.total_signed_amount,
    d.transactions_by_type,
    d.online_transactions,
    d.instore_transactions,
    CURRENT_TIMESTAMP                        AS _computed_at,
    '{{ var("run_id", "no_run_id") }}'       AS _pipeline_run_id,
    p._source_period_start,
    p._source_period_end
FROM daily_agg d
CROSS JOIN period_bounds p
ORDER BY d.transaction_date
