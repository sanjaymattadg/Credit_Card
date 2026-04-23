{{
    config(
        materialized='external',
        location='/app/data/gold/daily_summary/data.parquet',
        format='parquet'
    )
}}

WITH silver_resolvable AS (
    -- INV-34: _is_resolvable = true applied here — no aggregation column may
    -- reference Silver without this filter.
    -- INV-19: source is silver/transactions only — no bronze/ path.
    SELECT
        transaction_date,
        transaction_code,
        channel,
        _signed_amount
    FROM read_parquet('/app/data/silver/transactions/**/*.parquet')
    WHERE _is_resolvable = true
),

period_bounds AS (
    SELECT
        MIN(transaction_date) AS _source_period_start,
        MAX(transaction_date) AS _source_period_end
    FROM silver_resolvable
)

-- INV-21: GROUP BY transaction_date only — exactly one row per calendar date.
-- INV-24: full overwrite on every run (materialized='external', no append).
SELECT
    s.transaction_date,
    COUNT(*)                                                                       AS total_transactions,
    SUM(s._signed_amount)                                                          AS total_signed_amount,
    COUNT(*) FILTER (WHERE s.channel = 'ONLINE')                                   AS online_transactions,
    COUNT(*) FILTER (WHERE s.channel = 'IN_STORE')                                 AS instore_transactions,
    COUNT(*) FILTER (WHERE s.transaction_code = 'PURCH01')                         AS purchases_count,
    COALESCE(SUM(s._signed_amount) FILTER (WHERE s.transaction_code = 'PURCH01'), 0) AS purchases_amount,
    COUNT(*) FILTER (WHERE s.transaction_code = 'PAY01')                           AS payments_count,
    COALESCE(SUM(s._signed_amount) FILTER (WHERE s.transaction_code = 'PAY01'),   0) AS payments_amount,
    COUNT(*) FILTER (WHERE s.transaction_code = 'FEE01')                           AS fees_count,
    COALESCE(SUM(s._signed_amount) FILTER (WHERE s.transaction_code = 'FEE01'),   0) AS fees_amount,
    COUNT(*) FILTER (WHERE s.transaction_code = 'INT01')                           AS interest_count,
    COALESCE(SUM(s._signed_amount) FILTER (WHERE s.transaction_code = 'INT01'),   0) AS interest_amount,
    COUNT(*) FILTER (WHERE s.transaction_code = 'REF01')                           AS refunds_count,
    COALESCE(SUM(s._signed_amount) FILTER (WHERE s.transaction_code = 'REF01'),   0) AS refunds_amount,
    current_timestamp                                                              AS _computed_at,
    '{{ var("run_id") }}'                                                          AS _pipeline_run_id,
    p._source_period_start,
    p._source_period_end
FROM silver_resolvable s
CROSS JOIN period_bounds p
GROUP BY s.transaction_date, p._source_period_start, p._source_period_end
ORDER BY s.transaction_date
