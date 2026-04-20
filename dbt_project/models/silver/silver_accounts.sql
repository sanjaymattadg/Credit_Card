-- silver_accounts.sql
-- Upserts Bronze account records into Silver: latest record per account_id.
-- Ordering: _source_file DESC (cross-file), _row_number DESC (within-file).
-- _row_number is derived from physical scan order of the Bronze Parquet file.
-- This is stable ONLY because the Bronze loader preserves CSV row order (Gap 3).
-- Quality rules: NULL_REQUIRED_FIELD, INVALID_ACCOUNT_STATUS → quarantine.
-- I-02: every account_id from Bronze must appear in Silver after promotion.
-- I-11: exactly one row per account_id at all times.
-- I-12: latest _source_file wins; last row within file wins.

{{
  config(
    materialized='external',
    location='/app/data/silver/accounts/data.parquet',
    format='parquet'
  )
}}

WITH bronze_all AS (
    SELECT
        account_id,
        open_date,
        credit_limit,
        current_balance,
        billing_cycle_start,
        billing_cycle_end,
        account_status,
        _source_file,
        _ingested_at        AS _bronze_ingested_at,
        _pipeline_run_id,
        filename
    FROM read_parquet('/app/data/bronze/accounts/**/*.parquet', filename=true)
),

with_rownum AS (
    -- Assign within-file row position. Stable because Bronze preserves CSV order.
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY filename
            ORDER BY (SELECT NULL)
        ) AS _row_number
    FROM bronze_all
),

quality_check AS (
    SELECT
        *,
        CASE
            WHEN account_id IS NULL OR account_id = ''
              OR open_date IS NULL
              OR credit_limit IS NULL
              OR current_balance IS NULL
              OR billing_cycle_start IS NULL
              OR billing_cycle_end IS NULL
              OR account_status IS NULL OR account_status = ''
            THEN 'NULL_REQUIRED_FIELD'
            WHEN account_status NOT IN ('ACTIVE', 'SUSPENDED', 'CLOSED')
            THEN 'INVALID_ACCOUNT_STATUS'
            ELSE NULL
        END AS _rejection_reason
    FROM with_rownum
),

passed AS (
    SELECT * FROM quality_check WHERE _rejection_reason IS NULL
),

deduped AS (
    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY account_id
                ORDER BY _source_file DESC, _row_number DESC
            ) AS _rank
        FROM passed
    ) ranked
    WHERE _rank = 1
)

SELECT
    account_id,
    open_date,
    credit_limit,
    current_balance,
    billing_cycle_start,
    billing_cycle_end,
    account_status,
    _source_file,
    _bronze_ingested_at,
    '{{ var("run_id", "no_run_id") }}'  AS _pipeline_run_id,
    CURRENT_TIMESTAMP                   AS _record_valid_from
FROM deduped
