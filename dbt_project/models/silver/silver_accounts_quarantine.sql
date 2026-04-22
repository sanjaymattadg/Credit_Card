{{
    config(
        materialized='external',
        location='/app/data/silver/quarantine/date=' ~ var('process_date') ~ '/rejected.parquet',
        format='parquet'
    )
}}

WITH bronze_src AS (
    -- INV-08: source columns carried verbatim — no transformation before quarantine write.
    SELECT
        account_id,
        open_date,
        credit_limit,
        current_balance,
        billing_cycle_start,
        billing_cycle_end,
        account_status,
        _source_file,
        _ingested_at
    FROM read_parquet('/app/data/bronze/accounts/date={{ var("process_date") }}/data.parquet')
),

classified AS (
    -- Identical classification logic to silver_accounts.sql (INV-06: same rules, complementary filter).
    -- Priority order: NULL_REQUIRED_FIELD before INVALID_ACCOUNT_STATUS.
    -- INV-09: rejection reason values are string literals from the exact permitted set only.
    SELECT
        *,
        CASE
            WHEN account_id IS NULL OR TRIM(CAST(account_id AS VARCHAR)) = ''
              OR open_date IS NULL
              OR credit_limit IS NULL
              OR current_balance IS NULL
              OR billing_cycle_start IS NULL
              OR billing_cycle_end IS NULL
              OR account_status IS NULL OR TRIM(account_status) = ''
            THEN 'NULL_REQUIRED_FIELD'
            WHEN account_status NOT IN ('ACTIVE', 'SUSPENDED', 'CLOSED')
            THEN 'INVALID_ACCOUNT_STATUS'
            ELSE NULL
        END AS _rejection_reason
    FROM bronze_src
)

-- INV-06: complementary filter to silver_accounts.sql — _rejection_reason IS NOT NULL
-- guarantees no record is in both Silver and quarantine.
-- INV-08: all source column values are carried verbatim from Bronze, no transformation.
-- INV-09: _rejection_reason is exclusively 'NULL_REQUIRED_FIELD' or 'INVALID_ACCOUNT_STATUS'.
SELECT
    account_id,
    open_date,
    credit_limit,
    current_balance,
    billing_cycle_start,
    billing_cycle_end,
    account_status,
    _source_file,
    '{{ var("run_id") }}'   AS _pipeline_run_id,
    current_timestamp       AS _rejected_at,
    _rejection_reason
FROM classified
WHERE _rejection_reason IS NOT NULL
