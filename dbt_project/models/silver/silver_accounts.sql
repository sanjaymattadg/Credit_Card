{{
    config(
        materialized='external',
        location='/app/data/silver/accounts/data.parquet',
        format='parquet',
        post_hook=[
            "SELECT CASE WHEN (SELECT count(*) - count(DISTINCT account_id) FROM read_parquet('/app/data/silver/accounts/data.parquet')) != 0 THEN error('INV-17 violated: duplicate account_id found in Silver accounts') END"
        ]
    )
}}

{#
  _record_valid_from uses current_timestamp as a pure audit column — it records when this
  version of the account became current in Silver. It is NOT used for tie-breaking or ordering;
  the upsert key is account_id and the merge is deterministic regardless of timestamp value
  (INV-18 satisfied).
#}

{% set silver_path = '/app/data/silver/accounts/data.parquet' %}
{% if execute %}
    {% set file_check = run_query("SELECT count(*) AS n FROM glob('" ~ silver_path ~ "')") %}
    {% set silver_exists = file_check.columns['n'].values()[0] > 0 %}
{% else %}
    {% set silver_exists = false %}
{% endif %}

WITH bronze_src AS (
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
    -- INV-06: every Bronze record is assigned exactly one outcome tag.
    -- Priority order: NULL_REQUIRED_FIELD before INVALID_ACCOUNT_STATUS.
    -- Records with _rejection_reason IS NULL are valid and enter the upsert merge.
    -- Records with _rejection_reason IS NOT NULL are rejected and go to quarantine (silver_accounts_quarantine.sql).
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
),

bronze_delta AS (
    -- Only valid records enter the upsert merge (INV-06: mutually exclusive with quarantine path)
    SELECT
        account_id,
        open_date,
        credit_limit,
        current_balance,
        billing_cycle_start,
        billing_cycle_end,
        account_status,
        _source_file,
        _ingested_at            AS _bronze_ingested_at,
        '{{ var("run_id") }}'   AS _pipeline_run_id,
        current_timestamp       AS _record_valid_from
    FROM classified
    WHERE _rejection_reason IS NULL
),

{% if silver_exists %}
existing_silver AS (
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
        _pipeline_run_id,
        _record_valid_from
    FROM read_parquet('{{ silver_path }}')
),
retained_existing AS (
    -- Keep existing Silver records whose account_id is NOT in today's valid Bronze delta
    SELECT * FROM existing_silver
    WHERE account_id NOT IN (SELECT account_id FROM bronze_delta)
),
merged AS (
    SELECT * FROM retained_existing
    UNION ALL
    SELECT * FROM bronze_delta
)
{% else %}
merged AS (
    -- First run: no existing Silver accounts file; output is the valid delta only
    SELECT * FROM bronze_delta
)
{% endif %}

SELECT * FROM merged
