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

WITH bronze_delta AS (
    -- TODO (Task 4.3): Add rejection filtering here before upsert:
    --   NULL_REQUIRED_FIELD: exclude if account_id, open_date, credit_limit, current_balance,
    --     billing_cycle_start, billing_cycle_end, or account_status is null or empty string
    --   INVALID_ACCOUNT_STATUS: exclude if account_status not in ('ACTIVE', 'SUSPENDED', 'CLOSED')
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
    FROM read_parquet('/app/data/bronze/accounts/date={{ var("process_date") }}/data.parquet')
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
    -- Keep existing Silver records whose account_id is NOT in today's Bronze delta
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
    -- First run: no existing Silver accounts file; output is the delta only
    SELECT * FROM bronze_delta
)
{% endif %}

SELECT * FROM merged
