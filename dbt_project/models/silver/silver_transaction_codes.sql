{{
    config(
        materialized='external',
        location='/app/data/silver/transaction_codes/data.parquet',
        format='parquet',
        post_hook=[
            "SELECT CASE WHEN (SELECT count(*) FROM read_parquet('/app/data/silver/transaction_codes/data.parquet')) = 0 THEN error('INV-33 violated: Silver transaction_codes is empty after model run') END"
        ]
    )
}}

SELECT
    transaction_code,
    transaction_type,
    description,
    debit_credit_indicator,
    affects_balance,
    _source_file,
    _ingested_at            AS _bronze_ingested_at,
    '{{ var("run_id") }}'   AS _pipeline_run_id
FROM read_parquet('/app/data/bronze/transaction_codes/data.parquet')
