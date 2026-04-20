-- silver_transaction_codes.sql
-- Promotes all Bronze transaction code records to Silver.
-- Loaded once during historical pipeline initialisation (I-03).
-- No quality filtering — all Bronze codes are promoted.
-- Full table replace on re-run (materialized: table).

{{
  config(
    materialized='external',
    location='/app/data/silver/transaction_codes/data.parquet',
    format='parquet'
  )
}}

SELECT
    transaction_code,
    transaction_type,
    description,
    debit_credit_indicator,
    affects_balance,
    _source_file,
    _ingested_at        AS _bronze_ingested_at,
    _pipeline_run_id
FROM read_parquet('/app/data/bronze/transaction_codes/data.parquet')
