import duckdb
from datetime import datetime
from pathlib import Path
from typing import Dict

# Path constants duplicated from pipeline.py to avoid circular import
# (pipeline.py imports bronze_loader; bronze_loader cannot import pipeline)
BRONZE_DIR = Path("/app/data/bronze")
SOURCE_DIR  = Path("/app/source")


def partition_path(entity: str, date: str) -> Path:
    return BRONZE_DIR / entity / f"date={date}" / "data.parquet"


def partition_exists_and_valid(path: Path) -> bool:
    if not path.exists():
        return False
    try:
        count = duckdb.connect().execute(
            f"SELECT count(*) FROM read_parquet('{path}')"
        ).fetchone()[0]
        return count > 0
    except Exception:
        # Covers FileNotFoundError (race) and corrupt/partial Parquet files
        return False


def read_csv_to_duckdb(filepath: Path, schema: Dict[str, str]) -> duckdb.DuckDBPyRelation:
    return duckdb.read_csv(str(filepath), dtype=schema)


def load_bronze_transactions(date: str, run_id: str) -> None:
    source_file = f"transactions_{date}.csv"
    src_path    = SOURCE_DIR / source_file
    out_path    = partition_path("transactions", date)

    if partition_exists_and_valid(out_path):
        print(f"SKIP bronze_transactions {date} — partition already valid")
        return

    schema = {
        "transaction_id":   "VARCHAR",
        "account_id":       "VARCHAR",
        "transaction_date": "DATE",
        "amount":           "DECIMAL(18,4)",
        "transaction_code": "VARCHAR",
        "merchant_name":    "VARCHAR",
        "channel":          "VARCHAR",
    }

    # Read with explicit schema (INV-02) then materialise to Arrow for clean connection hand-off
    arrow_data  = read_csv_to_duckdb(src_path, schema).arrow()
    ingested_at = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    out_path.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    con.register("_src", arrow_data)
    con.execute(f"""
        COPY (
            SELECT
                transaction_id,
                account_id,
                transaction_date,
                amount,
                transaction_code,
                merchant_name,
                channel,
                '{source_file}'           AS _source_file,
                TIMESTAMP '{ingested_at}' AS _ingested_at,
                '{run_id}'                AS _pipeline_run_id
            FROM _src
        ) TO '{out_path}' (FORMAT PARQUET)
    """)

    if not partition_exists_and_valid(out_path):
        raise RuntimeError(f"Post-write validation failed for bronze_transactions {date}")


def load_bronze_accounts(date: str, run_id: str) -> None:
    source_file = f"accounts_{date}.csv"
    src_path    = SOURCE_DIR / source_file
    out_path    = partition_path("accounts", date)

    # Step 1: missing file = no delta that day — not an error
    if not src_path.exists():
        print(f"SKIP bronze_accounts {date} — no delta file")
        return

    # Step 2: idempotency guard
    if partition_exists_and_valid(out_path):
        print(f"SKIP bronze_accounts {date} — partition already valid")
        return

    # billing_cycle_start/end are DATE in source CSV (spec said INTEGER — corrected per engineer decision)
    # customer_name present in CSV but excluded from Bronze per engineer decision
    schema = {
        "account_id":          "VARCHAR",
        "customer_name":       "VARCHAR",
        "account_status":      "VARCHAR",
        "credit_limit":        "DECIMAL(18,4)",
        "current_balance":     "DECIMAL(18,4)",
        "open_date":           "DATE",
        "billing_cycle_start": "DATE",
        "billing_cycle_end":   "DATE",
    }

    arrow_data  = read_csv_to_duckdb(src_path, schema).arrow()
    ingested_at = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    out_path.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    con.register("_src", arrow_data)
    con.execute(f"""
        COPY (
            SELECT
                account_id,
                open_date,
                credit_limit,
                current_balance,
                billing_cycle_start,
                billing_cycle_end,
                account_status,
                '{source_file}'           AS _source_file,
                TIMESTAMP '{ingested_at}' AS _ingested_at,
                '{run_id}'                AS _pipeline_run_id
            FROM _src
        ) TO '{out_path}' (FORMAT PARQUET)
    """)

    if not partition_exists_and_valid(out_path):
        raise RuntimeError(f"Post-write validation failed for bronze_accounts {date}")
