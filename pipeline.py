import argparse
import duckdb
import sys
import uuid
from datetime import datetime
from pathlib import Path

from bronze_loader import (
    load_bronze_transaction_codes,
    load_bronze_transactions,
    load_bronze_accounts,
    bronze_row_count,
)
from pipeline_control import append_run_log_entry

DATA_DIR        = Path("/app/data")
SOURCE_DIR      = Path("/app/source")
BRONZE_DIR      = DATA_DIR / "bronze"
SILVER_DIR      = DATA_DIR / "silver"
GOLD_DIR        = DATA_DIR / "gold"
PIPELINE_DIR    = DATA_DIR / "pipeline"
DBT_PROJECT_DIR = Path("/app/dbt_project")


def run_historical(start_date: str, end_date: str) -> None:
    run_id = str(uuid.uuid4())
    print(f"[HISTORICAL] run_id={run_id} start={start_date} end={end_date}")

    # Bronze transaction codes (not date-partitioned)
    started_at = datetime.utcnow()
    try:
        load_bronze_transaction_codes(run_id=run_id)
        tc_path = BRONZE_DIR / "transaction_codes" / "data.parquet"
        count = duckdb.connect().execute(
            f"SELECT count(*) FROM read_parquet('{tc_path}')"
        ).fetchone()[0]
        completed_at = datetime.utcnow()
        append_run_log_entry(run_id, "HISTORICAL", "bronze_transaction_codes", "BRONZE",
                             started_at, completed_at, "SUCCESS", count, count, None, None)
    except Exception as e:
        completed_at = datetime.utcnow()
        append_run_log_entry(run_id, "HISTORICAL", "bronze_transaction_codes", "BRONZE",
                             started_at, completed_at, "FAILED", None, None, None, str(e))
        raise

    # Bronze transactions
    started_at = datetime.utcnow()
    try:
        load_bronze_transactions(date=start_date, run_id=run_id)
        count = bronze_row_count("transactions", start_date)
        completed_at = datetime.utcnow()
        append_run_log_entry(run_id, "HISTORICAL", "bronze_transactions", "BRONZE",
                             started_at, completed_at, "SUCCESS", count, count, None, None)
    except Exception as e:
        completed_at = datetime.utcnow()
        append_run_log_entry(run_id, "HISTORICAL", "bronze_transactions", "BRONZE",
                             started_at, completed_at, "FAILED", None, None, None, str(e))
        raise

    # Bronze accounts
    started_at = datetime.utcnow()
    try:
        load_bronze_accounts(date=start_date, run_id=run_id)
        count = bronze_row_count("accounts", start_date)
        completed_at = datetime.utcnow()
        append_run_log_entry(run_id, "HISTORICAL", "bronze_accounts", "BRONZE",
                             started_at, completed_at, "SUCCESS", count, count, None, None)
    except Exception as e:
        completed_at = datetime.utcnow()
        append_run_log_entry(run_id, "HISTORICAL", "bronze_accounts", "BRONZE",
                             started_at, completed_at, "FAILED", None, None, None, str(e))
        raise

    # Silver and Gold — not implemented yet (SKIPPED entries per task spec)
    now = datetime.utcnow()
    append_run_log_entry(run_id, "HISTORICAL", "silver", "SILVER",
                         now, now, "SKIPPED", None, None, None, None)
    append_run_log_entry(run_id, "HISTORICAL", "gold", "GOLD",
                         now, now, "SKIPPED", None, None, None, None)
    print("Silver — NOT IMPLEMENTED")
    print("Gold   — NOT IMPLEMENTED")


def run_incremental() -> None:
    run_id = str(uuid.uuid4())
    print(f"[INCREMENTAL] run_id={run_id} — NOT IMPLEMENTED")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Credit Card Transactions Lake pipeline"
    )
    subparsers = parser.add_subparsers(dest="command")

    hist = subparsers.add_parser(
        "historical",
        help="Run historical pipeline over a date range"
    )
    hist.add_argument("--start-date", required=True, metavar="YYYY-MM-DD")
    hist.add_argument("--end-date",   required=True, metavar="YYYY-MM-DD")

    subparsers.add_parser(
        "incremental",
        help="Run incremental pipeline for next unprocessed date"
    )

    args = parser.parse_args()

    if args.command == "historical":
        run_historical(args.start_date, args.end_date)
    elif args.command == "incremental":
        run_incremental()
    else:
        parser.print_help()
        sys.exit(0)
