import argparse
import duckdb
import json
import subprocess
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


def _parse_run_results(model_name):
    """Return (rows_affected, message) for a named model from the last dbt run_results.json."""
    path = DBT_PROJECT_DIR / "target" / "run_results.json"
    try:
        data = json.loads(path.read_text())
        for result in data.get("results", []):
            if result.get("unique_id", "").endswith(f".{model_name}"):
                rows = result.get("adapter_response", {}).get("rows_affected")
                msg = result.get("message")
                return rows, msg
    except Exception:
        pass
    return None, None


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

    # Ensure Silver directories exist before dbt writes
    for subdir in ("transaction_codes", "accounts", "transactions"):
        (SILVER_DIR / subdir).mkdir(parents=True, exist_ok=True)
    (SILVER_DIR / "quarantine" / f"date={start_date}").mkdir(parents=True, exist_ok=True)
    (SILVER_DIR / "transactions" / f"date={start_date}").mkdir(parents=True, exist_ok=True)

    # Silver transaction codes
    started_at = datetime.utcnow()
    try:
        subprocess.run(
            ["dbt", "run",
             "--project-dir", str(DBT_PROJECT_DIR),
             "--profiles-dir", str(DBT_PROJECT_DIR),
             "--select", "silver_transaction_codes",
             "--vars", f'{{"run_id": "{run_id}"}}'],
            check=True, capture_output=True, text=True
        )
        rows, _ = _parse_run_results("silver_transaction_codes")
        completed_at = datetime.utcnow()
        append_run_log_entry(run_id, "HISTORICAL", "silver_transaction_codes", "SILVER",
                             started_at, completed_at, "SUCCESS", rows, rows, None, None)
    except subprocess.CalledProcessError as e:
        _, msg = _parse_run_results("silver_transaction_codes")
        completed_at = datetime.utcnow()
        append_run_log_entry(run_id, "HISTORICAL", "silver_transaction_codes", "SILVER",
                             started_at, completed_at, "FAILED", None, None, None, msg or str(e))
        now = datetime.utcnow()
        append_run_log_entry(run_id, "HISTORICAL", "silver_accounts", "SILVER",
                             now, now, "SKIPPED", None, None, None, None)
        append_run_log_entry(run_id, "HISTORICAL", "silver_accounts_quarantine", "SILVER",
                             now, now, "SKIPPED", None, None, None, None)
        append_run_log_entry(run_id, "HISTORICAL", "silver_transactions", "SILVER",
                             now, now, "SKIPPED", None, None, None, None)
        append_run_log_entry(run_id, "HISTORICAL", "silver_quarantine", "SILVER",
                             now, now, "SKIPPED", None, None, None, None)
        raise

    # INV-33: Silver transaction_codes must be non-empty before any silver_transactions step
    tc_silver_count = duckdb.connect().execute(
        f"SELECT count(*) FROM read_parquet('{SILVER_DIR / 'transaction_codes' / 'data.parquet'}')"
    ).fetchone()[0]
    if tc_silver_count == 0:
        raise RuntimeError(
            "INV-33 violated: Silver transaction_codes is empty before transaction promotion"
        )

    # Silver accounts + quarantine
    started_at = datetime.utcnow()
    try:
        subprocess.run(
            ["dbt", "run",
             "--project-dir", str(DBT_PROJECT_DIR),
             "--profiles-dir", str(DBT_PROJECT_DIR),
             "--select", "silver_accounts", "silver_accounts_quarantine",
             "--vars", f'{{"run_id": "{run_id}", "process_date": "{start_date}"}}'],
            check=True, capture_output=True, text=True
        )
        accts_rows, _ = _parse_run_results("silver_accounts")
        quar_rows, _ = _parse_run_results("silver_accounts_quarantine")
        completed_at = datetime.utcnow()
        append_run_log_entry(run_id, "HISTORICAL", "silver_accounts", "SILVER",
                             started_at, completed_at, "SUCCESS", accts_rows, accts_rows, None, None)
        append_run_log_entry(run_id, "HISTORICAL", "silver_accounts_quarantine", "SILVER",
                             started_at, completed_at, "SUCCESS", quar_rows, quar_rows, None, None)
    except subprocess.CalledProcessError as e:
        _, accts_msg = _parse_run_results("silver_accounts")
        _, quar_msg = _parse_run_results("silver_accounts_quarantine")
        completed_at = datetime.utcnow()
        append_run_log_entry(run_id, "HISTORICAL", "silver_accounts", "SILVER",
                             started_at, completed_at, "FAILED", None, None, None, accts_msg or str(e))
        append_run_log_entry(run_id, "HISTORICAL", "silver_accounts_quarantine", "SILVER",
                             started_at, completed_at, "FAILED", None, None, None, quar_msg or str(e))
        now = datetime.utcnow()
        append_run_log_entry(run_id, "HISTORICAL", "silver_transactions", "SILVER",
                             now, now, "SKIPPED", None, None, None, None)
        append_run_log_entry(run_id, "HISTORICAL", "silver_quarantine", "SILVER",
                             now, now, "SKIPPED", None, None, None, None)
        raise

    # Silver transactions + quarantine
    started_at = datetime.utcnow()
    try:
        subprocess.run(
            ["dbt", "run",
             "--project-dir", str(DBT_PROJECT_DIR),
             "--profiles-dir", str(DBT_PROJECT_DIR),
             "--select", "silver_transactions", "silver_quarantine",
             "--vars", f'{{"run_id": "{run_id}", "process_date": "{start_date}"}}'],
            check=True, capture_output=True, text=True
        )
        completed_at = datetime.utcnow()
        conn = duckdb.connect()
        bronze_count = conn.execute(
            f"SELECT count(*) FROM read_parquet('{BRONZE_DIR / 'transactions' / f'date={start_date}' / 'data.parquet'}')"
        ).fetchone()[0]
        silver_count = conn.execute(
            f"SELECT count(*) FROM read_parquet('{SILVER_DIR / 'transactions' / f'date={start_date}' / 'data.parquet'}')"
        ).fetchone()[0]
        quarantine_count = conn.execute(
            f"SELECT count(*) FROM read_parquet('{SILVER_DIR / 'quarantine' / f'date={start_date}' / 'rejected.parquet'}', hive_partitioning=false) WHERE _source_file LIKE '%transactions%'"
        ).fetchone()[0]
        append_run_log_entry(run_id, "HISTORICAL", "silver_transactions", "SILVER",
                             started_at, completed_at, "SUCCESS",
                             bronze_count, silver_count, quarantine_count, None)
        append_run_log_entry(run_id, "HISTORICAL", "silver_quarantine", "SILVER",
                             started_at, completed_at, "SUCCESS",
                             quarantine_count, quarantine_count, None, None)
    except subprocess.CalledProcessError as e:
        _, txn_msg = _parse_run_results("silver_transactions")
        _, quar_msg = _parse_run_results("silver_quarantine")
        completed_at = datetime.utcnow()
        append_run_log_entry(run_id, "HISTORICAL", "silver_transactions", "SILVER",
                             started_at, completed_at, "FAILED", None, None, None, txn_msg or str(e))
        append_run_log_entry(run_id, "HISTORICAL", "silver_quarantine", "SILVER",
                             started_at, completed_at, "FAILED", None, None, None, quar_msg or str(e))
        raise

    # Gold — daily summary and weekly account summary
    (GOLD_DIR / "daily_summary").mkdir(parents=True, exist_ok=True)
    (GOLD_DIR / "weekly_account_summary").mkdir(parents=True, exist_ok=True)
    started_at = datetime.utcnow()
    try:
        subprocess.run(
            ["dbt", "run",
             "--project-dir", str(DBT_PROJECT_DIR),
             "--profiles-dir", str(DBT_PROJECT_DIR),
             "--select", "gold_daily_summary", "gold_weekly_account_summary",
             "--vars", f'{{"run_id": "{run_id}", "process_date": "{start_date}"}}'],
            check=True, capture_output=True, text=True
        )
        completed_at = datetime.utcnow()
        conn = duckdb.connect()
        silver_resolvable_count = conn.execute(
            f"SELECT count(*) FROM read_parquet('{SILVER_DIR}/transactions/**/*.parquet') WHERE _is_resolvable = true"
        ).fetchone()[0]
        daily_count = conn.execute(
            f"SELECT count(*) FROM read_parquet('{GOLD_DIR / 'daily_summary' / 'data.parquet'}')"
        ).fetchone()[0]
        weekly_count = conn.execute(
            f"SELECT count(*) FROM read_parquet('{GOLD_DIR / 'weekly_account_summary' / 'data.parquet'}')"
        ).fetchone()[0]
        append_run_log_entry(run_id, "HISTORICAL", "gold_daily_summary", "GOLD",
                             started_at, completed_at, "SUCCESS",
                             silver_resolvable_count, daily_count, None, None)
        append_run_log_entry(run_id, "HISTORICAL", "gold_weekly_account_summary", "GOLD",
                             started_at, completed_at, "SUCCESS",
                             silver_resolvable_count, weekly_count, None, None)
    except subprocess.CalledProcessError as e:
        _, daily_msg = _parse_run_results("gold_daily_summary")
        _, weekly_msg = _parse_run_results("gold_weekly_account_summary")
        completed_at = datetime.utcnow()
        append_run_log_entry(run_id, "HISTORICAL", "gold_daily_summary", "GOLD",
                             started_at, completed_at, "FAILED", None, None, None, daily_msg or str(e))
        append_run_log_entry(run_id, "HISTORICAL", "gold_weekly_account_summary", "GOLD",
                             started_at, completed_at, "FAILED", None, None, None, weekly_msg or str(e))
        raise


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
