import argparse
import duckdb
import json
import subprocess
import sys
import uuid
from datetime import date, datetime, timedelta
from pathlib import Path

from bronze_loader import (
    load_bronze_transaction_codes,
    load_bronze_transactions,
    load_bronze_accounts,
    bronze_row_count,
)
from pipeline_control import append_run_log_entry, read_watermark, write_watermark

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


def _date_fully_processed(process_date_str: str) -> bool:
    """Return True if Bronze + Silver partitions for this date all exist and Bronze has rows.

    Run log has no process_date column so file presence is the reliable per-date state check.
    Bronze non-empty AND both Silver partition files present implies the full sequence completed.
    """
    bronze_tx = BRONZE_DIR / "transactions" / f"date={process_date_str}" / "data.parquet"
    silver_tx = SILVER_DIR / "transactions" / f"date={process_date_str}" / "data.parquet"
    silver_q  = SILVER_DIR / "quarantine"   / f"date={process_date_str}" / "rejected.parquet"
    for path in (bronze_tx, silver_tx, silver_q):
        if not path.exists():
            return False
    return duckdb.connect().execute(
        f"SELECT count(*) FROM read_parquet('{bronze_tx}')"
    ).fetchone()[0] > 0


def _process_one_date(process_date_str: str, run_id: str) -> None:
    """Run the full Bronze → Silver sequence for a single date, with per-date skip check."""
    if _date_fully_processed(process_date_str):
        print(f"SKIP date={process_date_str} — all models SUCCESS")
        _now = datetime.utcnow()
        for model, layer in [
            ("bronze_transactions",        "BRONZE"),
            ("bronze_accounts",            "BRONZE"),
            ("silver_accounts",            "SILVER"),
            ("silver_accounts_quarantine", "SILVER"),
            ("silver_transactions",        "SILVER"),
            ("silver_quarantine",          "SILVER"),
        ]:
            append_run_log_entry(run_id, "HISTORICAL", model, layer,
                                 _now, _now, "SKIPPED", None, None, None, None)
        return

    # Bronze transactions
    started_at = datetime.utcnow()
    try:
        load_bronze_transactions(date=process_date_str, run_id=run_id)
        count = bronze_row_count("transactions", process_date_str)
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
        load_bronze_accounts(date=process_date_str, run_id=run_id)
        count = bronze_row_count("accounts", process_date_str)
        completed_at = datetime.utcnow()
        append_run_log_entry(run_id, "HISTORICAL", "bronze_accounts", "BRONZE",
                             started_at, completed_at, "SUCCESS", count, count, None, None)
    except Exception as e:
        completed_at = datetime.utcnow()
        append_run_log_entry(run_id, "HISTORICAL", "bronze_accounts", "BRONZE",
                             started_at, completed_at, "FAILED", None, None, None, str(e))
        raise

    # Ensure date-specific Silver directories exist before dbt writes
    (SILVER_DIR / "quarantine"   / f"date={process_date_str}").mkdir(parents=True, exist_ok=True)
    (SILVER_DIR / "transactions" / f"date={process_date_str}").mkdir(parents=True, exist_ok=True)

    # Silver accounts + accounts quarantine
    started_at = datetime.utcnow()
    try:
        subprocess.run(
            ["dbt", "run",
             "--project-dir", str(DBT_PROJECT_DIR),
             "--profiles-dir", str(DBT_PROJECT_DIR),
             "--select", "silver_accounts", "silver_accounts_quarantine",
             "--vars", f'{{"run_id": "{run_id}", "process_date": "{process_date_str}"}}'],
            check=True, capture_output=True, text=True
        )
        accts_rows, _ = _parse_run_results("silver_accounts")
        quar_rows, _  = _parse_run_results("silver_accounts_quarantine")
        completed_at = datetime.utcnow()
        append_run_log_entry(run_id, "HISTORICAL", "silver_accounts", "SILVER",
                             started_at, completed_at, "SUCCESS", accts_rows, accts_rows, None, None)
        append_run_log_entry(run_id, "HISTORICAL", "silver_accounts_quarantine", "SILVER",
                             started_at, completed_at, "SUCCESS", quar_rows, quar_rows, None, None)
    except subprocess.CalledProcessError as e:
        _, accts_msg = _parse_run_results("silver_accounts")
        _, quar_msg  = _parse_run_results("silver_accounts_quarantine")
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
             "--vars", f'{{"run_id": "{run_id}", "process_date": "{process_date_str}"}}'],
            check=True, capture_output=True, text=True
        )
        completed_at = datetime.utcnow()
        conn = duckdb.connect()
        bronze_count = conn.execute(
            f"SELECT count(*) FROM read_parquet('{BRONZE_DIR / 'transactions' / f'date={process_date_str}' / 'data.parquet'}')"
        ).fetchone()[0]
        silver_count = conn.execute(
            f"SELECT count(*) FROM read_parquet('{SILVER_DIR / 'transactions' / f'date={process_date_str}' / 'data.parquet'}')"
        ).fetchone()[0]
        quarantine_count = conn.execute(
            f"SELECT count(*) FROM read_parquet('{SILVER_DIR / 'quarantine' / f'date={process_date_str}' / 'rejected.parquet'}', hive_partitioning=false) WHERE _source_file LIKE '%transactions%'"
        ).fetchone()[0]
        append_run_log_entry(run_id, "HISTORICAL", "silver_transactions", "SILVER",
                             started_at, completed_at, "SUCCESS",
                             bronze_count, silver_count, quarantine_count, None)
        append_run_log_entry(run_id, "HISTORICAL", "silver_quarantine", "SILVER",
                             started_at, completed_at, "SUCCESS",
                             quarantine_count, quarantine_count, None, None)
    except subprocess.CalledProcessError as e:
        _, txn_msg  = _parse_run_results("silver_transactions")
        _, quar_msg = _parse_run_results("silver_quarantine")
        completed_at = datetime.utcnow()
        append_run_log_entry(run_id, "HISTORICAL", "silver_transactions", "SILVER",
                             started_at, completed_at, "FAILED", None, None, None, txn_msg or str(e))
        append_run_log_entry(run_id, "HISTORICAL", "silver_quarantine", "SILVER",
                             started_at, completed_at, "FAILED", None, None, None, quar_msg or str(e))
        raise


def run_historical(start_date: str, end_date: str) -> None:
    # INV-35: run_id generated once here — never regenerated inside the date loop.
    run_id = str(uuid.uuid4())
    print(f"[HISTORICAL] run_id={run_id} start={start_date} end={end_date}")

    # Transaction codes idempotency check (Architecture Decision 3):
    # Both run log SUCCESS and Silver row count > 0 must hold to skip.
    _rl_path = PIPELINE_DIR / "run_log.parquet"
    _tc_log_ok = (
        _rl_path.exists()
        and duckdb.connect().execute(
            f"SELECT count(*) FROM read_parquet('{_rl_path}') "
            f"WHERE model_name = 'bronze_transaction_codes' AND status = 'SUCCESS'"
        ).fetchone()[0] > 0
    )
    _tc_silver_path = SILVER_DIR / "transaction_codes" / "data.parquet"
    _tc_silver_ok = False
    if _tc_silver_path.exists():
        try:
            _tc_silver_ok = duckdb.connect().execute(
                f"SELECT count(*) FROM read_parquet('{_tc_silver_path}')"
            ).fetchone()[0] > 0
        except Exception:
            pass
    skip_tc = _tc_log_ok and _tc_silver_ok
    if skip_tc:
        print("SKIP transaction_codes — already loaded and Silver valid")
        _now = datetime.utcnow()
        append_run_log_entry(run_id, "HISTORICAL", "bronze_transaction_codes", "BRONZE",
                             _now, _now, "SKIPPED", None, None, None, None)
        append_run_log_entry(run_id, "HISTORICAL", "silver_transaction_codes", "SILVER",
                             _now, _now, "SKIPPED", None, None, None, None)

    # Bronze transaction codes (not date-partitioned) — only when not skipped
    if not skip_tc:
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

    # Ensure static Silver directories exist
    for subdir in ("transaction_codes", "accounts", "transactions"):
        (SILVER_DIR / subdir).mkdir(parents=True, exist_ok=True)

    # Silver transaction codes — only when not skipped
    if not skip_tc:
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

    # INV-33: Silver transaction_codes must be non-empty before any Silver transactions promotion
    tc_silver_count = duckdb.connect().execute(
        f"SELECT count(*) FROM read_parquet('{SILVER_DIR / 'transaction_codes' / 'data.parquet'}')"
    ).fetchone()[0]
    if tc_silver_count == 0:
        raise RuntimeError(
            "INV-33 violated: Silver transaction_codes is empty before transaction promotion"
        )

    # Ensure Silver transactions glob is never empty on first-ever pipeline run.
    # DuckDB raises "No files found" when read_parquet glob matches nothing — which happens
    # before any Silver partition is written. A 0-row stub with the full schema lets the
    # existing_silver_ids CTE run safely and returns no rows (no false dedup hits).
    _stub = SILVER_DIR / "transactions" / "date=1900-01-01" / "data.parquet"
    if not _stub.exists():
        _stub.parent.mkdir(parents=True, exist_ok=True)
        duckdb.connect().execute(f"""
            COPY (
                SELECT
                    NULL::VARCHAR    AS transaction_id,
                    NULL::VARCHAR    AS account_id,
                    NULL::DATE       AS transaction_date,
                    NULL::DECIMAL    AS amount,
                    NULL::DECIMAL    AS _signed_amount,
                    NULL::VARCHAR    AS transaction_code,
                    NULL::VARCHAR    AS merchant_name,
                    NULL::VARCHAR    AS channel,
                    NULL::VARCHAR    AS debit_credit_indicator,
                    NULL::VARCHAR    AS _source_file,
                    NULL::TIMESTAMP  AS _bronze_ingested_at,
                    NULL::VARCHAR    AS _pipeline_run_id,
                    NULL::TIMESTAMP  AS _promoted_at,
                    NULL::BOOLEAN    AS _is_resolvable
                WHERE false
            ) TO '{_stub}' (FORMAT PARQUET)
        """)

    # Date range iteration — INV-35: run_id is NOT regenerated inside this loop
    current = date.fromisoformat(start_date)
    end     = date.fromisoformat(end_date)
    while current <= end:
        _process_one_date(str(current), run_id)
        current += timedelta(days=1)

    # Gold — always recomputed after all dates Silver complete (full overwrite per Decision 5)
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
        _, daily_msg  = _parse_run_results("gold_daily_summary")
        _, weekly_msg = _parse_run_results("gold_weekly_account_summary")
        completed_at = datetime.utcnow()
        append_run_log_entry(run_id, "HISTORICAL", "gold_daily_summary", "GOLD",
                             started_at, completed_at, "FAILED", None, None, None, daily_msg or str(e))
        append_run_log_entry(run_id, "HISTORICAL", "gold_weekly_account_summary", "GOLD",
                             started_at, completed_at, "FAILED", None, None, None, weekly_msg or str(e))
        raise

    # Watermark — INV-28: after loop, INV-36: write end_date, INV-29: OQ-03 guard prevents ValueError on re-run
    end_date_obj = date.fromisoformat(end_date)
    current_wm = read_watermark()
    if current_wm is not None and current_wm >= end_date_obj:
        print(f"SKIP watermark — already at or past end_date={end_date}")
    else:
        write_watermark(end_date_obj, run_id)


def run_incremental() -> None:
    run_id = str(uuid.uuid4())

    # INV-28: read-only watermark check — no write here (write is Task 9.3)
    watermark = read_watermark()
    if watermark is None:
        raise RuntimeError(
            "Incremental pipeline cannot run — no watermark. Run historical pipeline first."
        )

    next_date = watermark + timedelta(days=1)
    print(f"[INCREMENTAL] run_id={run_id} next_date={next_date}")

    # INV-27: no-op — clean exit with no side effects if source file absent
    source_file = SOURCE_DIR / f"transactions_{next_date.isoformat()}.csv"
    if not source_file.exists():
        print(
            f"[INCREMENTAL] NOOP — no transactions file for {next_date}. "
            "No layers written. No watermark advance."
        )
        return

    # Bronze → Silver → Gold processing (Task 9.3)


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
