"""
End-to-end smoke test for the historical pipeline.
Runs a clean-state historical pipeline and asserts all layer invariants.
Not called from pipeline.py.

Adapted date range: 2024-01-01 to 2024-01-07.
Task spec used 2024-01-15 to 2024-01-21; source files only exist for 2024-01-01 to 2024-01-07.
"""

import shutil
import subprocess
import sys
from pathlib import Path

import duckdb

DATA_DIR     = Path("/app/data")
BRONZE_DIR   = DATA_DIR / "bronze"
SILVER_DIR   = DATA_DIR / "silver"
GOLD_DIR     = DATA_DIR / "gold"
PIPELINE_DIR = DATA_DIR / "pipeline"

START_DATE = "2024-01-01"
END_DATE   = "2024-01-07"
ALL_DATES  = [
    "2024-01-01", "2024-01-02", "2024-01-03",
    "2024-01-04", "2024-01-05", "2024-01-06", "2024-01-07",
]


def clean_data() -> None:
    for item in DATA_DIR.iterdir():
        if item.is_dir():
            shutil.rmtree(item)
        else:
            item.unlink()


def run_pipeline(start: str, end: str) -> None:
    result = subprocess.run(
        ["python", "pipeline.py", "historical", "--start-date", start, "--end-date", end],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        print("FAIL — pipeline exited non-zero")
        print(result.stderr[-2000:])
        sys.exit(1)


def check(label: str, passed: bool, detail: str = "") -> bool:
    tag = "PASS" if passed else "FAIL"
    suffix = f" — {detail}" if detail else ""
    print(f"[{tag}] {label}{suffix}")
    return passed


def main() -> None:
    failures = []
    con = duckdb.connect()

    # Step 1: clean state
    print("Step 1: Cleaning data/ ...")
    clean_data()

    # Step 2: run pipeline
    print(f"\nStep 2: Running pipeline {START_DATE} → {END_DATE} ...")
    run_pipeline(START_DATE, END_DATE)

    # Step 3: assertions
    print("\nStep 3: Assertions ...")

    # (a) Bronze: 7 transaction partitions, each row count > 0
    tx_present = [
        d for d in ALL_DATES
        if (BRONZE_DIR / "transactions" / f"date={d}" / "data.parquet").exists()
    ]
    tx_nonempty = all(
        con.execute(
            f"SELECT count(*) FROM read_parquet('{BRONZE_DIR}/transactions/date={d}/data.parquet')"
        ).fetchone()[0] > 0
        for d in tx_present
    )
    ok_a = len(tx_present) == 7 and tx_nonempty
    if not check("TC-8.5-A: Bronze 7 transaction partitions, each non-empty",
                 ok_a, f"partitions={len(tx_present)}, all_nonempty={tx_nonempty}"):
        failures.append("Bronze transaction partition count or row check failed")

    # (b) Bronze: accounts partitions exist (>=1; fewer than 7 is not an error)
    acct_present = [
        d for d in ALL_DATES
        if (BRONZE_DIR / "accounts" / f"date={d}" / "data.parquet").exists()
    ]
    if not check("TC-8.5-B: Bronze accounts partitions exist (>=1)",
                 len(acct_present) >= 1, f"partitions={len(acct_present)}"):
        failures.append("No Bronze accounts partitions found")

    # (c) Bronze transaction_codes: row count > 0
    tc_bronze = BRONZE_DIR / "transaction_codes" / "data.parquet"
    tc_b_count = (
        con.execute(f"SELECT count(*) FROM read_parquet('{tc_bronze}')").fetchone()[0]
        if tc_bronze.exists() else 0
    )
    if not check("TC-8.5-B: Bronze transaction_codes non-empty",
                 tc_b_count > 0, f"rows={tc_b_count}"):
        failures.append("Bronze transaction_codes empty or missing")

    # (d) Silver transaction_codes: row count > 0
    tc_silver = SILVER_DIR / "transaction_codes" / "data.parquet"
    tc_s_count = (
        con.execute(f"SELECT count(*) FROM read_parquet('{tc_silver}')").fetchone()[0]
        if tc_silver.exists() else 0
    )
    if not check("TC-8.5-B: Silver transaction_codes non-empty",
                 tc_s_count > 0, f"rows={tc_s_count}"):
        failures.append("Silver transaction_codes empty or missing")

    # (e) Silver accounts: row count > 0, count(*) = count(DISTINCT account_id)
    acct_silver = SILVER_DIR / "accounts" / "data.parquet"
    if acct_silver.exists():
        acct_total, acct_distinct = con.execute(
            f"SELECT count(*), count(DISTINCT account_id) FROM read_parquet('{acct_silver}')"
        ).fetchone()
        ok_e = acct_total > 0 and acct_total == acct_distinct
        if not check("TC-8.5-C: Silver accounts non-empty and unique on account_id",
                     ok_e, f"total={acct_total}, distinct={acct_distinct}"):
            failures.append(f"Silver accounts uniqueness failed: total={acct_total}, distinct={acct_distinct}")
    else:
        check("TC-8.5-C: Silver accounts non-empty and unique on account_id",
              False, "file missing")
        failures.append("Silver accounts file missing")

    # (f) Silver transactions: count(DISTINCT transaction_id) = count(*)
    tx_total, tx_distinct = con.execute(
        f"SELECT count(*), count(DISTINCT transaction_id) "
        f"FROM read_parquet('{SILVER_DIR}/transactions/**/*.parquet')"
    ).fetchone()
    ok_f = tx_total == tx_distinct
    if not check("TC-8.5-D: Silver transactions globally unique on transaction_id",
                 ok_f, f"total={tx_total}, distinct={tx_distinct}"):
        failures.append(f"Silver transactions not unique: total={tx_total}, distinct={tx_distinct}")

    # (g) Quarantine: count(*) >= 0 (zero is acceptable)
    q_files = list((SILVER_DIR / "quarantine").rglob("rejected.parquet")) if (SILVER_DIR / "quarantine").exists() else []
    if q_files:
        q_count = con.execute(
            f"SELECT count(*) FROM read_parquet('{SILVER_DIR}/quarantine/date=*/rejected.parquet', "
            f"hive_partitioning=false)"
        ).fetchone()[0]
    else:
        q_count = 0
    check("TC-8.5 (g): Quarantine row count >= 0 (zero is acceptable)",
          q_count >= 0, f"rows={q_count}")

    # (h) Gold daily_summary: row count >= 1
    gold_daily = GOLD_DIR / "daily_summary" / "data.parquet"
    daily_count = (
        con.execute(f"SELECT count(*) FROM read_parquet('{gold_daily}')").fetchone()[0]
        if gold_daily.exists() else 0
    )
    if not check("TC-8.5-E: Gold daily_summary non-empty",
                 daily_count >= 1, f"rows={daily_count}"):
        failures.append("Gold daily_summary empty or missing")

    # (i) Gold weekly_account_summary: row count >= 1
    gold_weekly = GOLD_DIR / "weekly_account_summary" / "data.parquet"
    weekly_count = (
        con.execute(f"SELECT count(*) FROM read_parquet('{gold_weekly}')").fetchone()[0]
        if gold_weekly.exists() else 0
    )
    if not check("TC-8.5-E: Gold weekly_account_summary non-empty",
                 weekly_count >= 1, f"rows={weekly_count}"):
        failures.append("Gold weekly_account_summary empty or missing")

    # (j) Watermark: last_processed_date = END_DATE
    control = PIPELINE_DIR / "control.parquet"
    wm = (
        con.execute(f"SELECT last_processed_date FROM read_parquet('{control}')").fetchone()[0]
        if control.exists() else None
    )
    if not check("TC-8.5-F: Watermark = END_DATE",
                 str(wm) == END_DATE, f"watermark={wm}"):
        failures.append(f"Expected watermark={END_DATE}, got {wm}")

    # (k) Run log: row count >= 8
    run_log = PIPELINE_DIR / "run_log.parquet"
    rl_count = (
        con.execute(f"SELECT count(*) FROM read_parquet('{run_log}')").fetchone()[0]
        if run_log.exists() else 0
    )
    if not check("TC-8.5-G: Run log >= 8 entries",
                 rl_count >= 8, f"rows={rl_count}"):
        failures.append(f"Run log has only {rl_count} entries (expected >= 8)")

    # (l) Run log: no FAILED entries
    failed_count = (
        con.execute(
            f"SELECT count(*) FROM read_parquet('{run_log}') WHERE status = 'FAILED'"
        ).fetchone()[0]
        if run_log.exists() else 0
    )
    if not check("TC-8.5-G: Run log — no FAILED entries",
                 failed_count == 0, f"FAILED={failed_count}"):
        failures.append(f"Run log has {failed_count} FAILED entries")

    print()
    if failures:
        print("RESULT: FAIL")
        for f in failures:
            print(f"  - {f}")
        sys.exit(1)
    else:
        print("RESULT: PASS")


if __name__ == "__main__":
    main()
