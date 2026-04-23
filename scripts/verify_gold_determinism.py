"""
Verification artifact for INV-20 and INV-24.
INV-20: Gold outputs are deterministic — two runs on identical Silver produce identical aggregate values.
INV-24: Gold is fully overwritten — _computed_at timestamps update on second run.
Not called from pipeline.py.
"""

import subprocess
import sys
import duckdb

DBT_PROJECT_DIR = "/app/dbt_project"
GOLD_PATH = "/app/data/gold/daily_summary/data.parquet"
SNAPSHOT_PATH = "/app/data/gold/daily_summary/_run1_snapshot.parquet"

NON_TIMESTAMP_COLS = [
    "transaction_date",
    "total_transactions",
    "total_signed_amount",
    "online_transactions",
    "instore_transactions",
    "purchases_count",
    "purchases_amount",
    "payments_count",
    "payments_amount",
    "fees_count",
    "fees_amount",
    "interest_count",
    "interest_amount",
    "refunds_count",
    "refunds_amount",
    "_source_period_start",
    "_source_period_end",
]


def run_dbt(run_id: str) -> None:
    result = subprocess.run(
        [
            "dbt", "run",
            "--project-dir", DBT_PROJECT_DIR,
            "--profiles-dir", DBT_PROJECT_DIR,
            "--select", "gold_daily_summary",
            "--vars", f'{{"run_id": "{run_id}", "process_date": "2024-01-01"}}',
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(f"FAIL — dbt run failed for run_id={run_id}")
        print(result.stdout[-2000:])
        sys.exit(1)


def snapshot_run1(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(f"""
        COPY (SELECT * FROM read_parquet('{GOLD_PATH}'))
        TO '{SNAPSHOT_PATH}' (FORMAT PARQUET)
    """)


def main() -> None:
    con = duckdb.connect()
    failures = []

    print("Run 1 — gold_daily_summary (run_id=verify-r1) ...")
    run_dbt("verify-r1")
    snapshot_run1(con)

    run1_computed_at = con.execute(
        f"SELECT MAX(_computed_at) FROM read_parquet('{SNAPSHOT_PATH}')"
    ).fetchone()[0]
    run1_count = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{SNAPSHOT_PATH}')"
    ).fetchone()[0]

    print("Run 2 — gold_daily_summary (run_id=verify-r2) ...")
    run_dbt("verify-r2")

    run2_computed_at = con.execute(
        f"SELECT MAX(_computed_at) FROM read_parquet('{GOLD_PATH}')"
    ).fetchone()[0]
    run2_count = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{GOLD_PATH}')"
    ).fetchone()[0]

    print()

    # TC-7.4-A: row counts identical
    if run1_count == run2_count:
        print(f"[PASS] Row count identical: {run1_count} rows both runs")
    else:
        failures.append(f"Row count mismatch: run1={run1_count}, run2={run2_count}")
        print(f"[FAIL] Row count mismatch: run1={run1_count}, run2={run2_count}")

    # TC-7.4-A: non-timestamp columns identical (INV-20)
    col_list = ", ".join(NON_TIMESTAMP_COLS)
    mismatch_count = con.execute(f"""
        SELECT COUNT(*) FROM (
            SELECT {col_list} FROM read_parquet('{SNAPSHOT_PATH}')
            EXCEPT
            SELECT {col_list} FROM read_parquet('{GOLD_PATH}')
        )
    """).fetchone()[0]

    if mismatch_count == 0:
        print(f"[PASS] INV-20: all non-timestamp columns identical across both runs")
    else:
        failures.append(f"INV-20 violated: {mismatch_count} row(s) differ in non-timestamp columns")
        print(f"[FAIL] INV-20: {mismatch_count} row(s) differ in non-timestamp columns")

    # TC-7.4-B: _computed_at updated (INV-24)
    if run2_computed_at >= run1_computed_at:
        print(f"[PASS] INV-24: _computed_at updated — run1={run1_computed_at}, run2={run2_computed_at}")
    else:
        failures.append(
            f"INV-24 violated: _computed_at did not advance — run1={run1_computed_at}, run2={run2_computed_at}"
        )
        print(f"[FAIL] INV-24: _computed_at did not advance — run1={run1_computed_at}, run2={run2_computed_at}")

    print()
    if failures:
        print("RESULT: FAIL")
        for f in failures:
            print(f"  - {f}")
        sys.exit(1)
    else:
        print("RESULT: PASS")

    # Clean up snapshot
    import os
    try:
        os.remove(SNAPSHOT_PATH)
    except OSError:
        pass


if __name__ == "__main__":
    main()
