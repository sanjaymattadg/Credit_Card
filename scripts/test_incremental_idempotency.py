"""
Incremental pipeline idempotency and no-op test.
Task 9.4 — Incremental Idempotency Test.

Deviation from spec:
  Spec uses dates 2024-01-21/2024-01-22/2024-01-23; source data ends at 2024-01-07.
  sim_date is derived as watermark + 1 day (self-adapting to actual watermark state).
  Template source file is the last available transactions CSV in /app/source.
"""

import shutil
import subprocess
import sys
from datetime import date as date_cls, timedelta
from pathlib import Path

import duckdb

DATA_DIR     = Path("/app/data")
SOURCE_DIR   = Path("/app/source")
BRONZE_DIR   = DATA_DIR / "bronze"
SILVER_DIR   = DATA_DIR / "silver"
GOLD_DIR     = DATA_DIR / "gold"
PIPELINE_DIR = DATA_DIR / "pipeline"


def read_watermark():
    control = PIPELINE_DIR / "control.parquet"
    if not control.exists():
        return None
    row = duckdb.connect().execute(
        f"SELECT last_processed_date FROM read_parquet('{control}')"
    ).fetchone()
    return row[0] if row else None


def run_incremental() -> subprocess.CompletedProcess:
    return subprocess.run(
        ["python", "pipeline.py", "incremental"],
        capture_output=True, text=True,
    )


def run_log_count() -> int:
    run_log = PIPELINE_DIR / "run_log.parquet"
    if not run_log.exists():
        return 0
    return duckdb.connect().execute(
        f"SELECT count(*) FROM read_parquet('{run_log}')"
    ).fetchone()[0]


def collect_layer_mtimes() -> dict:
    """Record mtime for every parquet file in Bronze, Silver, Gold, and pipeline dirs."""
    mtimes = {}
    for layer_dir in [BRONZE_DIR, SILVER_DIR, GOLD_DIR, PIPELINE_DIR]:
        if layer_dir.exists():
            for f in layer_dir.rglob("*.parquet"):
                mtimes[str(f)] = f.stat().st_mtime
    return mtimes


def check(label: str, passed: bool, detail: str = "") -> bool:
    tag = "PASS" if passed else "FAIL"
    suffix = f" — {detail}" if detail else ""
    print(f"[{tag}] {label}{suffix}")
    return passed


def main() -> None:
    failures = []
    sim_csv = None
    sim_acct_csv = None

    # ── Step 1: confirm watermark exists ────────────────────────────────────
    print("Step 1: Checking watermark ...")
    watermark = read_watermark()
    if watermark is None:
        print("ERROR: no watermark found. Run historical pipeline first.")
        sys.exit(1)

    # Normalise to datetime.date regardless of what DuckDB returns
    if not isinstance(watermark, date_cls):
        watermark = date_cls.fromisoformat(str(watermark))

    sim_date  = watermark + timedelta(days=1)
    noop_date = sim_date  + timedelta(days=1)
    print(f"  watermark = {watermark}")
    print(f"  sim_date  = {sim_date}  (incremental will process this)")
    print(f"  noop_date = {noop_date}  (second run expects NOOP — no file)")

    # ── Step 2: copy source files (transactions + accounts) for sim_date ────
    print("\nStep 2: Simulating new date ...")
    tx_files = sorted(SOURCE_DIR.glob("transactions_*.csv"))
    if not tx_files:
        print("ERROR: no source transactions CSV files found in /app/source")
        sys.exit(1)
    acct_files = sorted(SOURCE_DIR.glob("accounts_*.csv"))
    if not acct_files:
        print("ERROR: no source accounts CSV files found in /app/source")
        sys.exit(1)

    template_tx   = tx_files[-1]
    template_acct = acct_files[-1]
    sim_csv       = SOURCE_DIR / f"transactions_{sim_date.isoformat()}.csv"
    sim_acct_csv  = SOURCE_DIR / f"accounts_{sim_date.isoformat()}.csv"
    shutil.copy(template_tx,   sim_csv)
    shutil.copy(template_acct, sim_acct_csv)
    print(f"  Copied {template_tx.name}   → {sim_csv.name}")
    print(f"  Copied {template_acct.name} → {sim_acct_csv.name}")

    try:
        # ── Step 3: first incremental run ───────────────────────────────────
        print("\nStep 3: First incremental run ...")
        result1 = run_incremental()
        if result1.returncode != 0:
            print("FAIL — first incremental run exited non-zero")
            print(result1.stderr[-2000:])
            failures.append("First incremental run exited non-zero")
        else:
            # TC-9.4-A: watermark advanced to sim_date
            wm_after_first = read_watermark()
            if not isinstance(wm_after_first, date_cls):
                wm_after_first = date_cls.fromisoformat(str(wm_after_first))
            if not check(f"TC-9.4-A: watermark advanced to sim_date ({sim_date})",
                         wm_after_first == sim_date,
                         f"watermark={wm_after_first}"):
                failures.append(f"Expected watermark={sim_date}, got {wm_after_first}")

            # TC-9.4-A: Bronze partition for sim_date exists and non-empty
            bronze_part = (
                BRONZE_DIR / "transactions"
                / f"date={sim_date.isoformat()}" / "data.parquet"
            )
            bronze_rows = 0
            if bronze_part.exists():
                bronze_rows = duckdb.connect().execute(
                    f"SELECT count(*) FROM read_parquet('{bronze_part}')"
                ).fetchone()[0]
            if not check(f"TC-9.4-A: Bronze partition for {sim_date} exists and non-empty",
                         bronze_rows > 0,
                         f"rows={bronze_rows}"):
                failures.append(f"Bronze partition missing or empty for {sim_date}")

        # ── Step 4: second incremental run (NOOP) ───────────────────────────
        print("\nStep 4: Second incremental run (expect NOOP) ...")

        rl_before    = run_log_count()
        mtimes_before = collect_layer_mtimes()

        result2 = run_incremental()
        if result2.returncode != 0:
            print("FAIL — second incremental run exited non-zero")
            print(result2.stderr[-2000:])
            failures.append("Second incremental run exited non-zero")
        else:
            # TC-9.4-B: watermark unchanged
            wm_after_second = read_watermark()
            if not isinstance(wm_after_second, date_cls):
                wm_after_second = date_cls.fromisoformat(str(wm_after_second))
            if not check(f"TC-9.4-B: watermark unchanged after NOOP ({sim_date})",
                         wm_after_second == sim_date,
                         f"watermark={wm_after_second}"):
                failures.append(
                    f"Watermark changed on NOOP: expected {sim_date}, got {wm_after_second}"
                )

            # TC-9.4-B: run log row count unchanged
            rl_after = run_log_count()
            if not check("TC-9.4-B: run log row count unchanged after NOOP",
                         rl_after == rl_before,
                         f"before={rl_before}, after={rl_after}"):
                failures.append(f"Run log grew on NOOP: {rl_before} → {rl_after}")

            # TC-9.4-C: INV-27 — no layer file timestamps changed
            mtimes_after = collect_layer_mtimes()
            modified = [f for f, mt in mtimes_before.items() if mtimes_after.get(f) != mt]
            new_files = [f for f in mtimes_after if f not in mtimes_before]
            ok_ts = not modified and not new_files
            detail = ""
            if modified:
                detail += f"modified={modified[:3]}"
            if new_files:
                detail += f" new_files={new_files[:3]}"
            if not check("TC-9.4-C: INV-27 — no layer file timestamps changed after NOOP",
                         ok_ts,
                         detail if detail else "all unchanged"):
                failures.append("Layer file timestamps changed during NOOP run (INV-27 violated)")

    finally:
        # ── Step 5: cleanup simulated source files ──────────────────────────
        print("\nStep 5: Cleanup ...")
        if sim_csv and sim_csv.exists():
            sim_csv.unlink()
            print(f"  Removed {sim_csv.name}")
        if sim_acct_csv and sim_acct_csv.exists():
            sim_acct_csv.unlink()
            print(f"  Removed {sim_acct_csv.name}")

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
