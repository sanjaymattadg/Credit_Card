"""
Partial failure recovery test for the historical pipeline.
Verifies that a partially completed historical run can be resumed:
- Already-processed Bronze partitions are unchanged (INV-01, INV-04)
- Remaining dates are processed on resume
- Watermark advances monotonically from partial end_date to full end_date (INV-29)
Not called from pipeline.py.

Adapted date range: 2024-01-01 to 2024-01-03 (partial), 2024-01-01 to 2024-01-07 (full).
Source files only exist for 2024-01-01 to 2024-01-07; task spec used 2024-01-15 to 2024-01-21.
"""

import shutil
import subprocess
import sys
from pathlib import Path

import duckdb

DATA_DIR     = Path("/app/data")
BRONZE_DIR   = DATA_DIR / "bronze"
PIPELINE_DIR = DATA_DIR / "pipeline"

PARTIAL_START  = "2024-01-01"
PARTIAL_END    = "2024-01-03"
FULL_START     = "2024-01-01"
FULL_END       = "2024-01-07"

PARTIAL_DATES  = ["2024-01-01", "2024-01-02", "2024-01-03"]
RESUME_DATES   = ["2024-01-04", "2024-01-05", "2024-01-06", "2024-01-07"]
ALL_DATES      = PARTIAL_DATES + RESUME_DATES


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
        print(f"FAIL — pipeline exited non-zero for {start} → {end}")
        print(result.stderr[-2000:])
        sys.exit(1)


def bronze_row_count(d: str) -> int:
    path = BRONZE_DIR / "transactions" / f"date={d}" / "data.parquet"
    return duckdb.connect().execute(
        f"SELECT count(*) FROM read_parquet('{path}')"
    ).fetchone()[0]


def read_watermark():
    control = PIPELINE_DIR / "control.parquet"
    if not control.exists():
        return None
    row = duckdb.connect().execute(
        f"SELECT last_processed_date FROM read_parquet('{control}')"
    ).fetchone()
    return row[0] if row else None


def check(label: str, passed: bool, detail: str = "") -> bool:
    tag = "PASS" if passed else "FAIL"
    suffix = f" — {detail}" if detail else ""
    print(f"[{tag}] {label}{suffix}")
    return passed


def main() -> None:
    failures = []

    # ── Step 1: clean state ──────────────────────────────────────────────────
    print("Step 1: Cleaning data/ ...")
    clean_data()

    # ── Step 2: partial run (dates 01–03) ───────────────────────────────────
    print(f"\nStep 2: Partial run {PARTIAL_START} → {PARTIAL_END} ...")
    run_pipeline(PARTIAL_START, PARTIAL_END)

    # ── Step 3: verify partial run ──────────────────────────────────────────
    print("\nStep 3: Verifying partial run ...")

    partial_counts = {}
    for d in PARTIAL_DATES:
        path = BRONZE_DIR / "transactions" / f"date={d}" / "data.parquet"
        count = bronze_row_count(d) if path.exists() else 0
        partial_counts[d] = count
        ok = path.exists() and count > 0
        if not check(f"TC-8.4-A: Bronze partition exists and non-empty [{d}]", ok, f"rows={count}"):
            failures.append(f"Bronze partition missing or empty after partial run: {d}")

    n_partial = sum(
        1 for d in PARTIAL_DATES
        if (BRONZE_DIR / "transactions" / f"date={d}" / "data.parquet").exists()
    )
    if not check("TC-8.4-A: 3 Bronze partitions after partial run",
                 n_partial == 3, f"found={n_partial}"):
        failures.append(f"Expected 3 Bronze partitions after partial run, got {n_partial}")

    wm_partial = read_watermark()
    if not check("TC-8.4-A: watermark = PARTIAL_END after partial run",
                 str(wm_partial) == PARTIAL_END, f"watermark={wm_partial}"):
        failures.append(f"Expected watermark={PARTIAL_END} after partial run, got {wm_partial}")

    # ── Step 4: full run (resume 01–07) ─────────────────────────────────────
    print(f"\nStep 4: Full run {FULL_START} → {FULL_END} (resume) ...")
    run_pipeline(FULL_START, FULL_END)

    # ── Steps 5–7: verify resume ─────────────────────────────────────────────
    print("\nSteps 5-7: Verifying resumed run ...")

    n_full = sum(
        1 for d in ALL_DATES
        if (BRONZE_DIR / "transactions" / f"date={d}" / "data.parquet").exists()
    )
    if not check("TC-8.4-B: 7 Bronze partitions after full run",
                 n_full == 7, f"found={n_full}"):
        failures.append(f"Expected 7 Bronze partitions after full run, got {n_full}")

    wm_full = read_watermark()
    if not check("TC-8.4-C: watermark advanced to FULL_END",
                 str(wm_full) == FULL_END, f"watermark={wm_full}"):
        failures.append(f"Expected watermark={FULL_END} after full run, got {wm_full}")

    for d in PARTIAL_DATES:
        count_after = bronze_row_count(d)
        count_before = partial_counts[d]
        if not check(f"TC-8.4-D: Bronze row count unchanged [{d}]",
                     count_after == count_before,
                     f"before={count_before}, after={count_after}"):
            failures.append(f"Bronze row count changed for {d}: {count_before} → {count_after}")

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
