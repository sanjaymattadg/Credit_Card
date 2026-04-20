import argparse
import sys
import uuid
from pathlib import Path

DATA_DIR        = Path("/app/data")
SOURCE_DIR      = Path("/app/source")
BRONZE_DIR      = DATA_DIR / "bronze"
SILVER_DIR      = DATA_DIR / "silver"
GOLD_DIR        = DATA_DIR / "gold"
PIPELINE_DIR    = DATA_DIR / "pipeline"
DBT_PROJECT_DIR = Path("/app/dbt_project")


def run_historical(start_date: str, end_date: str) -> None:
    run_id = str(uuid.uuid4())
    print(f"[HISTORICAL] run_id={run_id} start={start_date} end={end_date} — NOT IMPLEMENTED")


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
