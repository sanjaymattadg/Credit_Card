# pipeline.py — stub (to be implemented in Task 1.4)
# Entry points: run_historical(start_date, end_date), run_incremental()

import argparse


def run_historical(start_date: str, end_date: str) -> None:
    print("NOT IMPLEMENTED")


def run_incremental() -> None:
    print("NOT IMPLEMENTED")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Credit Card Transactions Lake pipeline")
    subparsers = parser.add_subparsers(dest="command")

    hist = subparsers.add_parser("historical", help="Run historical pipeline over a date range")
    hist.add_argument("--start-date", required=True)
    hist.add_argument("--end-date", required=True)

    subparsers.add_parser("incremental", help="Run incremental pipeline for next unprocessed date")

    args = parser.parse_args()

    if args.command == "historical":
        run_historical(args.start_date, args.end_date)
    elif args.command == "incremental":
        run_incremental()
    else:
        parser.print_help()
