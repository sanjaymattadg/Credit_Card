import duckdb
from pathlib import Path
from typing import Dict

from pipeline import BRONZE_DIR


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
