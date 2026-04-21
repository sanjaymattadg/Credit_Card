import duckdb
from datetime import date, datetime
from pathlib import Path

# Path constant duplicated from pipeline.py to avoid circular import
PIPELINE_DIR = Path("/app/data/pipeline")

_CONTROL_PATH = PIPELINE_DIR / "control.parquet"


def read_watermark() -> date | None:
    if not _CONTROL_PATH.exists():
        return None
    row = duckdb.connect().execute(
        f"SELECT last_processed_date FROM read_parquet('{_CONTROL_PATH}')"
    ).fetchone()
    return row[0] if row else None


def write_watermark(new_date: date, run_id: str) -> None:
    current = read_watermark()
    if current is not None and new_date <= current:
        raise ValueError(
            f"Watermark monotonicity violation: new_date={new_date} <= current={current}"
        )

    updated_at = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    _CONTROL_PATH.parent.mkdir(parents=True, exist_ok=True)

    duckdb.connect().execute(f"""
        COPY (
            SELECT
                DATE '{new_date}'          AS last_processed_date,
                TIMESTAMP '{updated_at}'   AS updated_at,
                '{run_id}'                 AS updated_by_run_id
        ) TO '{_CONTROL_PATH}' (FORMAT PARQUET)
    """)
