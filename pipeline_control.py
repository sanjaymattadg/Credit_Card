import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import date, datetime
from pathlib import Path

# Path constant duplicated from pipeline.py to avoid circular import
PIPELINE_DIR = Path("/app/data/pipeline")

_CONTROL_PATH  = PIPELINE_DIR / "control.parquet"
_RUN_LOG_PATH  = PIPELINE_DIR / "run_log.parquet"

_VALID_STATUSES = {"SUCCESS", "FAILED", "SKIPPED"}

_RUN_LOG_COLUMNS = [
    "run_id", "pipeline_type", "model_name", "layer",
    "started_at", "completed_at", "status",
    "records_processed", "records_written", "records_rejected", "error_message",
]

# Explicit schema ensures correct Parquet types even when all-null columns present
_RUN_LOG_SCHEMA = pa.schema([
    pa.field("run_id",             pa.string()),
    pa.field("pipeline_type",      pa.string()),
    pa.field("model_name",         pa.string()),
    pa.field("layer",              pa.string()),
    pa.field("started_at",         pa.timestamp("us", tz="UTC")),
    pa.field("completed_at",       pa.timestamp("us", tz="UTC")),
    pa.field("status",             pa.string()),
    pa.field("records_processed",  pa.int64()),
    pa.field("records_written",    pa.int64()),
    pa.field("records_rejected",   pa.int64()),
    pa.field("error_message",      pa.string()),
])


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


def read_run_log() -> pd.DataFrame:
    if not _RUN_LOG_PATH.exists():
        return pd.DataFrame(columns=_RUN_LOG_COLUMNS)
    return pd.read_parquet(str(_RUN_LOG_PATH))


def append_run_log_entry(
    run_id, pipeline_type, model_name, layer,
    started_at, completed_at, status,
    records_processed, records_written, records_rejected, error_message,
) -> None:
    # INV-32: status must be enumerated value
    if status not in _VALID_STATUSES:
        raise ValueError(
            f"Invalid status '{status}' — must be one of {_VALID_STATUSES}"
        )
    # INV-31: required fields must be non-null
    for field, value in [
        ("run_id", run_id), ("model_name", model_name),
        ("started_at", started_at), ("completed_at", completed_at),
    ]:
        if value is None:
            raise ValueError(f"Required field '{field}' must not be null")

    # INV-30: read all existing rows before write (append-only guarantee)
    df = read_run_log()

    new_row = pd.DataFrame([{
        "run_id":             run_id,
        "pipeline_type":      pipeline_type,
        "model_name":         model_name,
        "layer":              layer,
        "started_at":         started_at,
        "completed_at":       completed_at,
        "status":             status,
        "records_processed":  records_processed,
        "records_written":    records_written,
        "records_rejected":   records_rejected,
        "error_message":      error_message,
    }])

    combined = pd.concat([df, new_row], ignore_index=True)
    _RUN_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pandas(combined, schema=_RUN_LOG_SCHEMA, preserve_index=False)
    pq.write_table(table, str(_RUN_LOG_PATH))
