from __future__ import annotations

import json
from contextlib import contextmanager
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import types as T

from .config import schema_name, table_name

API_CALL_LOG_SCHEMA = T.StructType([
    T.StructField("run_id", T.StringType()),
    T.StructField("resource", T.StringType()),
    T.StructField("window_start", T.TimestampType()),
    T.StructField("window_end", T.TimestampType()),
    T.StructField("page_number", T.IntegerType()),
    T.StructField("request_url", T.StringType()),
    T.StructField("request_params", T.StringType()),
    T.StructField("status_code", T.IntegerType()),
    T.StructField("entry_count", T.IntegerType()),
    T.StructField("api_called_at", T.TimestampType()),
    T.StructField("duration_ms", T.LongType()),
    T.StructField("raw_file_path", T.StringType()),
    T.StructField("saved_at", T.TimestampType()),
])

RUN_LOG_SCHEMA = T.StructType([
    T.StructField("run_id", T.StringType()),
    T.StructField("step", T.StringType()),
    T.StructField("resource", T.StringType()),
    T.StructField("status", T.StringType()),
    T.StructField("started_at", T.TimestampType()),
    T.StructField("finished_at", T.TimestampType()),
    T.StructField("rows_processed", T.LongType()),
    T.StructField("details", T.StringType()),
])

WATERMARK_SCHEMA = T.StructType([
    T.StructField("resource", T.StringType()),
    T.StructField("last_window_end", T.TimestampType()),
    T.StructField("last_run_id", T.StringType()),
    T.StructField("updated_at", T.TimestampType()),
])


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def control_table(cfg: dict, key: str) -> str:
    return table_name(cfg, "control", cfg["tables"]["control"][key])


def create_control_objects(spark: SparkSession, cfg: dict) -> None:
    for layer in cfg["catalog"]["schemas"]:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name(cfg, layer)}")
    for key, schema in [("api_call_log", API_CALL_LOG_SCHEMA), ("run_log", RUN_LOG_SCHEMA), ("watermark", WATERMARK_SCHEMA)]:
        name = control_table(cfg, key)
        if not spark.catalog.tableExists(name):
            spark.createDataFrame([], schema).write.format("delta").saveAsTable(name)


def log_api_calls(spark: SparkSession, cfg: dict, rows: list[dict]) -> None:
    if rows:
        spark.createDataFrame(rows, API_CALL_LOG_SCHEMA).write.format("delta").mode("append").saveAsTable(
            control_table(cfg, "api_call_log"))


def get_watermark(spark: SparkSession, cfg: dict, resource: str) -> datetime | None:
    row = (spark.table(control_table(cfg, "watermark"))
           .where(f"resource = '{resource}'")
           .agg({"last_window_end": "max"}).first())
    return row[0] if row else None


def set_watermark(spark: SparkSession, cfg: dict, resource: str, window_end: datetime, run_id: str) -> None:
    spark.createDataFrame([(resource, window_end, run_id, _now())], WATERMARK_SCHEMA).createOrReplaceTempView("_wm")
    spark.sql(f"""
        MERGE INTO {control_table(cfg, 'watermark')} t
        USING _wm s ON t.resource = s.resource
        WHEN MATCHED AND s.last_window_end > t.last_window_end THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)


@contextmanager
def run_step(spark: SparkSession, cfg: dict, run_id: str, step: str, resource: str | None = None):
    state = {"rows": 0, "details": {}}
    started = _now()
    status = "SUCCEEDED"
    try:
        yield state
    except Exception as exc:
        status = "FAILED"
        state["details"]["error"] = str(exc)[:2000]
        raise
    finally:
        row = (run_id, step, resource, status, started, _now(), int(state["rows"]), json.dumps(state["details"], default=str))
        spark.createDataFrame([row], RUN_LOG_SCHEMA).write.format("delta").mode("append").saveAsTable(
            control_table(cfg, "run_log"))
