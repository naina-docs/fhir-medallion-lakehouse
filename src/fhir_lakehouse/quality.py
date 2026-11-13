from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import types as T

from .config import enabled_resources, resource_table, table_name
from .control import control_table

DQ_SCHEMA = T.StructType([
    T.StructField("run_id", T.StringType()),
    T.StructField("check_name", T.StringType()),
    T.StructField("table_name", T.StringType()),
    T.StructField("severity", T.StringType()),
    T.StructField("failed_rows", T.LongType()),
    T.StructField("total_rows", T.LongType()),
    T.StructField("passed", T.BooleanType()),
    T.StructField("checked_at", T.TimestampType()),
])


@dataclass
class Check:
    name: str
    table: str
    severity: str
    failed_sql: str
    total_sql: str
    max_ratio: float = 0.0


def build_checks(cfg: dict) -> list[Check]:
    q = cfg["quality"]
    checks = []
    for r in enabled_resources(cfg):
        b, s = resource_table(cfg, "bronze", r), resource_table(cfg, "silver", r)
        checks += [
            Check(f"{r.lower()}_bronze_lineage", b, "error",
                  f"SELECT count(*) FROM {b} WHERE extraction_timestamp IS NULL OR api_url_or_params IS NULL "
                  f"OR raw_saved_at IS NULL",
                  f"SELECT count(*) FROM {b}"),
            Check(f"{r.lower()}_silver_key_not_null", s, "error",
                  f"SELECT count(*) FROM {s} WHERE resource_id IS NULL OR version_id IS NULL",
                  f"SELECT count(*) FROM {s}"),
            Check(f"{r.lower()}_silver_one_current", s, "error",
                  f"SELECT count(*) FROM (SELECT resource_id FROM {s} WHERE is_current GROUP BY resource_id HAVING count(*) > 1)",
                  f"SELECT count(DISTINCT resource_id) FROM {s}"),
            Check(f"{r.lower()}_silver_valid_period", s, "error",
                  f"SELECT count(*) FROM {s} WHERE effective_to < effective_from",
                  f"SELECT count(*) FROM {s}"),
        ]
    patient = resource_table(cfg, "silver", "Patient")
    allowed = ", ".join(f"'{g}'" for g in q["allowed_gender"])
    checks.append(Check("patient_gender_domain", patient, "warn",
                        f"SELECT count(*) FROM {patient} WHERE is_current AND gender IS NOT NULL AND gender NOT IN ({allowed})",
                        f"SELECT count(*) FROM {patient} WHERE is_current"))
    unknown = cfg["gold"]["unknown_key"]
    for fact in ("fact_encounter", "fact_observation", "fact_condition"):
        f = table_name(cfg, "gold", cfg["tables"]["gold"][fact])
        checks.append(Check(f"{fact}_unknown_patient", f, "warn",
                            f"SELECT count(*) FROM {f} WHERE patient_key = '{unknown}'",
                            f"SELECT count(*) FROM {f}", q["max_unknown_patient_ratio"]))
    wm = control_table(cfg, "watermark")
    checks.append(Check("watermark_freshness", wm, "warn",
                        f"SELECT count(*) FROM {wm} WHERE last_window_end < current_timestamp() - INTERVAL {int(q['freshness_days'])} DAYS",
                        f"SELECT count(*) FROM {wm}"))
    return checks


def run_checks(spark: SparkSession, cfg: dict, run_id: str) -> list[dict]:
    now = datetime.now(timezone.utc)
    results = []
    for c in build_checks(cfg):
        failed = spark.sql(c.failed_sql).first()[0] or 0
        total = spark.sql(c.total_sql).first()[0] or 0
        passed = failed <= c.max_ratio * total
        results.append({"run_id": run_id, "check_name": c.name, "table_name": c.table, "severity": c.severity,
                        "failed_rows": int(failed), "total_rows": int(total), "passed": passed, "checked_at": now})
    spark.createDataFrame(results, DQ_SCHEMA).write.format("delta").mode("append").saveAsTable(
        control_table(cfg, "dq_results"))
    errors = [r["check_name"] for r in results if not r["passed"] and r["severity"] == "error"]
    if errors and cfg["quality"].get("fail_on_error", True):
        raise RuntimeError(f"data quality checks failed: {errors}")
    return results
