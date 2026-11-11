from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from .config import resource_table, schema_name
from .control import control_table

BUNDLE_SCHEMA = T.StructType([
    T.StructField("resourceType", T.StringType()),
    T.StructField("entry", T.ArrayType(T.StructType([
        T.StructField("fullUrl", T.StringType()),
        T.StructField("resource", T.StringType()),
    ]))),
])


def pending_files(spark: SparkSession, cfg: dict, resource: str, run_id: str | None = None) -> DataFrame:
    calls = (spark.table(control_table(cfg, "api_call_log"))
             .where((F.col("resource") == resource) & (F.col("entry_count") > 0)))
    if run_id:
        calls = calls.where(F.col("run_id") == run_id)
    target = resource_table(cfg, "bronze", resource)
    if spark.catalog.tableExists(target):
        calls = calls.join(spark.table(target).select("raw_file_path").distinct(), "raw_file_path", "left_anti")
    return calls


def build_bronze(spark: SparkSession, calls: DataFrame) -> DataFrame:
    paths = [r.raw_file_path for r in calls.select("raw_file_path").distinct().collect()]
    if not paths:
        return None
    raw = (spark.read.text(paths, wholetext=True)
           .withColumn("file_name", F.element_at(F.split("_metadata.file_path", "/"), -1)))
    calls = calls.withColumn("file_name", F.element_at(F.split("raw_file_path", "/"), -1))
    bundles = raw.join(F.broadcast(calls), "file_name")
    entries = (bundles
               .withColumn("bundle", F.from_json("value", BUNDLE_SCHEMA))
               .select("*", F.posexplode("bundle.entry").alias("entry_index", "entry"))
               .withColumn("resource_json", F.col("entry.resource")))
    return entries.select(
        F.get_json_object("resource_json", "$.id").alias("resource_id"),
        F.get_json_object("resource_json", "$.resourceType").alias("resource_type"),
        F.get_json_object("resource_json", "$.meta.versionId").alias("version_id"),
        F.to_timestamp(F.get_json_object("resource_json", "$.meta.lastUpdated")).alias("last_updated"),
        "resource_json",
        F.col("entry.fullUrl").alias("full_url"),
        F.col("api_called_at").alias("extraction_timestamp"),
        F.col("request_url").alias("api_url_or_params"),
        "request_params",
        "run_id",
        "page_number",
        "entry_index",
        "raw_file_path",
        F.to_date("window_start").alias("window_date"),
        F.current_timestamp().alias("bronze_loaded_at"),
    )


def load_bronze(spark: SparkSession, cfg: dict, resource: str, run_id: str | None = None) -> int:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name(cfg, 'bronze')}")
    df = build_bronze(spark, pending_files(spark, cfg, resource, run_id))
    if df is None:
        return 0
    df = df.cache()
    rows = df.count()
    (df.write.format("delta").mode("append").option("mergeSchema", "true")
     .partitionBy("window_date").saveAsTable(resource_table(cfg, "bronze", resource)))
    df.unpersist()
    return rows
