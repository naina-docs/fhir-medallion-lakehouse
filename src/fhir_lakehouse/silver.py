from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from .config import resource_table, schema_name
from .scd2 import build_changes, merge_scd2, with_row_hash


def column_expr(path: str, kind: str) -> str:
    value = f"nullif(trim(get_json_object(resource_json, '{path}')), '')"
    if kind == "lower":
        return f"lower({value})"
    if kind == "ref":
        return f"nullif(regexp_extract({value}, '([^/]+)$', 1), '')"
    if kind == "timestamp":
        return f"try_to_timestamp({value})"
    if kind == "date":
        return f"to_date(try_to_timestamp({value}))"
    if kind in ("double", "int", "boolean"):
        return f"try_cast({value} as {kind})"
    return value


def attribute_columns(cfg: dict, resource: str) -> list[str]:
    return list(cfg["silver_columns"][resource])


def new_bronze_rows(spark: SparkSession, cfg: dict, resource: str) -> DataFrame:
    bronze = spark.table(resource_table(cfg, "bronze", resource))
    target = resource_table(cfg, "silver", resource)
    if spark.catalog.tableExists(target):
        last = spark.table(target).agg(F.max("source_loaded_at")).first()[0]
        if last is not None:
            bronze = bronze.where(F.col("bronze_loaded_at") > F.lit(last))
    return bronze


def flatten(bronze: DataFrame, cfg: dict, resource: str) -> DataFrame:
    mapping = cfg["silver_columns"][resource]
    attrs = [F.expr(column_expr(m["path"], m["type"])).alias(name) for name, m in mapping.items()]
    return (bronze
            .where(F.col("resource_id").isNotNull() & (F.col("resource_type") == resource)
                   & F.col("resource_json").isNotNull())
            .select(
                "resource_id",
                *attrs,
                F.col("version_id").cast("long").alias("version_id"),
                "last_updated",
                "extraction_timestamp",
                "api_url_or_params",
                "raw_saved_at",
                F.col("run_id").alias("source_run_id"),
                F.col("bronze_loaded_at").alias("source_loaded_at"),
            ))


def dedup_versions(df: DataFrame) -> DataFrame:
    w = Window.partitionBy("resource_id", "version_id").orderBy(F.col("extraction_timestamp").desc_nulls_last())
    return df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")


def load_silver(spark: SparkSession, cfg: dict, resource: str) -> dict:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name(cfg, 'silver')}")
    opts = cfg["scd2"]
    bits = int(opts["hash_algorithm"].split("_")[-1])
    target = resource_table(cfg, "silver", resource)
    src = with_row_hash(dedup_versions(flatten(new_bronze_rows(spark, cfg, resource), cfg, resource)),
                        attribute_columns(cfg, resource), bits)
    changes = build_changes(spark, src, target, "resource_id", ["version_id", "last_updated"], opts)
    return merge_scd2(spark, changes, target, "resource_id", opts) if changes.take(1) else {"inserted": 0, "closed": 0}
