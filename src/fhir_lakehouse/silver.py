from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from .config import resource_table, schema_name

LINEAGE_COLS = ["version_id", "last_updated", "extraction_timestamp", "source_run_id", "source_loaded_at"]


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
            .where(F.col("resource_id").isNotNull() & (F.col("resource_type") == resource))
            .select(
                "resource_id",
                *attrs,
                F.col("version_id").cast("long").alias("version_id"),
                "last_updated",
                "extraction_timestamp",
                F.col("run_id").alias("source_run_id"),
                F.col("bronze_loaded_at").alias("source_loaded_at"),
            ))


def latest_per_key(df: DataFrame) -> DataFrame:
    w = Window.partitionBy("resource_id").orderBy(
        F.col("version_id").desc_nulls_last(),
        F.col("last_updated").desc_nulls_last(),
        F.col("extraction_timestamp").desc_nulls_last(),
    )
    return df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")


def upsert(spark: SparkSession, df: DataFrame, target: str) -> None:
    df = df.withColumn("silver_updated_at", F.current_timestamp())
    if not spark.catalog.tableExists(target):
        df.write.format("delta").saveAsTable(target)
        return
    df.createOrReplaceTempView("_silver_src")
    spark.sql(f"""
        MERGE INTO {target} t
        USING _silver_src s ON t.resource_id = s.resource_id
        WHEN MATCHED AND (s.version_id > t.version_id OR (s.version_id = t.version_id AND s.last_updated > t.last_updated))
          THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)


def load_silver(spark: SparkSession, cfg: dict, resource: str) -> int:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name(cfg, 'silver')}")
    df = latest_per_key(flatten(new_bronze_rows(spark, cfg, resource), cfg, resource)).cache()
    rows = df.count()
    if rows:
        upsert(spark, df, resource_table(cfg, "silver", resource))
    df.unpersist()
    return rows
