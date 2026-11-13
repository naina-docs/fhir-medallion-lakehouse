from __future__ import annotations

import sys
from functools import reduce

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

XML_ENTRY_SCHEMA = T.ArrayType(T.StructType([
    T.StructField("entry_index", T.IntegerType()),
    T.StructField("full_url", T.StringType()),
    T.StructField("resource_type", T.StringType()),
    T.StructField("resource_id", T.StringType()),
    T.StructField("version_id", T.StringType()),
    T.StructField("last_updated", T.StringType()),
    T.StructField("resource_xml", T.StringType()),
]))


def split_xml_bundle(body: str) -> list[tuple]:
    import xml.etree.ElementTree as ET

    fhir = "http://hl7.org/fhir"
    ET.register_namespace("", fhir)
    ns = f"{{{fhir}}}"

    def value(node, path):
        found = node.find(path)
        return found.get("value") if found is not None else None

    out = []
    for i, entry in enumerate(ET.fromstring(body).findall(f"{ns}entry")):
        holder = entry.find(f"{ns}resource")
        if holder is None or len(holder) == 0:
            continue
        res = holder[0]
        out.append((i, value(entry, f"{ns}fullUrl"), res.tag.split("}")[-1], value(res, f"{ns}id"),
                    value(res, f"{ns}meta/{ns}versionId"), value(res, f"{ns}meta/{ns}lastUpdated"),
                    ET.tostring(res, encoding="unicode")))
    return out


def pending_files(spark: SparkSession, cfg: dict, resource: str, run_id: str | None = None) -> DataFrame:
    calls = (spark.table(control_table(cfg, "api_call_log"))
             .where((F.col("resource") == resource) & (F.col("entry_count") > 0)))
    if run_id:
        calls = calls.where(F.col("run_id") == run_id)
    target = resource_table(cfg, "bronze", resource)
    if spark.catalog.tableExists(target):
        calls = calls.join(spark.table(target).select("raw_file_path").distinct(), "raw_file_path", "left_anti")
    return calls


def _json_entries(bundles: DataFrame) -> DataFrame:
    return (bundles
            .withColumn("bundle", F.from_json("value", BUNDLE_SCHEMA))
            .select("*", F.posexplode("bundle.entry").alias("entry_index", "entry"))
            .select("*",
                    F.col("entry.fullUrl").alias("full_url"),
                    F.col("entry.resource").alias("resource_json"),
                    F.get_json_object("entry.resource", "$.id").alias("resource_id"),
                    F.get_json_object("entry.resource", "$.resourceType").alias("resource_type"),
                    F.get_json_object("entry.resource", "$.meta.versionId").alias("version_id"),
                    F.get_json_object("entry.resource", "$.meta.lastUpdated").alias("last_updated_raw")))


def _xml_entries(bundles: DataFrame) -> DataFrame:
    from pyspark import cloudpickle

    cloudpickle.register_pickle_by_value(sys.modules[__name__])
    split = F.udf(split_xml_bundle, XML_ENTRY_SCHEMA)
    return (bundles
            .select("*", F.explode(split("value")).alias("e"))
            .select("*", "e.entry_index", "e.full_url", "e.resource_id", "e.resource_type", "e.version_id",
                    F.col("e.last_updated").alias("last_updated_raw"), "e.resource_xml",
                    F.lit(None).cast("string").alias("resource_json")))


def build_bronze(spark: SparkSession, calls: DataFrame) -> DataFrame | None:
    calls = (calls.withColumn("file_name", F.element_at(F.split("raw_file_path", "/"), -1))
             .withColumn("fmt", F.lower(F.element_at(F.split("file_name", r"\."), -1))))
    files = calls.select("raw_file_path", "fmt").distinct().collect()
    if not files:
        return None
    parts = []
    for fmt in sorted({f.fmt for f in files}):
        raw = (spark.read.text([f.raw_file_path for f in files if f.fmt == fmt], wholetext=True)
               .withColumn("file_name", F.element_at(F.split("_metadata.file_path", "/"), -1)))
        bundles = raw.join(F.broadcast(calls.where(F.col("fmt") == fmt)), "file_name")
        parts.append(_xml_entries(bundles) if fmt == "xml" else _json_entries(bundles))
    entries = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), parts)
    if "resource_xml" not in entries.columns:
        entries = entries.withColumn("resource_xml", F.lit(None).cast("string"))
    return entries.select(
        "resource_id",
        "resource_type",
        "version_id",
        F.expr("try_to_timestamp(last_updated_raw)").alias("last_updated"),
        "resource_json",
        "resource_xml",
        F.col("fmt").alias("source_format"),
        "full_url",
        F.col("api_called_at").alias("extraction_timestamp"),
        F.col("request_url").alias("api_url_or_params"),
        F.col("saved_at").alias("raw_saved_at"),
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
    rows = df.count()
    (df.write.format("delta").mode("append").option("mergeSchema", "true")
     .partitionBy("window_date").saveAsTable(resource_table(cfg, "bronze", resource)))
    return rows
