# Databricks notebook source
# MAGIC %run ../_bootstrap

# COMMAND ----------

from fhir_lakehouse.control import run_step
from fhir_lakehouse.raw_ingest import ingest_resource

results = []
for resource in resources:
    with run_step(spark, cfg, run_id, "raw_ingest", resource) as step:
        stats = ingest_resource(spark, cfg, resource, run_id, run_date)
        step["rows"] = stats["entries"]
        step["details"] = {k: v for k, v in stats.items() if k != "files"}
    results.append(stats)

# COMMAND ----------

display(spark.createDataFrame([{k: v for k, v in r.items() if k != "files"} for r in results]))
