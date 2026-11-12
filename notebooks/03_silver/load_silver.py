# Databricks notebook source
# MAGIC %run ../_bootstrap

# COMMAND ----------

from fhir_lakehouse.config import resource_table
from fhir_lakehouse.control import run_step
from fhir_lakehouse.silver import load_silver

results = []
for resource in resources:
    with run_step(spark, cfg, run_id, "silver_load", resource) as step:
        stats = load_silver(spark, cfg, resource)
        step["rows"], step["details"] = stats["inserted"], stats
    results.append((resource, resource_table(cfg, "silver", resource), stats["inserted"], stats["closed"]))

# COMMAND ----------

display(spark.createDataFrame(results, "resource string, table string, versions_inserted long, versions_closed long"))
