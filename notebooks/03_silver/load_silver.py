# Databricks notebook source
# MAGIC %run ../_bootstrap

# COMMAND ----------

from fhir_lakehouse.config import resource_table
from fhir_lakehouse.control import run_step
from fhir_lakehouse.silver import load_silver

results = []
for resource in resources:
    with run_step(spark, cfg, run_id, "silver_load", resource) as step:
        step["rows"] = load_silver(spark, cfg, resource)
    results.append((resource, resource_table(cfg, "silver", resource), step["rows"]))

# COMMAND ----------

display(spark.createDataFrame(results, "resource string, table string, rows_merged long"))
