# Databricks notebook source
# MAGIC %run ../_bootstrap

# COMMAND ----------

from fhir_lakehouse.bronze import load_bronze
from fhir_lakehouse.config import resource_table
from fhir_lakehouse.control import run_step

results = []
for resource in resources:
    with run_step(spark, cfg, run_id, "bronze_load", resource) as step:
        step["rows"] = load_bronze(spark, cfg, resource)
    results.append((resource, resource_table(cfg, "bronze", resource), step["rows"]))

# COMMAND ----------

display(spark.createDataFrame(results, "resource string, table string, rows_loaded long"))
